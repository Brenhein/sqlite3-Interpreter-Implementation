"""
This class represents an individual table in a database
"""
from Errors import SQLTypeError, QueryError
from operator import itemgetter
import functools
_OPERATORS = ["<", ">", "=", "!=", "IS", "IS NOT"]


class Table:
    def __init__(self, name, columns, join, rel_tables, default):
        """
        Constructor
        :param name: The name of the table
        :param columns: The name of the columns for a table (holds tuples with 0: name and 1: type)
        """
        self.name = name  # The name of the table
        self.headers = dict()  # The column headers of the table
        self.types = dict()  # The types of the columns
        self.table = list()  # The underlying data in the table
        self.rel_tables = rel_tables
        self.rowCnt = 0  # The size of the table
        self.default = default

        # Creates the column headers for the table
        if not join:
            self.create(columns)

    def append_table_name(self, container):
        """
        Helper function that adds potential table names to the columns
        :param container: The container of columns to add table names to
        :return: The edited container
        """
        for i in range(len(container)):
            ind = container[i].find(".")
            if ind == -1 and container[i] != "*":  # Found a column without a table name
                for el in self.rel_tables:
                    new_name = el + "." + container[i]
                    if new_name in self.headers or container[i] == "*":
                        container[i] = new_name
                        break;

        return container

    def where(self, where):
        """
        Goes through all the columns checking the where condition
        :param where: The where operands
        :return: The list of indexes where WHERE CLAUSE is true
        """
        where_true = []
        op = where.pop(1)
        val = where.pop()
        where = self.append_table_name(where)[0]

        # Error checking
        if op not in _OPERATORS:
            raise QueryError("Operator {} is not valid".format(op))
        elif where not in self.headers:
            raise QueryError("Column {} not in table {}".format(where, self.name))

        # Executes WHERE clause
        for i, row in enumerate(self.table):
            col = row[self.headers[where]]

            # Handles conditional
            if op == "IS" and val is None:
                if col is None:
                    where_true.append(i)
            elif op == "IS NOT" and val is None:
                if col is not None:
                    where_true.append(i)
            elif op == ">":
                if col is not None and col > val:
                    where_true.append(i)
            elif op == "<":
                if col is not None and col < val:
                    where_true.append(i)
            elif op == "=":
                if col is not None and col == val:
                    where_true.append(i)
            elif op == "!=":
                if col is not None and col != val:
                    where_true.append(i)
            else:
                raise QueryError("IS/IS NOT must be followed by NULL")

        return where_true

    def create(self, columns):
        """
        Creates a table that IS NOT TEMPORARY i.e. created from a JOIN query
        :param columns: The columns to add to the table
        """
        for i, column in enumerate(columns):
            if column[0] in self.headers:
                raise QueryError("{} can't be the column name for multiple columns".format(column[0]))
            self.headers[self.name + "." + column[0]] = i
            self.types[self.name + "." + column[0]] = column[1]

    def insert(self, values, columns_to_insert, all_default):
        """
        Inserts the individual values into the table, type checking to make sure everything lines up
        :param values: The values to insert into the table, IN ORDER OF THE COLUMNS
        """
        # Are we doing a default insert??
        if all_default:
            if len(self.default) != len(self.headers):
                raise QueryError("There aren't default values specified for every column")
            values = [v for v in self.default.values()]
            self.table.append(values)
            return

        # Are we doing a full-insert??
        elif len(columns_to_insert) == 0:
            columns_to_insert = [h for h in self.headers.keys()]
            for i, row in enumerate(values):  # Add NULLs to the missing columns
                for j in range(len(row), len(columns_to_insert)):

                    # Is there a default for this column??
                    try:
                        default = self.default[j]
                    except KeyError:
                        default = None

                    row.append(default)

        # Nah, we dealing with a ordered-insert
        else:
            columns_to_insert = self.append_table_name(columns_to_insert)
            for i, row in enumerate(values):
                full_row = [None] * len(self.headers)
                for j, col in enumerate(columns_to_insert):
                    full_row[self.headers[col]] = row[j]

                # Adds default if value wasn't provided
                for v in range(len(full_row)):
                    if full_row[v] is None:
                        try:
                            full_row[v] = self.default[v]
                        except KeyError:
                            pass

                values[i] = full_row

        # Inserting too many values
        for val in values:
            if len(val) > len(self.headers):
                raise QueryError("Must enter values that are equal to or less than the length of the insert columns")

        # Error-checking for types
        for row in values:
            for i, type in enumerate(self.types):
                error = False
                if type == "INTEGER":  # If the value is an integer
                    if not isinstance(row[i], int) and row[i] is not None:
                        raise SQLTypeError("Value: {} is not {}".format(row[i], type))
                elif type == "REAL":  # If the value is a float
                    if not isinstance(row[i], float) and row[i] is not None:
                        raise SQLTypeError("Value: {} is not {}".format(row[i], type))
                elif type == "TEXT":  # If the value is a string
                    if not isinstance(row[i], str) and row[i] is not None:
                        raise SQLTypeError("Value: {} is not {}".format(row[i], type))

        self.table += values

    def delete(self, where):
        """
        Deletes all the rows where the WHERE clause is true, or none if specified
        :param where: The WHERE clause tokens
        """
        # delete all rows in the table
        if len(where) == 0:
            self.table.clear()

        # Delete rows based on WHERE
        else:
            where_true = self.where(where)
            new_table = []
            for i in range(len(self.table)):
                if i not in where_true:
                    new_table.append(self.table[i])
            self.table = new_table

    def update(self, where, columns_to_get):
        """
        Updates the rows of the table matching the WHERE clause, or all of them if none
        :param where:
        :param columns_to_get: The columns that need updating, each el as tuple with column name and value
        """
        # Get the rows to update
        if len(where) > 0:
            where_true = self.where(where)
        else:
            where_true = [i for i in range(len(self.table))]

        # Adds the table name to the columns
        for col in columns_to_get:
            col[0] = self.append_table_name([col[0]])[0]

        # Updates the actual table
        for i, row in enumerate(self.table):
            if i in where_true:
                for col in columns_to_get:

                    # Type checking to make sure you're setting the correct type
                    if self.types[col[0]] == "INTEGER":  # If the value is an integer
                        if not isinstance(col[1], int) and col[1] is not None:
                            raise SQLTypeError("Value: {} is not INTEGER".format(col[1]))
                    elif self.types[col[0]] == "REAL":  # If the value is a float
                        if not isinstance(col[1], float) and row[i] is not None:
                            raise SQLTypeError("Value: {} is not REAL".format(col[1]))
                    elif self.types[col[0]] == "TEXT":  # If the value is a string
                        if not isinstance(col[1], str) and row[i] is not None:
                            raise SQLTypeError("Value: {} is not TEXT".format(col[1]))

                    row[self.headers[col[0]]] = col[1]

    def select(self, columns, order_by, distinct, where, collations, aggregates):
        """
        Selects records from the table
        :param columns:
        :param order_by:
        """
        if len(self.table) == 0:
            return []

        order_by_ind = list()

        columns = self.append_table_name(columns)

        # Replaces * with columns
        new_columns = list()
        for col in columns:
            ind = col.find("*")
            if col == "*":  # Get everything
                new_columns += [k for k in self.headers.keys()]
            elif ind != -1:  # get everything from a specific table
                table_name = col[:ind-1]
                if table_name in self.rel_tables:
                    for el in self.headers:
                        if len(table_name) + 1 < len(el) and el[:len(table_name) + 1] == table_name + ".":
                            new_columns.append(el)
                else:
                    raise QueryError("Table to * is not part of outer table")
            else:
                new_columns.append(col)

        columns = new_columns
        matching_rows = self.table

        # Handles the where clause if it exists
        if len(where) > 0:
            where_true = self.where(where)
            new_matches = []
            for i in where_true:
                new_matches.append(matching_rows[i])
            matching_rows = new_matches

        # Handles removing duplicate rows
        if distinct:
            matching_rows = set(matching_rows)
            matching_rows = list(matching_rows)

        # Gets the indexes to sort by
        if len(order_by) > 0:
            # Separates the order_by table names and their directions
            directions = []
            new_order_by = []
            for order in order_by:
                new_order_by.append(order[1:])
                directions.append(order[0])

            order_by = self.append_table_name(new_order_by)

            headers = [h for h in self.headers.keys()]
            for i in range(len(order_by)):
                for j in range(len(headers)):
                    if order_by[i] == headers[j]:
                        order_by_ind.append(j)
            if len(order_by_ind) != len(set(order_by)):  # makes sure all the columns were valid
                raise QueryError("Cannot Order records with non-existent column")

            # Are we ordering the data to be returned
            if directions[0] == "A":
                if collations[0] is not None:
                    temp = {row[order_by_ind[0]]: row for row in matching_rows}
                    keys = [k for k in temp]
                    keys.sort(key=functools.cmp_to_key(collations[0]))
                    matching_rows = []
                    for val in keys:
                        matching_rows.append(temp[val])
                else:
                    matching_rows.sort(key=itemgetter(order_by_ind[0]))

            elif directions[0] == "D":
                if collations[0] is not None:
                    temp = {row[order_by_ind[0]]: row for row in matching_rows}
                    keys = [k for k in temp]
                    keys.sort(reverse=True, key=functools.cmp_to_key(collations[0]))
                    matching_rows = []
                    for val in keys:
                        matching_rows.append(temp[val])
                else:
                    matching_rows.sort(reverse=True, key=itemgetter(order_by_ind[0]))

            new_matching_rows = matching_rows.copy()
            not_duplicate = []

            # Handles extra sorts for duplicate columns
            for i in range(1, len(order_by_ind)):
                prev_col = order_by_ind[i-1]
                curr_dup_rows, dup_rows = [], []
                j = 0

                while j < len(matching_rows):

                    # Keep going through duplicates as you find them
                    dup_found = False

                    while j + 1 < len(matching_rows) and j not in not_duplicate:
                        # Check if matching
                        if collations[i - 1] is not None:
                            matched = collations[0](matching_rows[j][prev_col], matching_rows[j + 1][prev_col])
                            if not matched:  # matched == 0, so same
                                matched = True
                            else:
                                matched = False
                        else:
                            matched = matching_rows[j][prev_col] == matching_rows[j + 1][prev_col]

                        if not matched:
                            break

                        dup_found = True
                        curr_dup_rows.append(matching_rows[j])
                        j += 1

                    # One last duplicate to add to the tail end
                    if dup_found and j not in not_duplicate:
                        curr_dup_rows.append(matching_rows[j])
                        dup_rows.append(curr_dup_rows)
                        curr_dup_rows = []

                    # Was not duplicated so add it to list of rows to avoid
                    elif j not in not_duplicate:
                        not_duplicate.append(j)

                    j += 1

                # Are we ordering the data to be returned
                for matches in dup_rows:
                    if directions[i] == "A":
                        if collations[i] is not None:
                            temp = {row[order_by_ind[i]]: row for row in matches}
                            keys = [k for k in temp]
                            keys.sort(key=functools.cmp_to_key(collations[i]))
                            matches = []
                            for val in keys:
                                matches.append(temp[val])
                        else:
                            matches.sort(key=itemgetter(order_by_ind[i]))
                    elif directions[i] == "D":
                        if collations[i] is not None:
                            temp = {row[order_by_ind[i]]: row for row in matches}
                            keys = [k for k in temp]
                            keys.sort(reverse=True, key=functools.cmp_to_key(collations[i]))
                            matches = []
                            for val in keys:
                                matches.append(temp[val])
                        else:
                            matches.sort(reverse=True, key=itemgetter(order_by_ind[i]))

                # Replace the data in the table
                dup_rows = [item for t in dup_rows for item in t]
                dup = 0
                for row in range(len(matching_rows)):
                    if row not in not_duplicate:
                        new_matching_rows[row] = dup_rows[dup]
                        dup += 1

                matching_rows = new_matching_rows

        # Gets the aggregates now to avoid wasted runtime by repeatedly doing it in a loop
        agg_found = False
        for i in range(len(aggregates)):
            if aggregates[i] == "max":
                aggregates[i] = max([r[self.headers[columns[i]]] for r in matching_rows])
                agg_found = True
            elif aggregates[i] == "min":
                aggregates[i] = min([r[self.headers[columns[i]]] for r in matching_rows])
                agg_found = True

        # Uh-Oh, combining an aggregate with a non-aggregate
        if agg_found and None in aggregates:
            raise QueryError("Cannot combine aggregate with non aggregate")

        # Create single row of aggregate data and return
        if agg_found:
            matching_rows = [tuple(aggregates)]

        # Nah, just gather the rows normally
        else:
            # Gather the actual data
            new_matching_rows = []
            for row in matching_rows:
                record = list()
                for col in columns:
                    if col not in self.headers:  # column does not exist
                        raise QueryError("{} is not a column name in {}".format(col, self.name))
                    record.append(row[self.headers[col]])
                new_matching_rows.append(tuple(record))
            matching_rows = new_matching_rows

        return matching_rows

