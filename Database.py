"""
This class represents the entire database, holding 0 or more tables, all with relations to one another
"""
from Table import Table
from View import View
from Errors import SQLTypeError, QueryError, TableError
_TYPES = ["INTEGER", "REAL", "TEXT"]


class Database:
    def __init__(self, name):
        """
        Constructor
        :param name: The name of the database
        """
        self.name = name  # The name of the database
        self.collations = {}
        self.tables = dict()  # All of the tables in the database

    def create_prep(self, tokens, exists):
        """
        Prepares the tokens to be processed by the database, error checking if need be
        :param tokens: The list of tokens to be processed
        """
        name = tokens[2]
        columns = list()
        default = {}

        # Gets the column names and their types
        i = 4
        while tokens[i] != ";" and i < len(tokens):
            if tokens[i + 1] not in _TYPES:  # Typing Error
                raise SQLTypeError("Type '{}' not recognized by SQL".format(tokens[i + 1]))
            columns.append((tokens[i], tokens[i + 1]))

            # Checks for default values
            if tokens[i + 2] == "DEFAULT":
                if tokens[i + 1] == "INTEGER":
                    default[len(columns) - 1] = int(tokens[i + 3])
                elif tokens[i + 1] == "TEXT":
                    default[len(columns) - 1] = str(tokens[i + 4])
                    i += 2
                elif tokens[i + 1] == "REAL":
                    default[len(columns) - 1] = float(tokens[i + 3])
                i += 2

            i += 3

        self.create(name, columns, exists, default)

    def drop(self, tokens, exists):
        """
        Prepares the tokens to be processed by the database, error checking if need be
        :param tokens: The list of tokens to be processed
        """
        columns = list()
        if len(tokens) > 2:
            name = tokens[2]
        else:
            raise QueryError("Must provide a table name to drop")

        # Tries to drop the table
        if name not in self.tables.keys() and not exists:
            raise TableError("Table {} does not exist".format(name))
        elif name not in self.tables.keys() and exists:
            return
        else:
            del self.tables[name]

    def insert_prep(self, tokens):
        """
        Prepares the tokens to be processed as an insert command
        :param tokens: The list of tokens to be processed
        """
        name = tokens[2]
        values = list()
        columns_to_insert = list()
        i = 3

        # Are we just inserting default values
        if tokens[3] + " " + tokens[4] == "DEFAULT VALUES":
            self.insert(name, values, columns_to_insert, True)
            return

        # Gets the columns to insert into
        if tokens[i] == "(":
            i += 1
            while i < len(tokens) and tokens[i] != ')':
                if tokens[i] == ",":
                    i += 1
                    continue
                if tokens[i + 1] == "," or tokens[i + 1] == ")":  # preceded by a comma or ) is next
                    columns_to_insert.append(tokens[i])
                else:
                    raise QueryError("Missing comma separator")
                i += 1
                if i >= len(tokens):  # Never found a FROM
                    raise QueryError("'FROM' clause not found")
            i += 1

        if tokens[i] == "VALUES":
            # Gets the values to insert
            i += 2
            row = list()
            while i < len(tokens) and (tokens[i-1] == "(" or tokens[i-1] == ","):
                if not isinstance(tokens[i], int) and not isinstance(tokens[i], float) and tokens[i] is not None:
                    if tokens[i] == "'":  # Its a string value:
                        row.append(tokens[i + 1])
                        i += 2
                    else:
                        raise SQLTypeError("{} is not a valid SQL type".format(tokens[i]))
                else:
                    row.append(tokens[i])
                i += 2
                if tokens[i] == "," and tokens[i-1] == ")" and tokens[i+1] == "(":  # we gotta insert more shit
                    values.append(row)
                    row = []
                    i += 2

            values.append(row)
            if i >= len(tokens):
                raise QueryError("Can't find ')' to end INSERT statement")
            if tokens[i] != ";":
                raise QueryError("Missing ',' separator in value list")

        else:
            raise QueryError("Can't perform INSERT statement")

        self.insert(name, values, columns_to_insert, False)

    def update_prep(self, tokens):
        """
        Updates a table with new values based ona where clause
        :param tokens: The list of tokens of the query
        """
        i = 3
        colums_to_set = []
        where = []
        name = tokens[i-2]

        # Gets the columns to set
        while i + 3 < len(tokens):
            if tokens[i + 1] != "=":
                raise QueryError("Invalid SET command.  Need '=' operator")
            if tokens[i + 2] == "'":  # It's a string to set to
                colums_to_set.append([tokens[i], tokens[i+3]])
                i += 2
            else:
                colums_to_set.append([tokens[i], tokens[i+2]])
            i += 3

            if tokens[i] == ",":
                i += 1
            elif tokens[i] == ";" or tokens[i] == "WHERE":
                break
            else:
                raise QueryError("Missing comma separator")
        else:
            raise QueryError("Missing ';' or 'WHERE' clause")

        # Dealing with a WHERE clause
        if tokens[i] == "WHERE":
            i += 1
            i = self.process_where(tokens, i, where)

        # Is the UPDATE statement ended validly
        if tokens[i] != ";":
            raise QueryError("Missing ';' at the end of update statement")

        self.update(name, where, colums_to_set)

    def delete_prep(self, tokens):
        """
        Deletes rows based on a where clause
        :param tokens: The list of tokens of the query
        """
        i = 3
        where = []
        name = tokens[i-1]

        # Is there a where clause
        if i + 4 < len(tokens) and tokens[i] == "WHERE":
            i += 1
            i = self.process_where(tokens, i, where)
            if tokens[i] != ";":
                raise QueryError("Delete statement missing ';'")
        # We're deleting a whole table
        elif i < len(tokens) and tokens[i] == ";":
            pass
        else:
            raise QueryError("Invalid delete statement")

        self.delete(name, where)

    def select_prep(self, tokens):
        """
        Prepares the tokens to be processed as an select command
        :param tokens: The list of tokens to be processed
        """
        columns_to_get = list()
        order_by = list()
        collations = list()
        aggregates = list()
        i = 1
        distinct = False

        if tokens[1] == "DISTINCT":  # distinct select
            distinct = True
            i += 1

        # Gets the list of columns and the table name
        while tokens[i] != "FROM":
            if tokens[i] == ",":
                i += 1
                continue
            if (tokens[i] == "max" or tokens[i] == "min") and tokens[i+1] == "(" and tokens[i+3] == ")":
                columns_to_get.append(tokens[i+2])
                aggregates.append(tokens[i])
                i += 3
            elif tokens[i+1] == "," or tokens[i+1] == "FROM":  # preceded by a comma or FROM is next
                columns_to_get.append(tokens[i])
                aggregates.append(None)
            else:
                raise QueryError("Missing comma separator")
            i += 1
            if i >= len(tokens):  # Never found a FROM
                raise QueryError("'FROM' clause not found")
        i += 2
        name = tokens[i-1]

        # Checks the next clause
        jname = None
        where = []
        while i < len(tokens):
            # LEFT OUTER JOIN
            if i + 2 < len(tokens) and tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2] == "LEFT OUTER JOIN":
                i += 3
                i, jname = self.process_left_outer_join(tokens, i, name)
                name = jname

            # WHERE
            elif tokens[i] == "WHERE":
                i += 1
                i = self.process_where(tokens, i, where)

            # ORDER BY
            elif i + 1 < len(tokens) and tokens[i] + " " + tokens[i + 1] == "ORDER BY":
                i += 2
                i = self.process_order_by(tokens, i, order_by, collations)

            # ;
            elif tokens[i] == ';':
                break
            else:
                raise QueryError("Invalid Query. Stuck at token {}".format(tokens[i]))

        # Handles column from different table
        ret_table = self.select(columns_to_get, name, order_by, distinct, where, collations, aggregates)

        if jname is not None:
            del self.tables[jname]

        return ret_table

    """Handles the special SELECT clauses"""

    def process_where(self, tokens, i, where):
        """
        Gets the operators for the WHERE clause
        :param tokens: The list of current tokens for the query
        :param i: the current index into hte tokens
        :param where: The list of where tokens, specifically
        :return: the new index into the list of tokens
        """
        # Grabs the operator statement values
        left_key = tokens[i]
        op = tokens[i + 1]

        if tokens[i+2] == "'":  # It's a string
            right_key = tokens[i + 3]
            i += 2
        else:
            right_key = tokens[i + 2]

        where.append(left_key)
        where.append(op)
        where.append(right_key)

        return i + 3

    def process_order_by(self, tokens, i, order_by, collations):
        """
        Gets the list of tokens to sort the returned records by
        :param order_by: A list of all the elements to order the records by
        :param tokens: The list of tokens processed by the tokenizer
        :param i: the current index into the token list
        :return: the new index
        """
        while i < len(tokens) and (tokens[i-2] + " " + tokens[i-1] == "ORDER BY" or tokens[i-1] == ","):
            col_name = "A" + tokens[i]
            collate = False

            # Do we have a custom collation
            if tokens[i + 1] == "COLLATE":
                collate = True
                collate_name = tokens[i + 2]
                if collate_name not in self.collations.keys():
                    raise QueryError("Collation does not exist")
                collations.append(self.collations[collate_name])
                i += 2

            # Are we ascending or descending
            if tokens[i+1] == "DESC":
                col_name = "D" + col_name[1:]
            order_by.append(col_name)

            # If we have a DESC or ASC token, add 1
            if tokens[i+1] == "DESC" or tokens[i+1] == "ASC":
                i += 1

            i += 1

            if not collate:
                collations.append(None)

            if tokens[i] == ',':  # If tokens is a ',', skip it
                i += 1

        return i

    def process_left_outer_join(self, tokens, i, normal_name):
        join_name = tokens[i]
        i += 1

        # Handles grabbing ALL THE DATA from the table to join on first
        normal_table = self.select(["*"], normal_name, aggregates=[None])
        normal_headers = self.tables[normal_name].headers
        normal_types = self.tables[normal_name].types
        normal_default = self.tables[normal_name].default

        # Handles grabbing ALL THE DATA from the table to join on first
        join_table = self.select(["*"], join_name, aggregates=[None])
        join_headers = self.tables[join_name].headers
        join_types = self.tables[join_name].types
        join_default = self.tables[join_name].default

        # Valid syntax check
        if i + 3 < len(tokens) and tokens[i] != "ON" or tokens[i+2] != "=":
            raise QueryError("Need to have key to join on")
        i += 1

        # Grabs the equality statement values
        left_key = tokens[i]
        right_key = tokens[i+2]
        if left_key == right_key:
            raise QueryError("Joining keys can't be the same key")
        i += 3

        # Creates the joined table with name for a TEMPORARY TABLE
        jname = "JOIN1"
        ind = 1
        while jname in self.tables:
            ind += 1
            jname = "JOIN" + str(ind)
        table = Table(jname, [], True, [normal_name, join_name], {})
        table.headers = {**normal_headers, **join_headers}
        table.types = {**normal_types, **join_types}
        table.rowCnt = len(normal_table)
        table.default = {**normal_default, **join_default}

        # Adds the right side table headers with updated indexes
        cnt = len(normal_headers)
        for key in join_headers:
            table.headers[key] = cnt
            cnt += 1

        left_key = table.append_table_name([left_key])[0]
        right_key = table.append_table_name([right_key])[0]

        # Checks to see which table contains which key
        if left_key in normal_headers and right_key in join_headers: pass
        elif right_key in normal_headers and left_key in join_headers: left_key, right_key = right_key, left_key
        else: raise QueryError("Can't join tables based on keys provided")

        # Handles joining
        new_table = list()
        for record in normal_table:
            key_to_search = record[normal_headers[left_key]]
            for join_record in join_table:
                if key_to_search == join_record[join_headers[right_key]] and key_to_search is not None:
                    new_table.append(record + join_record)
                    break
            else:
                new_table.append(record + tuple([None] * len(join_record)))

        # Creates the temporary table in memory
        table.table = new_table

        self.tables[jname] = table
        return i, jname

    """Basic Processing for queries once the tokens have been interpreted"""

    def create(self, name, columns, exists, default):
        """
        This method creates a table in the database
        :param name: The name of the table
        :param columns: The columns and their types
        """
        # Checks to see if the table already exists
        if name in self.tables.keys() and not exists:
            raise TableError("Table {} already exists".format(name))
        elif name in self.tables.keys() and exists:
            return

        table = Table(name, columns, False, [name], default)
        self.tables[name] = table

    def create_view(self, tokens):
        """
        Creates a view from a select statement
        :param tokens: The tokens to use
        """
        # Checks to see if the table exists
        name = tokens[2]
        if name in self.tables.keys():
            raise TableError("Table {} already exists".format(name))

        view = View(name, tokens[4:], self)
        self.tables[name] = view

    def insert(self, name, values, columns_to_insert, all_default):
        """
        Inserts a row into the database, if possible
        :param values: The values to be inserted into the database
        :param name: The name of the table
        """
        # Checks to see if the table exists
        if name not in self.tables.keys():
            raise TableError("Table {} does not exists".format(name))

        self.tables[name].insert(values, columns_to_insert, all_default)

    def update(self, name, where, columns_to_set):
        # Checks to see if the table exists
        if name not in self.tables.keys():
            raise TableError("Table {} does not exists".format(name))

        self.tables[name].update(where, columns_to_set)

    def delete(self, name, where):
        # Checks to see if the table exists
        if name not in self.tables.keys():
            raise TableError("Table {} does not exists".format(name))

        self.tables[name].delete(where)

    def select(self, columns_to_get, name, order_by=[], distinct=False, where=[], collations=[], aggregates=[]):
        # Checks to see if the table exists
        if name not in self.tables.keys():
            raise TableError("Table {} does not exists".format(name))

        return self.tables[name].select(columns_to_get, order_by, distinct, where, collations, aggregates)
