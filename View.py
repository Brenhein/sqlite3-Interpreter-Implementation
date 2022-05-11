"""
This class is derived class of a table
"""
from Errors import QueryError
from Table import Table


class View:
    def __init__(self, name, query, database):
        """
        Initializes the view
        :param query:
        """
        self.name = name
        self.query = query
        self.database = database
        self.columns = list()

        # Parses the query to get the data it needs
        i = 1
        while query[i] != "FROM":
            if query[i] == ",":
                i += 1
                continue
            if query[i + 1] == "," or query[i + 1] == "FROM":  # preceded by a comma or FROM is next
                self.columns.append(query[i])
            else:
                raise QueryError("Missing comma separator")
            i += 1
        i += 2
        self.sub_name = query[i-1]

        # removes '*'
        for col in self.columns:
            if col == '*':
                self.columns = [h for h in self.database.tables[self.sub_name].headers.keys()]

    def select(self, columns_to_get, order_by, distinct, where, collations, aggregates):
        data = self.database.select_prep(self.query)
        table = Table(self.sub_name, self.columns, False, [self.sub_name], {})
        table.table = data

        # Sets the types/headers for the table
        table.headers = {col: i for i, col in enumerate(self.columns)}

        # Gets the related table names
        for col in self.columns:
            found = col.find('.')
            if found != -1 and col[:found] not in table.rel_tables:
                table.rel_tables.append(col[:found])

        return table.select(columns_to_get, order_by, distinct, where, collations, aggregates)