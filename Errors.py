"""
This file holds all the error classes
"""


class QueryError(Exception):
    pass


class CommandError(Exception):
    pass


class SQLTypeError(Exception):
    pass


class TableError(Exception):
    pass


class TransactionError(Exception):
    pass