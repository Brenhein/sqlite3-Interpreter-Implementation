"""
This class represents a single connection to a database
"""
from copy import deepcopy
from Database import Database
from tokenizer import tokenize
from Errors import CommandError, QueryError, TransactionError

"""Global Variables"""
_ALL_DATABASES = {}
_LOCKS = {}


class Connection(object):
    def __init__(self, filename, timeout, isolation_level):
        """
        Takes a filename, but doesn't do anything with it.
        """
        self.filename = filename  # The filename of the database
        self.timeout = timeout
        self.isolation_level = isolation_level
        self.database = None

        # Creates or connects to a database
        if filename not in _ALL_DATABASES:  # Create a database
            _ALL_DATABASES[filename] = Database(filename)
            _LOCKS[filename] = {"S": 0, "R": 0, "E": 0}

        # The data for transaction
        self.auto_commit = True
        self.modified = False
        self.mode = "D"
        self.shared = False
        self.reserved = False
        self.exclusive = False

    def close(self):
        """
        Closes a connection (Empty for now)
        """
        pass

    def lockable(self, command):
        """
        Sees if the element can be locked
        :param command: The type of the query we want to perform
        :return:
        """

        # Aight so what locks do we need to read
        if command == "SELECT":
            if not self.exclusive and _LOCKS[self.filename]["E"] > 0:
                self.unlock()
                raise TransactionError("Exclusive lock cannot be granted for {}".format(self.filename))
            if not self.shared and not self.reserved:
                _LOCKS[self.filename]["S"] += 1
                self.shared = True

        # Aight so what locks do we need to write
        elif command == "UPDATE" or command == "INSERT" or command == "DELETE":
            self.modified = True

            if not self.exclusive and _LOCKS[self.filename]["E"] > 0:
                self.unlock()
                raise TransactionError("Exclusive lock cannot be granted for {}".format(self.filename))

            # Is there another reserved lock or exclusive lock on the database?
            if not self.reserved:
                if _LOCKS[self.filename]["R"] == 0:  # We can get the shared lock
                    if self.shared:  # Get rid of the current shared lock
                        self.shared = False
                        _LOCKS[self.filename]["S"] -= 1
                    self.reserved = True
                    _LOCKS[self.filename]["R"] += 1
                else:  # Someone else has the shared lock
                    self.unlock()
                    raise TransactionError("Reserved lock cannot be granted for {}".format(self.filename))

    def can_be_reserved(self):
        """
        Returns if a reserved lock can be grabbed for the connection
        """
        # If there is an exclusive lock on this element, GTFO
        locking = _LOCKS[self.filename]["E"]
        if _LOCKS[self.filename]["E"] > 0 and not self.exclusive:
            self.unlock()
            raise TransactionError("Exclusive lock already exists on this element")

        # Is there a reserved lock
        if _LOCKS[self.filename]["R"] > 0:
            if not self.reserved:
                self.unlock()
                raise TransactionError("Exclusive lock cannot be granted for {}".format(self.filename))

    def can_be_exclusive(self):
        """
        Checks if an exclusive lock can be grabbed for the connection
        :return:
        """
        self.can_be_reserved()

        if _LOCKS[self.filename]["S"] > 0:   # Are there any shared locks
            if _LOCKS[self.filename]["S"] > 1 or not self.shared:
                self.unlock()
                raise TransactionError("Exclusive lock cannot be granted for {}".format(self.filename))

    def unlock(self):
        """
        UNlocks the elements of the database
        :return:
        """
        if self.shared:
            _LOCKS[self.filename]["S"] -= 1
            self.shared = False
        if self.reserved:
            _LOCKS[self.filename]["R"] -= 1
            self.reserved = False
        if self.exclusive:
            _LOCKS[self.filename]["E"] -= 1
            self.exclusive = False

    def begin_deferred(self):
        """
        Begins a transaction and handles setting it up
        """
        self.database = deepcopy(_ALL_DATABASES[self.filename])

    def begin_exclusive(self):
        """
        Begins a transaction and handles setting it up FOR ONLY EXCLUSIVE TRANSACTIONS
        """
        self.database = deepcopy(_ALL_DATABASES[self.filename])
        self.can_be_exclusive()
        _LOCKS[self.filename]["E"] += 1
        self.exclusive = True

    def begin_immediate(self):
        """
        Begins a transaction and handles setting it up FOR ONLY IMMEDIATE TRANSACTIONS
        """
        self.database = deepcopy(_ALL_DATABASES[self.filename])
        self.can_be_reserved()
        _LOCKS[self.filename]["R"] += 1
        self.reserved = True

    def commit(self, command):
        """
        Commits a transaction and releases all of the locks it currently holds
        """
        # Only required to handle DML inside transactions for locking for this project
        if command == "CREATE" or command == "DROP":
            _ALL_DATABASES[self.filename] = deepcopy(self.database)
            return

        # This transaction did not modify the database so we should just remove its shared lock if it has one
        if not self.modified:
            if self.shared:
                _LOCKS[self.filename]["S"] -= 1
                self.shared = False
            if self.reserved:
                _LOCKS[self.filename]["R"] -= 1
                self.reserved = False
            if self.exclusive:
                _LOCKS[self.filename]["E"] -= 1
                self.exclusive = False
            return

        # This transaction did modify the database so let's try to commit it
        self.can_be_exclusive()
        _ALL_DATABASES[self.filename] = deepcopy(self.database)

        # Clear up the locks for other transactions to use
        self.unlock()

    def rollback(self):
        """
        If the user wants to rollback a table
        """
        self.database = None

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        tokens = tokenize(statement)
        result = None
        if tokens[-1] != ";":
            raise QueryError("Query missing ';' at the end")

        if self.auto_commit:  # If we are in autocommit mode, write to the database
            self.begin_deferred()
        self.lockable(tokens[0])

        # Handles transaction processing #

        if tokens[0] == "BEGIN":
            if not self.auto_commit:  # Were we currently in a transaction??
                self.unlock()
                raise TransactionError("Cannot begin a transaction inside of another transaction")
            if tokens[1] == "TRANSACTION" or tokens[1] + " " + tokens[2] == "DEFERRED TRANSACTION":
                self.mode = "D"
                self.auto_commit = False
                self.begin_deferred()
            elif tokens[1] + " " + tokens[2] == "IMMEDIATE TRANSACTION":
                self.mode = "I"
                self.auto_commit = False
                self.begin_immediate()
            elif tokens[1] + " " + tokens[2] == "EXCLUSIVE TRANSACTION":
                self.mode = "E"
                self.auto_commit = False
                self.begin_exclusive()

        elif tokens[0] + " " + tokens[1] == "COMMIT TRANSACTION":
            if self.auto_commit:  # Were we currently in a transaction??
                self.unlock()
                raise TransactionError("Tried to commit a non-existent transaction")
            self.commit(tokens[0])
            self.modified = False
            self.auto_commit = True

        elif tokens[0] + " " + tokens[1] == "ROLLBACK TRANSACTION":
            if self.auto_commit:  # Were we currently in a transaction??
                self.unlock()
                raise TransactionError("Tried to rollback a non-existent transaction")
            self.rollback()
            self.modified = False
            self.auto_commit = True

        # Handles DDL processing #

        elif tokens[0] + " " + tokens[1] == "CREATE TABLE":
            exists = False
            if tokens[2] + " " + tokens[3] + " " + tokens[4] == "IF NOT EXISTS":
                exists = True
                del tokens[2:5]
            self.database.create_prep(tokens, exists)

        elif tokens[0] + " " + tokens[1] == "DROP TABLE":
            exists = False
            if tokens[2] + " " + tokens[3] == "IF EXISTS":
                exists = True
                del tokens[2:4]
            self.database.drop(tokens, exists)

        elif tokens[0] + " " + tokens[1] == "CREATE VIEW" and tokens[3] == "AS":
            self.database.create_view(tokens)

        # Handles DML processing #

        elif tokens[0] + " " + tokens[1] == "INSERT INTO":
            self.database.insert_prep(tokens)

        elif tokens[0] == "SELECT":
            result = self.database.select_prep(tokens)

        elif tokens[0] == "UPDATE" and tokens[2] == "SET":
            result = self.database.update_prep(tokens)

        elif tokens[0] + " " + tokens[1] == "DELETE FROM":
            result = self.database.delete_prep(tokens)

        else:  # Command not recognized
            raise CommandError("Command not recognized")

        # If we are in autocommit mode, write to the database
        if self.auto_commit:
            self.commit(tokens[0])
            self.modified = False

        return result

    def executemany(self, statement, values):
        """
        Allows parameters to be entered into an execute statement
        :param values: the values to insert
        """
        # Finds the VALUES
        half = statement.find("VALUES")
        first = statement[:half]
        second = statement[half:]

        start = second.find('(')
        end = second.find(')')
        if start == -1 or end == -1:
            raise QueryError('Cannot find statement to insert into')
        start_statement = first + "VALUES "

        # Find the locations of the '?'s
        for val in values:
            i = 0
            new = second[start:end + 1]
            qmark = new.find('?')
            while qmark != -1:

                # Checks if the parameter is a string
                value = val[i]
                if isinstance(value, str):
                    value = "'" + value + "'"

                new = new[:qmark] + str(value) + new[qmark + 1:]
                qmark = new.find('?', qmark+1)
                i += 1
            start_statement += new + ", "

        start_statement = start_statement[:-2] + ";"
        self.execute(start_statement)

    def create_collation(self, name, function):
        """
        Creates a sorting collation for the database
        :param name: the name of the collation
        :param function: The function itself
        """
        self.database.collations[name] = function
        _ALL_DATABASES[self.filename] = deepcopy(self.database)
