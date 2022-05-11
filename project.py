"""
Name: Brenden Hein
Time To Completion: 8 hours
Comments: None

Sources: None
"""
from Connection import Connection


def connect(filename, timeout=0.1, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename, timeout, isolation_level)


def check(sql_statement, conn, expected):
  print("SQL: " + sql_statement)
  result = conn.execute(sql_statement)
  result_list = list(result)

  print("expected:")
  print("student: ")
  print(result)
  assert expected == result_list


conn = connect("test1.db")
conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
conn.executemany("INSERT INTO students VALUES (?, ?, ?);",
    [('Josh', 3.5, 480),
    ('Tyler', 2.5, 480),
    ('Tosh', 4.5, 450),
    ('Losh', 3.2, 450),
    ('Grant', 3.3, 480),
    ('Emily', 2.25, 450),
    ('James', 2.25, 450)])
check("SELECT min(grade), max(name) FROM students WHERE name > 'T' ORDER BY grade, name;",
conn,
[(2.5, 'Tosh')]
)