import psycopg2

def connectDatabase():
  print("chamada func")
  try:
    print("dentro try")
    conn = psycopg2.connect(
      database = "log",
      user = "postgres",
      password = "postgres",
    )
    return conn
  except psycopg2.DatabaseError as error:
    print(error)
    exit()

def closeDatabase(conn):
  conn.close()

def createTable():
  psql = (
    """drop table if exists dados;""",

        """create table dados(
            id serial,
            A integer not null,
            B integer not null,
            constraint pk primary key (id)
        );"""
  )
  try:
    conn = connectDatabase()
    cursor = conn.cursor()
    
    for command in commands:
      cursor.execute(command)
    cursor.close()
    conn.commit()

  except (Exception, psycopg2.DatabaseError) as Error:
    print(error)
  closeDatabase(cursor)

createTable()