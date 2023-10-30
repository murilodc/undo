import psycopg2

def connect_database():
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