import psycopg2
import json

def connectDatabase():
  try:
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
  print("CRIANDO TABELA")
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
    print("CRINADO TABELA TRY")
    for command in psql:
      cursor.execute(command)
    cursor.close()
    conn.commit()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  closeDatabase(cursor)

def insertData():
  file = open('metadado.json', 'r')

  try:
    data = json.load(file)['INITIAL']
    tuples = list( zip(data['id'], data['A'], data['B']) )

    conn = connectDatabase()
    cursor = conn.cursor()

    for tuple in tuples:
      tuple = [str(column) for column in tuple]
      values = ', '.join(tuple)
      command = ("""INSERT INTO dados(id,a,b) VALUES ("""+ values + """)""")
      cursor.execute(command)
    
    cursor.close()
    conn.commit()
  
  finally:
    closeDatabase(cursor)
    file.close()

createTable()
insertData()