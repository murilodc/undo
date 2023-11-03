import psycopg2
import json
import re
from file_read_backwards import FileReadBackwards

listaSemCommit = []

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

def readLog():
    conn = connectDatabase()
    cursor = conn.cursor()
    log = lerArquivo()
    print(log)
    regex = ''
    with FileReadBackwards('entradaLog', encoding="utf-8") as file:
      for line in file:
          print(line)
          if "END CKPT" in line:
              break
          if 'start' in line:
              transacao = line[-3:-1]
              regex = r"<commit {}>".format(transacao)
              vazio = not re.search(regex, lerArquivo())
              if(vazio):
                    listaSemCommit.append(transacao)
    checkpoint = listaSemCommit[::-1]
    undo(cursor)
    printTransactions(checkpoint)
    printInitial(cursor)


def undo(cursor):
  file = open('entradaLog', 'r')
  for transaction in listaSemCommit:
                file.seek(0)
                conteudo = file.read()
                inicioTransaction = conteudo.index('<start '+ transaction +'>')
                file.seek(inicioTransaction)
                for line in file:
                    matches = re.search('<'+ transaction +',(.+?)>', line)
                    if('<commit '+ transaction +'>' in line):
                         continue
                    if matches:
                        values = matches.group(1).split(',')
                        cursor.execute('SELECT ' + values[1] + ' FROM dados WHERE id = ' + values[0])
                        tuple = cursor.fetchone()[0]
                        print(tuple)
                        if(int(values[2]) != tuple):
                            cursor.execute(f'UPDATE dados SET {values[1]} = {values[2]} WHERE id = {values[0]}')
                            print('Na linha ' + values[0] + ' a coluna ' + values[1] + ' era ' + str(tuple) + ' e os novos valores sao: ' + values[2])
def printInitial(cursor):
    id = []
    a = []
    b = []

    cursor.execute('SELECT * FROM dados ORDER BY id')
    tuples = cursor.fetchall()

    for tuple in tuples:
        id.append(tuple[0])
        a.append(tuple[1])
        b.append(tuple[2])

    print('''
        {
          "INITIAL": {
            "id": ''' + str(id)[1:-1] + ''',
            "A: ''' + str(a)[1:-1] + ''',
            "B": ''' + str(b)[1:-1] + '''
          }
        }
      ''')

def printTransactions(committed_transactions):
  for transaction in committed_transactions:
      print('Transação '+ transaction +': realizou UNDO')

def lerArquivo():
      with FileReadBackwards('entradaLog', encoding="utf-8") as file:
        log='\n'.join(file)
      return log