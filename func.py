import psycopg2
import json
import re

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
    with open('entradaLog', 'r') as file:
        log = file.readlines()

    listaCommit = []
    listaStart = []
    listaCKPT = []
    listaOperacoes = []
    listaUndo = []
    for line in log:
        if line.startswith("<END CKPT>"):
            break
        if line.startswith("<start"):
            listaStart.append(line)
        elif line.startswith("<T"):
            listaOperacoes.append(line)
        elif line.startswith("<commit"):
            listaCommit.append(line.strip("<>"))
        elif line.startswith("<START CKPT"):
            transacao = re.findall(r'T\d', line)
            if transacao:
                listaCKPT.extend(transacao)

    listaStart.reverse()
    listaOperacoes.reverse()
    listaCommit.reverse()

    print(f"Lista de Starts: {listaStart}")
    print(f"Lista de Operacoes: {listaOperacoes}")
    print(f"Lista de Commits: {listaCommit}")
    print(f"Lista de Checkpoints: {listaCKPT}")

    for commit in listaCommit:
        transacao = commit.split()[1]
        listaUndo.append(transacao)

    print(f"Lista de Undo: {listaUndo}")
    undo(listaCKPT, listaOperacoes, listaCommit, listaStart)

def undo(CKPT, OP, COMMIT, START):
    undo = []
    listaOperacao = []
    print(COMMIT)
    for commmit in COMMIT:
        commmit = re.sub('[<>]', '', commmit)
        commmit = re.split(" ", commmit)

        print(f"Transação {commmit[1]} realizou UNDO")
        undo.append(commmit[1])

        if len(CKPT) >= 1 and commmit[1] in CKPT:
            CKPT.remove(commmit[1])

    if len(CKPT) >= 1:     
        for trn in CKPT:
            print(f"Transação {trn} não realizou UNDO")
    print(undo)
    for un in undo:
        for operacao in OP:
            match = re.search(un, operacao)
            if match:
                listaOperacao.append(operacao)
  
    listaOperacao.reverse()

    for operacao in listaOperacao:
        operacao = re.sub('[<>]', '', operacao)
        operacao = re.split(",", operacao)

        idTupla = operacao[1]
        coluna = operacao[2]
        valorAntigo = operacao[3]

        try:
            conn = connectDatabase()
            cur = conn.cursor()

            command = ("""SELECT """ + coluna + """ FROM dados WHERE id = """ + idTupla)

            cur.execute(command)
            tuple = cur.fetchone()[0]

            if tuple == valorAntigo:
                break
            else:
                command = ("""UPDATE dados SET """ + coluna + """ = """ + valorAntigo + """ where id = """ + idTupla)
                cur.execute(command)

                printInitial(cur)
                cur.close()
                conn.commit()

        finally:
            closeDatabase(conn)

def printInitial(cur):
    id = []
    a = []
    b = []

    cur.execute('SELECT * FROM dados ORDER BY id')
    tuples = cur.fetchall()

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