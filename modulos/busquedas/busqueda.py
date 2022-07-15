from modulos.db.dbconn import conectarDB

def cargar_datos():
    sqlQry = """SELECT * 
    FROM auditorias a INNER JOIN auditoriasprecios a3
	ON a.IdAuditoria = a3.IdAuditoria 
    WHERE SUBSTR(a.Fecha,4,7) ='03-2022'"""

    dataBase = conectarDB()
    # preparing a cursor object
    cursorObject = dataBase.cursor()
  
    cursorObject.execute(sqlQry)
   
    myresult = cursorObject.fetchall()
   
    for x in myresult:
        print(x)
 
    # disconnecting from server
    dataBase.close()