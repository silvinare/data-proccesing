import pandas as pd
from modulos.db.dbconn import conectarDB

sqlQry = """SELECT * 
    FROM auditorias a INNER JOIN auditoriasprecios a3
	ON a.IdAuditoria = a3.IdAuditoria 
    WHERE SUBSTR(a.Fecha,4,7) ='03-2022'"""

df = pd.read_sql(sqlQry, conectarDB())

df.to_csv('file001.csv')