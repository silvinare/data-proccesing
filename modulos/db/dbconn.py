import mysql.connector

def conectarDB():

    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="data"
    )
    return mydb;
