import mysql.connector
from mysql.connector import Error 

def database_connection():
    
    try:
        connection=mysql.connector.connect(
            host="localhost",
            user='root',
            password="",
            database_name="",
            port=""
        )
        
        if connection.is_connected():
            print("connection successful")
            return connection
        else:
            print("connection unsuccessful")
    
    except  Error as e:
        
        print("connection error:",e)

conn=database_connection()