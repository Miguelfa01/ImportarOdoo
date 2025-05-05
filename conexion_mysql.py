import mysql.connector

# 📌 Función para conectar con MySQL
def conectar():
    conexion = mysql.connector.connect(
    host="localhost",
    user="root",  # Cambia por tu usuario de MySQL
    password="123456789",  # Cambia por tu contraseña de MySQL
    database="bdfenix"  # Cambia por el nombre de tu base de datos
    )
    return conexion
