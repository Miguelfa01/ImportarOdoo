import mysql.connector

# Conectar a MySQL
conexion = mysql.connector.connect(
    host="localhost",
    user="root",  # Cambia por tu usuario de MySQL
    password="123456789",  # Cambia por tu contraseña de MySQL
    database="bdfenix"  # Cambia por el nombre de tu base de datos
)

cursor = conexion.cursor()
print("Conexión exitosa ✅")

import pandas as pd

# Leer la hoja "Hoja2" del archivo Excel
df = pd.read_excel("C:/mysql_import/vendedores.xlsx", sheet_name="Hoja2", engine="openpyxl")
df.columns = ["nombre"]


for index, row in df.iterrows():
    cursor.execute("INSERT INTO vendedores (nombre, supervisor) VALUES (%s, %s)", (row["nombre"], None))

conexion.commit()
cursor.close()
conexion.close()

print("Importación completada 🚀")
