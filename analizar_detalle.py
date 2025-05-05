import pandas as pd

# --- Configura esto ---
archivo_excel = "C:/mysql_import/Asiento contable (account.move) - detalle.xlsx" # <-- ¡¡TU RUTA Y NOMBRE EXACTOS!!
nombre_hoja = "Sheet1" # <-- ¡¡TU NOMBRE DE HOJA EXACTO!!
numero_filas_mostrar = 8 # Puedes ajustar cuántas filas de ejemplo quieres ver
# ---------------------

print(f"--- Analizando: {archivo_excel} (Hoja: {nombre_hoja}) ---")

try:
    df = pd.read_excel(archivo_excel, sheet_name=nombre_hoja, engine="openpyxl")

    print("\n1. Nombres de las Columnas:")
    print(df.columns.tolist())

    print(f"\n2. Primeras {numero_filas_mostrar} filas (datos de ejemplo):")
    # Convertir a string para evitar problemas de visualización, pero cuidado si necesitas ver tipos exactos
    # print(df.head(numero_filas_mostrar).to_string())
    # Mejor opción: imprimir directamente, pandas intentará formatear
    print(df.head(numero_filas_mostrar))


    print("\n3. Información General (Tipos de datos detectados por Pandas, valores no nulos):")
    # df.info() imprime directamente a la consola
    df.info()

    print("\n4. Resumen Estadístico (para columnas numéricas):")
    # Describe solo columnas que pandas interpreta como numéricas
    print(df.describe(include='number'))

    print("\n5. Conteo de Valores Nulos por Columna:")
    print(df.isnull().sum())

    # Opcional: Ver valores únicos en columnas clave (si sabes cuáles son)
    # try:
    #     columna_id_factura_excel = "Nombre Columna ID Factura en Excel" # CAMBIA ESTO
    #     print(f"\n6. Valores únicos en '{columna_id_factura_excel}' (primeros 20):")
    #     print(df[columna_id_factura_excel].unique()[:20])
    # except KeyError:
    #     print(f"   (Columna '{columna_id_factura_excel}' no encontrada)")

    # try:
    #      columna_id_producto_excel = "Nombre Columna ID Producto en Excel" # CAMBIA ESTO
    #      print(f"\n7. Valores únicos en '{columna_id_producto_excel}' (primeros 20):")
    #      print(df[columna_id_producto_excel].unique()[:20])
    # except KeyError:
    #      print(f"   (Columna '{columna_id_producto_excel}' no encontrada)")


except FileNotFoundError:
    print(f"\n[ERROR] No se encontró el archivo: {archivo_excel}")
except Exception as e:
    print(f"\n[ERROR] Ocurrió un error al leer o analizar el archivo: {e}")

print("\n--- Fin del Análisis ---")