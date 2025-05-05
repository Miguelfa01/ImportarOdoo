# -*- coding: utf-8 -*-
# Guardar como: importar_detalles_factura.py

import pandas as pd
from decimal import Decimal, InvalidOperation # Usar Decimal para precisión
from conexion_mysql import conectar
import sys
import numpy as np # Para reemplazar infinitos si ocurren

print("\n--- Script: importar_detalles_factura.py ---")

# --- Configuración ---
ARCHIVO_EXCEL_DETALLES = "C:/mysql_import/Asiento contable (account.move) - detalle.xlsx" # <-- CONFIRMA RUTA
NOMBRE_HOJA_EXCEL = "Sheet1" # <-- CONFIRMA NOMBRE HOJA

# Mapeo de columnas: Clave = Nombre EXACTO en Excel, Valor = Nombre interno en script/DataFrame
COLUMN_MAPPING = {
    "ID": "idodoo_factura",
    "Número": "num_factura",
    "Líneas de factura/Producto/Nombre": "nombre_Producto",
    "Líneas de factura/Producto/Precio de venta": "precio_venta",
    "Líneas de factura/Cantidad": "cantidad",
    "Líneas de factura/Producto/Peso": "galonaje",
    "Líneas de factura/Producto/ID": "idodoo_producto",
    "Líneas de factura/ID": "idodoo_linea", # El ID único de la línea
    "Líneas de factura/Producto/Referencia": "Cod_producto"
    # Asegúrate de que no falta la columna 'Subtotal' aquí, ya que no viene del Excel
}

# Nombre de la tabla en MySQL
NOMBRE_TABLA_DETALLE = "factura_detalle"

# --- Variables Globales y Contadores ---
conexion = None
cursor = None
proceso_exitoso = False
lineas_leidas_excel = 0
lineas_procesadas_bd = 0 # Cuenta inserts y updates exitosos
lineas_omitidas_no_factura = 0
lineas_omitidas_no_id_linea = 0
lineas_con_error_fila = 0

# --- Funciones Auxiliares ---
def limpiar_decimal(valor):
    """Limpia y convierte valor a Decimal. Maneja comas y errores."""
    if pd.isna(valor):
        return Decimal('0.0')
    try:
        # Convertir a string, reemplazar coma por punto, quitar espacios
        valor_str = str(valor).replace(',', '.').strip()
        if valor_str.lower() in ["<na>", "nan", "none", "", "#n/a", "false"]:
            return Decimal('0.0')
        # Intentar convertir a Decimal
        dec_valor = Decimal(valor_str)
        # Reemplazar infinito o NaN resultante (raro pero posible)
        if not dec_valor.is_finite():
            return Decimal('0.0')
        return dec_valor
    except (InvalidOperation, ValueError, TypeError):
        # print(f"Advertencia: No se pudo convertir '{valor}' a Decimal. Usando 0.0.") # Debug
        return Decimal('0.0')

def limpiar_int(valor):
    """Limpia y convierte a int, devolviendo None si no es posible."""
    if pd.isna(valor):
        return None
    try:
        # Intentar convertir a float primero (maneja '123.0') y luego a int
        return int(float(valor))
    except (ValueError, TypeError):
        # print(f"Advertencia: No se pudo convertir '{valor}' a int. Usando None.") # Debug
        return None

# --- Lógica Principal ---
try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion:
        print("[ERROR] Fatal: No se pudo conectar a la base de datos.")
        sys.exit(1)
    cursor = conexion.cursor(dictionary=True)
    print("[OK] Conexión establecida.")

    # 2. OBTENER MAPEO DE FACTURAS (idodoo -> id)
    print("[DB] Obteniendo mapeo de IDs de Facturas desde la BD...")
    cursor.execute("SELECT id, idodoo FROM facturas WHERE idodoo IS NOT NULL")
    facturas_db = cursor.fetchall()
    # Convertir idodoo a int en el diccionario para la búsqueda
    facturas_dict = {int(f['idodoo']): f['id'] for f in facturas_db if f.get('idodoo') and isinstance(f['idodoo'], (int, float, str)) and str(f['idodoo']).isdigit()}
    if not facturas_dict:
        print("[WARN] No se encontraron facturas con ID de Odoo numérico en la BD. No se pueden vincular detalles.")
        sys.exit(1)
    print(f"[OK] Mapeo de {len(facturas_dict)} facturas obtenido.")

    # 3. LEER EXCEL DE DETALLES
    print(f"[INFO] Leyendo archivo Excel de Detalles: {ARCHIVO_EXCEL_DETALLES} (Hoja: {NOMBRE_HOJA_EXCEL})")
    try:
        # Leer sin interpretar tipos inicialmente para manejar mejor la limpieza
        df_detalles = pd.read_excel(ARCHIVO_EXCEL_DETALLES, sheet_name=NOMBRE_HOJA_EXCEL, engine="openpyxl", dtype=str)
        lineas_leidas_excel = len(df_detalles)
        if lineas_leidas_excel == 0:
            print("[INFO] El archivo Excel de detalles está vacío. Proceso completado.")
            proceso_exitoso = True
            sys.exit(0)
        print(f"[INFO] Archivo leído. {lineas_leidas_excel} líneas de detalle encontradas.")
    except FileNotFoundError:
        print(f"[ERROR] Fatal: No se encontró el archivo Excel: {ARCHIVO_EXCEL_DETALLES}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Fatal al leer el archivo Excel de detalles: {e}")
        sys.exit(1)

    # 4. PREPARAR DATAFRAME
    print("[INFO] Preparando datos del DataFrame de detalles...")
    df_detalles = df_detalles.rename(columns=COLUMN_MAPPING)

    # Verificar columnas mapeadas requeridas
    columnas_requeridas = ['idodoo_factura', 'idodoo_linea', 'nombre_Producto', 'cantidad', 'precio_venta']
    columnas_presentes = df_detalles.columns.tolist()
    columnas_faltantes = [col for col in columnas_requeridas if col not in columnas_presentes]
    if columnas_faltantes:
        msg = f"Faltan columnas mapeadas esenciales en el DataFrame: {', '.join(columnas_faltantes)}. Verifica COLUMN_MAPPING y el Excel."
        print(f"[ERROR] Fatal: {msg}")
        sys.exit(1)

    # Aplicar Lógica Fill-Down (Propagar hacia abajo) para IDs y Números de Factura
    fill_down_cols = ['idodoo_factura', 'num_factura']
    print("[INFO] Aplicando lógica 'fill-down' para idodoo_factura y num_factura...")
    for col in fill_down_cols:
        if col in df_detalles.columns:
            # Reemplazar vacíos/placeholders comunes con NaN antes de ffill
            df_detalles[col] = df_detalles[col].replace(['', ' ', '<NA>', 'None', None, 'FALSE', 'False', 'false'], np.nan)
            df_detalles[col] = df_detalles[col].ffill()
        else:
            print(f"[WARN] Columna '{col}' para fill-down no encontrada en el DataFrame (después del mapeo).")

    # Limpiar y convertir tipos de datos
    print("[INFO] Limpiando y convirtiendo tipos de datos...")
    df_detalles['idodoo_factura'] = df_detalles['idodoo_factura'].apply(limpiar_int)
    df_detalles['idodoo_linea'] = df_detalles['idodoo_linea'].apply(limpiar_int)
    df_detalles['idodoo_producto'] = df_detalles['idodoo_producto'].apply(limpiar_int)

    df_detalles['cantidad'] = df_detalles['cantidad'].apply(limpiar_decimal)
    df_detalles['precio_venta'] = df_detalles['precio_venta'].apply(limpiar_decimal)
    df_detalles['galonaje'] = df_detalles['galonaje'].apply(limpiar_decimal)

    # Calcular Subtotal
    print("[INFO] Calculando subtotal (cantidad * precio_venta)...")
    df_detalles['subtotal_calculado'] = df_detalles['cantidad'] * df_detalles['precio_venta']
    # Redondear el subtotal a la precisión de la BD (ej. 6 decimales)
    df_detalles['subtotal_calculado'] = df_detalles['subtotal_calculado'].apply(lambda x: x.quantize(Decimal('0.000001')) if isinstance(x, Decimal) else Decimal('0.0'))

    # Mapear id_factura (interno DB) usando el diccionario
    print("[INFO] Mapeando ID de factura interno...")
    def obtener_id_factura_db(idodoo_factura_limpio):
        if idodoo_factura_limpio is None:
            return None
        return facturas_dict.get(idodoo_factura_limpio) # Busca el int limpio

    df_detalles['id_factura'] = df_detalles['idodoo_factura'].apply(obtener_id_factura_db)

    # Contar omisiones iniciales
    lineas_omitidas_no_factura = df_detalles['id_factura'].isna().sum()
    lineas_omitidas_no_id_linea = df_detalles['idodoo_linea'].isna().sum()

    # Convertir NaNs restantes a None para SQL (importante hacerlo al final)
    # Usamos replace en lugar de astype/where para manejar mejor tipos mixtos post-limpieza
    df_detalles = df_detalles.replace({np.nan: None})

    print("[OK] Datos de detalles preparados.")
    if lineas_omitidas_no_factura > 0:
        print(f"[WARN] {lineas_omitidas_no_factura} líneas serán omitidas porque su 'idodoo_factura' no se encontró en la tabla 'facturas'.")
    if lineas_omitidas_no_id_linea > 0:
        print(f"[WARN] {lineas_omitidas_no_id_linea} líneas serán omitidas porque no tienen 'idodoo_linea' (necesario para UPSERT).")


    # 5. PROCESAR FILAS (INSERT / UPDATE)
    print(f"[INFO] Procesando {lineas_leidas_excel} líneas para INSERT/UPDATE en '{NOMBRE_TABLA_DETALLE}'...")

    # Construir la parte de columnas y placeholders para el INSERT
    # Obtener columnas de la tabla DB (excepto 'id' auto-incremental) y las del DataFrame
    columnas_db = [
        'id_factura', 'idodoo_factura', 'idodoo_linea', 'idodoo_producto',
        'num_factura', 'Cod_producto', 'nombre_Producto', 'cantidad',
        'precio_venta', 'galonaje', 'subtotal' # Incluimos subtotal aquí
    ]
    placeholders = ', '.join(['%s'] * len(columnas_db))

    # Construir la parte de UPDATE para ON DUPLICATE KEY
    update_parts = [f"{col}=VALUES({col})" for col in columnas_db] # Actualizar todas las columnas con los nuevos valores
    update_sql = ', '.join(update_parts)

    sql_upsert = f"""
        INSERT INTO {NOMBRE_TABLA_DETALLE} ({', '.join(columnas_db)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_sql}
    """

    for index, row in df_detalles.iterrows():
        #print(f"\rProcesando línea Excel {index + 1}/{lineas_leidas_excel}...", end="")

        # Validaciones por fila antes de intentar el UPSERT
        if row['id_factura'] is None:
            # Ya contamos esto antes, pero es bueno tener el check aquí
            continue # Saltar si no pudimos encontrar la factura padre

        if row['idodoo_linea'] is None:
            # Ya contamos esto antes
            continue # Saltar si no hay ID de línea para hacer el UPSERT

        # Preparar los valores en el orden correcto para el SQL
        # Asegurarse de que los Decimal se pasen como string para evitar problemas de precisión con el conector
        try:
            valores_tupla = (
                row.get('id_factura'),
                row.get('idodoo_factura'),
                row.get('idodoo_linea'),
                row.get('idodoo_producto'),
                row.get('num_factura'),
                row.get('Cod_producto'),
                row.get('nombre_Producto'),
                # Convertir Decimal a string para la inserción
                str(row.get('cantidad', Decimal('0.0'))),
                str(row.get('precio_venta', Decimal('0.0'))),
                str(row.get('galonaje', Decimal('0.0'))),
                str(row.get('subtotal_calculado', Decimal('0.0'))) # Usar el calculado
            )

            # Ejecutar el UPSERT
            cursor.execute(sql_upsert, valores_tupla)
            lineas_procesadas_bd += 1
            # rowcount = 1 para INSERT, 2 para UPDATE en MySQL Connector/Python
            # if cursor.rowcount == 1: lineas_insertadas += 1
            # elif cursor.rowcount == 2: lineas_actualizadas += 1

        except Exception as e:
            print(f"\n[ERROR] en fila Excel {index + 2} (idodoo_linea: {row.get('idodoo_linea', 'N/A')}): {e}")
            # print("      Datos de la fila:", row.to_dict()) # Descomentar para depurar fila con error
            lineas_con_error_fila += 1
            # Continuar con la siguiente fila

    print(f"\n[INFO] Procesamiento de {lineas_leidas_excel} líneas de Excel completado.")

    # 6. COMMIT o ROLLBACK
    if lineas_con_error_fila == 0:
        print("\n[DB] Realizando COMMIT de los cambios de detalles...")
        conexion.commit()
        proceso_exitoso = True
        print("(+) Commit realizado.")
    elif lineas_procesadas_bd > 0:
        # Hubo errores, pero también éxitos. ¿Hacer commit parcial o rollback total?
        # Opción: Commit parcial (los exitosos se guardan)
        print(f"\n[WARN] Hubo {lineas_con_error_fila} errores, pero {lineas_procesadas_bd} líneas se procesaron.")
        print("[DB] Realizando COMMIT de los cambios procesados correctamente...")
        conexion.commit()
        proceso_exitoso = True # Consideramos éxito parcial
        print("(+) Commit parcial realizado.")
        # Opción alternativa: Rollback total si hubo CUALQUIER error
        # print(f"\n[WARN] Hubo {lineas_con_error_fila} errores.")
        # print("[DB] Realizando ROLLBACK para deshacer todos los cambios...")
        # conexion.rollback()
        # proceso_exitoso = False
        # print("(-) Rollback realizado.")
    else:
        # No se procesó nada o solo hubo errores
        print("\n[INFO] No se procesaron líneas correctamente o no hubo cambios válidos. No se requiere COMMIT/ROLLBACK.")
        if lineas_con_error_fila > 0:
            proceso_exitoso = False # Hubo errores, marcar como fallo
        else:
            proceso_exitoso = True # No hubo datos válidos, pero no fue un error


except Exception as e_general:
    print(f"\n[ERROR] ERROR GENERAL INESPERADO (Importación Detalles): {e_general}")
    proceso_exitoso = False
    if conexion:
        try:
            print("[DB] Intentando realizar ROLLBACK debido a error general...")
            conexion.rollback()
            print("(-) Rollback realizado.")
        except Exception as rb_err:
            print(f"[WARN] Error durante el rollback: {rb_err}")

finally:
    # 7. MOSTRAR RESUMEN
    print("\n--- Resumen Importación Detalles Factura ---")
    print(f"Total líneas leídas del Excel  : {lineas_leidas_excel}")
    print(f"Líneas Insertadas/Actualizadas : {lineas_procesadas_bd}")
    # print(f"  - Nuevas Insertadas        : {lineas_insertadas}") # Si decides contarlas por separado
    # print(f"  - Existentes Actualizadas  : {lineas_actualizadas}") # Si decides contarlas por separado
    print("-------------------------------------------")
    print(f"Líneas Omitidas (Factura no encontrada): {lineas_omitidas_no_factura}")
    print(f"Líneas Omitidas (ID de Línea faltante): {lineas_omitidas_no_id_linea}")
    print(f"Líneas con Error de Procesamiento    : {lineas_con_error_fila}")
    print("===========================================")
    total_final = lineas_procesadas_bd + lineas_omitidas_no_factura + lineas_omitidas_no_id_linea + lineas_con_error_fila
    if total_final == lineas_leidas_excel:
        print("[OK] Verificación: Suma coincide con total leído del Excel.")
    else:
        print(f"[WARN] Verificación: Suma ({total_final}) NO coincide con total leído ({lineas_leidas_excel}).")

    # 8. CERRAR RECURSOS
    if cursor:
        cursor.close()
        print("[DB] Cursor de detalles cerrado.")
    if conexion and conexion.is_connected():
        conexion.close()
        print("[DB] Conexión a MySQL cerrada.")

# 9. SALIDA FINAL DEL SCRIPT (para subprocess si se usa)
if proceso_exitoso:
    print("\n[OK] Script de importación de detalles finalizado correctamente.")
    sys.exit(0) # Éxito
else:
    print("\n[ERROR] Script de importación de detalles finalizado con errores.")
    sys.exit(1) # Error