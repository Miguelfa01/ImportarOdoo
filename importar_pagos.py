# -*- coding: utf-8 -*-
# Guardar como: importar_pagos.py

import pandas as pd
from decimal import Decimal, InvalidOperation
from conexion_mysql import conectar
import sys
import numpy as np

print("\n--- Script: importar_pagos.py ---")

# --- Configuración ---
ARCHIVO_EXCEL_PAGOS = "C:\mysql_Import\Pagos (account.payment) encabezado.xlsx" # <-- ¡¡CONFIRMA RUTA Y NOMBRE!!
NOMBRE_HOJA_EXCEL = "Sheet1" # <-- ¡¡CONFIRMA NOMBRE HOJA!!

# Mapeo de columnas: Clave = Nombre EXACTO en Excel, Valor = Nombre interno
COLUMN_MAPPING = {
    "ID": "idodoo_pago",
    "Cliente/Proveedor/ID": "idodoo_cliente",
    "Diario": "diario",
    "Estado": "estado",
    "Fecha": "fecha_pago",
    "Importe con signo en la moneda de la compañía": "monto",
    "Número": "referencia",
    # Ignoramos las otras columnas como Cliente/Proveedor (nombre), Método, etc.
}

NOMBRE_TABLA_PAGOS = "pagos"

# --- Variables Globales y Contadores ---
conexion = None
cursor = None
proceso_exitoso = False
pagos_leidos_excel = 0
pagos_procesados_bd = 0 # Inserts/Updates exitosos
pagos_cancelados_encontrados = 0
pagos_eliminados_bd = 0
pagos_omitidos_no_cliente = 0
pagos_con_error_fila = 0

# --- Funciones Auxiliares ---
def limpiar_decimal_pagos(valor):
    """Limpia y convierte valor a Decimal. Maneja comas y errores."""
    if pd.isna(valor):
        return Decimal('0.0')
    try:
        valor_str = str(valor).replace(',', '.').strip()
        if valor_str.lower() in ["<na>", "nan", "none", "", "#n/a", "false"]:
            return Decimal('0.0')
        # Quitar signo si existe (asumimos que el monto siempre es positivo en la tabla)
        # Si necesitas manejar pagos negativos (ej. devoluciones), ajusta esta lógica
        if valor_str.startswith('-'):
            valor_str = valor_str[1:]
        elif valor_str.startswith('+'):
            valor_str = valor_str[1:]

        dec_valor = Decimal(valor_str)
        if not dec_valor.is_finite():
            return Decimal('0.0')
        return dec_valor
    except (InvalidOperation, ValueError, TypeError):
        return Decimal('0.0')

def limpiar_int_pagos(valor):
    """Limpia y convierte a int, devolviendo None si no es posible."""
    if pd.isna(valor):
        return None
    try:
        return int(float(valor))
    except (ValueError, TypeError):
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
    
    #print(f"[DB] Vaciando tabla '{NOMBRE_TABLA_PAGOS}'...")
    #try:
    #    cursor.execute(f"TRUNCATE TABLE {NOMBRE_TABLA_PAGOS};")
    #    print(f"[OK] Comando TRUNCATE ejecutado.")
    #    tabla_truncada = True
    #except Exception as e_truncate: raise Exception(f"Fallo al truncar tabla: {e_truncate}")
    
    # 2. OBTENER MAPEO DE CLIENTES (idodoo -> id)
    print("[DB] Obteniendo mapeo de IDs de Clientes desde la BD...")
    cursor.execute("SELECT id, idodoo FROM clientes WHERE idodoo IS NOT NULL")
    clientes_db = cursor.fetchall()
    clientes_dict = {int(c['idodoo']): c['id'] for c in clientes_db if c.get('idodoo') and isinstance(c['idodoo'], (int, float, str)) and str(c['idodoo']).isdigit()}
    if not clientes_dict:
        print("[WARN] No se encontraron clientes con ID de Odoo numérico en la BD. No se pueden vincular pagos.")
        # Decidimos continuar, pero las filas sin cliente encontrado serán omitidas.
    print(f"[OK] Mapeo de {len(clientes_dict)} clientes obtenido.")

    # 3. LEER EXCEL DE PAGOS
    print(f"[INFO] Leyendo archivo Excel de Pagos: {ARCHIVO_EXCEL_PAGOS} (Hoja: {NOMBRE_HOJA_EXCEL})")
    try:
        df_pagos = pd.read_excel(ARCHIVO_EXCEL_PAGOS, sheet_name=NOMBRE_HOJA_EXCEL, engine="openpyxl", dtype=str)
        pagos_leidos_excel = len(df_pagos)
        if pagos_leidos_excel == 0:
            print("[INFO] El archivo Excel de pagos está vacío. Proceso completado.")
            proceso_exitoso = True
            sys.exit(0)
        print(f"[INFO] Archivo leído. {pagos_leidos_excel} pagos encontrados.")
    except FileNotFoundError:
        print(f"[ERROR] Fatal: No se encontró el archivo Excel: {ARCHIVO_EXCEL_PAGOS}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Fatal al leer el archivo Excel de pagos: {e}")
        sys.exit(1)

    # 4. PREPARAR DATAFRAME
    print("[INFO] Preparando datos del DataFrame de pagos...")
    df_pagos = df_pagos.rename(columns=COLUMN_MAPPING)

    # Verificar columnas mapeadas requeridas
    columnas_requeridas = ['idodoo_pago', 'idodoo_cliente', 'estado', 'fecha_pago', 'monto']
    columnas_presentes = df_pagos.columns.tolist()
    columnas_faltantes = [col for col in columnas_requeridas if col not in columnas_presentes]
    if columnas_faltantes:
        msg = f"Faltan columnas mapeadas esenciales: {', '.join(columnas_faltantes)}. Verifica COLUMN_MAPPING y el Excel."
        print(f"[ERROR] Fatal: {msg}")
        sys.exit(1)

    # Limpiar y convertir tipos de datos
    print("[INFO] Limpiando y convirtiendo tipos de datos...")
    df_pagos['idodoo_pago'] = df_pagos['idodoo_pago'].apply(limpiar_int_pagos)
    df_pagos['idodoo_cliente'] = df_pagos['idodoo_cliente'].apply(limpiar_int_pagos)
    df_pagos['monto'] = df_pagos['monto'].apply(limpiar_decimal_pagos)
    df_pagos['fecha_pago'] = pd.to_datetime(df_pagos['fecha_pago'], errors='coerce').dt.date

    # Convertir NaNs restantes a None para SQL
    df_pagos = df_pagos.replace({np.nan: None})
    print("[OK] Datos de pagos preparados.")

    # 5. PROCESAR FILAS (DELETE o INSERT/UPDATE)
    print(f"[INFO] Procesando {pagos_leidos_excel} pagos para DELETE/INSERT/UPDATE en '{NOMBRE_TABLA_PAGOS}'...")

    # Columnas para INSERT/UPDATE (excluyendo 'id' y 'estado' que no guardamos)
    columnas_db = ['idodoo_pago', 'id_cliente', 'fecha_pago', 'monto', 'diario', 'referencia']
    placeholders = ', '.join(['%s'] * len(columnas_db))
    update_parts = [f"{col}=VALUES({col})" for col in columnas_db if col != 'idodoo_pago'] # No actualizar idodoo_pago
    update_sql = ', '.join(update_parts)

    sql_upsert = f"""
        INSERT INTO {NOMBRE_TABLA_PAGOS} ({', '.join(columnas_db)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_sql}
    """
    sql_delete = f"DELETE FROM {NOMBRE_TABLA_PAGOS} WHERE idodoo_pago = %s"

    for index, row in df_pagos.iterrows():
        print(f"\rProcesando pago Excel {index + 1}/{pagos_leidos_excel}...", end="")

        idodoo_pago_actual = row.get('idodoo_pago')
        estado_actual = str(row.get('estado', '')).lower() # Convertir a minúsculas para comparar

        # Validar ID de Pago Odoo
        if idodoo_pago_actual is None:
            #print(f"\n[WARN] Fila Excel {index + 2} omitida: Falta 'idodoo_pago'.")
            #pagos_con_error_fila += 1
            continue

        try:
            # --- Lógica para Pagos Cancelados ---
            if estado_actual == 'cancel':
                pagos_cancelados_encontrados += 1
                # Intentar borrar el pago si existe en la BD
                cursor.execute(sql_delete, (idodoo_pago_actual,))
                if cursor.rowcount > 0:
                    pagos_eliminados_bd += 1
                    # print(f"\n[INFO] Pago cancelado (ID Odoo: {idodoo_pago_actual}) eliminado de la BD.") # Debug
                # No continuar con insert/update para este pago
                continue

            # --- Lógica para Pagos Válidos (No cancelados) ---
            idodoo_cliente_actual = row.get('idodoo_cliente')
            id_cliente_interno = None
            if idodoo_cliente_actual is not None:
                id_cliente_interno = clientes_dict.get(idodoo_cliente_actual)

            # Validar si encontramos el cliente interno
            if id_cliente_interno is None:
                print(f"\n[WARN] Fila Excel {index + 2} (Pago Odoo: {idodoo_pago_actual}) omitida: Cliente Odoo ID '{idodoo_cliente_actual}' no encontrado en la tabla 'clientes'.")
                pagos_omitidos_no_cliente += 1
                continue

            # Preparar valores para UPSERT
            valores_tupla = (
                idodoo_pago_actual,
                id_cliente_interno,
                row.get('fecha_pago'),
                str(row.get('monto', Decimal('0.0'))), # Convertir Decimal a string
                row.get('diario'),
                row.get('referencia')
            )

            # Ejecutar UPSERT
            cursor.execute(sql_upsert, valores_tupla)
            pagos_procesados_bd += 1

        except Exception as e:
            print(f"\n[ERROR] en fila Excel {index + 2} (Pago Odoo: {idodoo_pago_actual}): {e}")
            # print("      Datos de la fila:", row.to_dict()) # Descomentar para depurar
            pagos_con_error_fila += 1
            # Continuar con la siguiente fila

    print(f"\n[INFO] Procesamiento de {pagos_leidos_excel} pagos de Excel completado.")

    # 6. COMMIT o ROLLBACK
    # Haremos commit si no hubo errores graves, incluso si algunos fueron omitidos
    if pagos_con_error_fila == 0:
        print("\n[DB] Realizando COMMIT de los cambios en pagos...")
        conexion.commit()
        proceso_exitoso = True
        print("(+) Commit realizado.")
    else:
        print(f"\n[WARN] Hubo {pagos_con_error_fila} errores durante el procesamiento.")
        print("[DB] Realizando ROLLBACK para deshacer todos los cambios...")
        conexion.rollback()
        proceso_exitoso = False
        print("(-) Rollback realizado.")


except Exception as e_general:
    print(f"\n[ERROR] ERROR GENERAL INESPERADO (Importación Pagos): {e_general}")
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
    print("\n--- Resumen Importación Pagos ---")
    print(f"Total pagos leídos del Excel     : {pagos_leidos_excel}")
    print(f"Pagos Insertados/Actualizados BD : {pagos_procesados_bd}")
    print(f"Pagos Cancelados encontrados     : {pagos_cancelados_encontrados}")
    print(f"Pagos Existentes Eliminados (BD) : {pagos_eliminados_bd}")
    print("--------------------------------------")
    print(f"Pagos Omitidos (Cliente no encontrado): {pagos_omitidos_no_cliente}")
    print(f"Pagos con Error de Procesamiento   : {pagos_con_error_fila}")
    print("======================================")

    # 8. CERRAR RECURSOS
    if cursor:
        cursor.close()
        print("[DB] Cursor de pagos cerrado.")
    if conexion and conexion.is_connected():
        conexion.close()
        print("[DB] Conexión a MySQL cerrada.")

# 9. SALIDA FINAL DEL SCRIPT
if proceso_exitoso:
    print("\n[OK] Script de importación de pagos finalizado correctamente.")
    sys.exit(0) # Éxito
else:
    print("\n[ERROR] Script de importación de pagos finalizado con errores.")
    sys.exit(1) # Error