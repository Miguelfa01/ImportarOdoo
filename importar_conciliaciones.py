# -*- coding: utf-8 -*-
# Guardar como: importar_conciliaciones.py

import pandas as pd
from decimal import Decimal, InvalidOperation, DivisionByZero
from conexion_mysql import conectar
import sys
import numpy as np

print("\n--- Script: importar_conciliaciones.py ---")

# --- Configuración ---
ARCHIVO_EXCEL_ASIENTOS = "C:/mysql_import/Asientos_Contables_con_Conciliacion.xlsx" # <-- ¡¡CONFIRMA RUTA Y NOMBRE!!
NOMBRE_HOJA_EXCEL = "Sheet1" # <-- ¡¡CONFIRMA NOMBRE HOJA!!
PREFIJO_FACTURA_ESPERADO = "NV-" # <-- AJUSTA si tus números de factura usan otro prefijo o no usan

# Mapeo de columnas: Clave = Nombre EXACTO en Excel, Valor = Nombre interno
COLUMN_MAPPING = {
    "Fecha": "fecha_asiento",
    "Pago/ID": "idodoo_pago",
    "Apuntes contables/Débitos conciliados/ID": "idodoo_conciliacion",
    "Apuntes contables/Débitos conciliados/Importe": "monto_aplicado_str",
    "Apuntes contables/Débitos conciliados/Importe en moneda del haber": "monto_vef_str",
    # Asegúrate que este nombre de columna Excel sea EXACTO
    "Apuntes contables/Débitos conciliados/Movimiento de débito": "num_factura_aplicada_raw",
    # Columnas necesarias para fill-down (si aplica)
    "Diario": "diario_asiento",
    "Número": "numero_asiento",
    "Referencia": "referencia_asiento",
    "ID": "id_linea_asiento",
}

# Nombre de la tabla de destino en MySQL
NOMBRE_TABLA_CONCILIADOS = "pago_conciliados"

# --- Variables Globales y Contadores ---
conexion = None
cursor = None
proceso_exitoso = False
lineas_leidas_excel = 0
# Inicializar variables que se usan en finally para evitar NameError si falla antes
num_filas_conciliacion = None
conciliaciones_procesadas_bd = 0
conciliaciones_omitidas_no_info = 0
conciliaciones_omitidas_no_pago = 0
conciliaciones_omitidas_no_factura = 0
conciliaciones_con_error_fila = 0

# --- Funciones Auxiliares ---
def limpiar_decimal_conc(valor):
    if pd.isna(valor): return Decimal('0.0')
    try:
        valor_str = str(valor).replace(',', '.').strip()
        if valor_str.lower() in ["<na>", "nan", "none", "", "#n/a", "false"]: return Decimal('0.0')
        dec_valor = Decimal(valor_str)
        if not dec_valor.is_finite(): return Decimal('0.0')
        return dec_valor
    except (InvalidOperation, ValueError, TypeError): return Decimal('0.0')

def limpiar_int_conc(valor):
    if pd.isna(valor): return None
    try: return int(float(valor))
    except (ValueError, TypeError): return None

# --- Nueva versión de la función ---
def extraer_num_factura_limpio(valor_raw):
    """
    Extrae el número de factura de un string, tomando todo
    desde el inicio hasta el primer espacio encontrado.
    Si no hay espacio, toma todo el string.
    """
    if pd.isna(valor_raw):
        return None
    try:
        # Convertir a string y quitar espacios al inicio/final
        valor_str = str(valor_raw).strip()
        if not valor_str:
            return None # Si está vacío después de limpiar, devolver None

        # Buscar la posición del primer espacio
        valor_str=valor_str+" "
        posicion_espacio = valor_str.find(' ')

        if posicion_espacio != -1:
            # Si se encontró un espacio, extraer desde el inicio hasta esa posición
            num_factura = valor_str[:posicion_espacio]
        else:
            # Si no se encontró espacio, usar el string completo (ya limpiado)
            num_factura = valor_str

        # Devolver el resultado (asegurándose de que no sea vacío por si acaso)
        return num_factura if num_factura else None

    except Exception as e:
        # print(f"[DEBUG] Excepción en extraer_num_factura_limpio para '{valor_raw}': {e}") # Debug
        return None
# --- Fin Nueva versión ---

    except Exception as e:
        # print(f"[DEBUG] Excepción en extraer_num_factura_limpio para '{valor_raw}': {e}") # Debug
        return None

# --- Lógica Principal ---
try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion: raise Exception("No se pudo conectar a la base de datos.")
    cursor = conexion.cursor(dictionary=True)
    print("[OK] Conexión establecida.")
    
    print(f"[DB] Vaciando tabla '{NOMBRE_TABLA_CONCILIADOS}'...")
    try:
        cursor.execute(f"TRUNCATE TABLE {NOMBRE_TABLA_CONCILIADOS};")
        print(f"[OK] Comando TRUNCATE para '{NOMBRE_TABLA_CONCILIADOS}' ejecutado.")
        
    except Exception as e_truncate:
        print(f"[ERROR] Fatal: No se pudo truncar la tabla '{NOMBRE_TABLA_CONCILIADOS}': {e_truncate}")
        raise Exception(f"Fallo al truncar tabla: {e_truncate}")

    # 2. OBTENER MAPEOS NECESARIOS DESDE DB
    print("[DB] Obteniendo mapeo de IDs de Pagos desde la BD...")
    cursor.execute("SELECT id, idodoo_pago FROM pagos WHERE idodoo_pago IS NOT NULL")
    pagos_db = cursor.fetchall()
    pagos_dict = {int(p['idodoo_pago']): p['id'] for p in pagos_db if p.get('idodoo_pago') and isinstance(p['idodoo_pago'], (int, float, str)) and str(p['idodoo_pago']).isdigit()}
    if not pagos_dict: print("[WARN] No se encontraron pagos con ID de Odoo en la tabla 'pagos'.")
    print(f"[OK] Mapeo de {len(pagos_dict)} pagos obtenido.")

    print("[DB] Obteniendo mapeo de Números de Factura desde la BD...")
    cursor.execute("SELECT id, num_factura FROM facturas WHERE num_factura IS NOT NULL AND num_factura != ''")
    facturas_db = cursor.fetchall()
    facturas_dict = {f['num_factura'].strip(): f['id'] for f in facturas_db if f.get('num_factura')}
    if not facturas_dict: print("[WARN] No se encontraron facturas con número de factura en la BD.")
    print(f"[OK] Mapeo de {len(facturas_dict)} facturas obtenido.")

    # 3. LEER EXCEL DE ASIENTOS CONTABLES
    print(f"[INFO] Leyendo archivo Excel de Asientos: {ARCHIVO_EXCEL_ASIENTOS} (Hoja: {NOMBRE_HOJA_EXCEL})")
    try:
        df_asientos = pd.read_excel(ARCHIVO_EXCEL_ASIENTOS, sheet_name=NOMBRE_HOJA_EXCEL, engine="openpyxl", dtype=str)
        lineas_leidas_excel = len(df_asientos)
    except FileNotFoundError:
        print(f"[ERROR] Fatal: No se encontró el archivo Excel: {ARCHIVO_EXCEL_ASIENTOS}")
        raise # Relanzar la excepción para que el bloque principal la capture
    except Exception as e:
        print(f"[ERROR] Fatal al leer el archivo Excel de asientos: {e}")
        raise # Relanzar

    if lineas_leidas_excel == 0:
        print("[INFO] El archivo Excel de asientos está vacío. Proceso completado.")
        proceso_exitoso = True
        # Usar sys.exit(0) aquí podría evitar que el finally se ejecute correctamente
        # Es mejor dejar que el script termine normalmente después del finally
    else:
        print(f"[INFO] Archivo leído. {lineas_leidas_excel} líneas de asiento encontradas.")

        # 4. PREPARAR DATAFRAME
        print("[INFO] Preparando datos del DataFrame de asientos...")
        df_asientos = df_asientos.rename(columns=COLUMN_MAPPING)

        columnas_requeridas = ['fecha_asiento', 'idodoo_pago', 'idodoo_conciliacion',
                            'monto_aplicado_str', 'monto_vef_str', 'num_factura_aplicada_raw']
        columnas_presentes = df_asientos.columns.tolist()
        columnas_faltantes = [col for col in columnas_requeridas if col not in columnas_presentes]
        if columnas_faltantes:
            msg = f"Faltan columnas mapeadas esenciales: {', '.join(columnas_faltantes)}. Verifica COLUMN_MAPPING y el Excel."
            print(f"[ERROR] Fatal: {msg}")
            raise ValueError(msg) # Lanzar excepción

        fill_down_cols = ['diario_asiento', 'fecha_asiento', 'numero_asiento',
                        'referencia_asiento', 'id_linea_asiento', 'idodoo_pago']
        print("[INFO] Aplicando lógica 'fill-down' a columnas relevantes...")
        for col in fill_down_cols:
            if col in df_asientos.columns:
                df_asientos[col] = df_asientos[col].replace(['', ' ', '<NA>', 'None', None, 'FALSE', 'False', 'false'], np.nan)
                df_asientos[col] = df_asientos[col].ffill()
            else: print(f"[WARN] Columna '{col}' para fill-down no encontrada.")

        print("[INFO] Filtrando filas que contienen información de conciliación...")
        df_conciliaciones = df_asientos.dropna(subset=['idodoo_conciliacion']).copy()
        num_filas_conciliacion = len(df_conciliaciones) # Definir aquí
        conciliaciones_omitidas_no_info = lineas_leidas_excel - num_filas_conciliacion
        print(f"[OK] {num_filas_conciliacion} filas con datos de conciliación encontradas.")
        if conciliaciones_omitidas_no_info > 0: print(f"[INFO] {conciliaciones_omitidas_no_info} líneas ignoradas (sin ID conciliación).")

        if num_filas_conciliacion > 0:
            print("[INFO] Limpiando y convirtiendo tipos de datos para conciliaciones...")
            df_conciliaciones['idodoo_conciliacion'] = df_conciliaciones['idodoo_conciliacion'].apply(limpiar_int_conc)
            df_conciliaciones['idodoo_pago'] = df_conciliaciones['idodoo_pago'].apply(limpiar_int_conc)
            df_conciliaciones['monto_aplicado'] = df_conciliaciones['monto_aplicado_str'].apply(limpiar_decimal_conc)
            df_conciliaciones['Monto_vef'] = df_conciliaciones['monto_vef_str'].apply(limpiar_decimal_conc)
            df_conciliaciones['fecha_aplicacion'] = pd.to_datetime(df_conciliaciones['fecha_asiento'], errors='coerce').dt.date

            print("[INFO] Extrayendo número de factura aplicado...")
            #df_conciliaciones['num_factura_aplicada'] = df_conciliaciones['num_factura_aplicada_raw'].apply(lambda x: extraer_num_factura_limpio(x, ["NV-", "00-"]))
            df_conciliaciones['num_factura_aplicada'] = df_conciliaciones['num_factura_aplicada_raw'].apply(extraer_num_factura_limpio)

            print("[INFO] Calculando tasa de cambio (Monto_vef / monto_aplicado)...")
            def calcular_tasa(row):
                monto_vef = row['Monto_vef']
                monto_aplicado = row['monto_aplicado']
                if monto_aplicado is not None and monto_aplicado != Decimal('0.0') and monto_vef is not None:
                    try: return (monto_vef / monto_aplicado).quantize(Decimal('0.00000001'))
                    except (InvalidOperation, DivisionByZero): return Decimal('0.0')
                return Decimal('0.0')
            df_conciliaciones['tasa'] = df_conciliaciones.apply(calcular_tasa, axis=1)

            print("[INFO] Mapeando IDs internos de Pago y Factura...")
            df_conciliaciones['id_pago'] = df_conciliaciones['idodoo_pago'].map(pagos_dict)
            df_conciliaciones['id_factura'] = df_conciliaciones['num_factura_aplicada'].map(facturas_dict)

            conciliaciones_omitidas_no_pago = df_conciliaciones['id_pago'].isna().sum()
            conciliaciones_omitidas_no_factura = df_conciliaciones['id_factura'].isna().sum()

            df_conciliaciones = df_conciliaciones.replace({np.nan: None})
            print("[OK] Datos de conciliaciones preparados.")

            # --- Mensaje de Advertencia Mejorado ---
            if conciliaciones_omitidas_no_pago > 0:
                print(f"[WARN] {conciliaciones_omitidas_no_pago} conciliaciones omitidas (Pago no encontrado en BD).")
            if conciliaciones_omitidas_no_factura > 0:
                nums_no_encontrados = df_conciliaciones.loc[df_conciliaciones['id_factura'].isna() & df_conciliaciones['num_factura_aplicada'].notna(), 'num_factura_aplicada'].unique()
                descripciones_o_malformados = []
                raw_col_name = 'num_factura_aplicada_raw'
                if raw_col_name in df_conciliaciones.columns: # Verificar si existe
                    descripciones_o_malformados = df_conciliaciones.loc[
                        df_conciliaciones['id_factura'].isna() & df_conciliaciones['num_factura_aplicada'].isna(),
                        raw_col_name
                    ].unique()
                else:
                    print(f"[WARN] Columna original '{raw_col_name}' no encontrada para mostrar ejemplos.")

                print(f"[WARN] {conciliaciones_omitidas_no_factura} conciliaciones omitidas (Factura no encontrada en BD):")
                if len(nums_no_encontrados) > 0: print(f"         - Números no encontrados (ej: '{nums_no_encontrados[0]}'...).")
                if len(descripciones_o_malformados) > 0: print(f"         - Campo fuente no válido (ej: '{str(descripciones_o_malformados[0])[:60]}'...).")
            # --- Fin Mensaje Mejorado ---

            # 5. PROCESAR FILAS (INSERT / UPDATE)
            print(f"[INFO] Procesando {num_filas_conciliacion} conciliaciones para INSERT/UPDATE en '{NOMBRE_TABLA_CONCILIADOS}'...")
            columnas_db = ['id_pago', 'id_factura', 'idodoo_conciliacion',
                        'monto_aplicado', 'Monto_vef', 'tasa', 'fecha_aplicacion']
            placeholders = ', '.join(['%s'] * len(columnas_db))
            update_parts = [f"{col}=VALUES({col})" for col in columnas_db if col != 'idodoo_conciliacion']
            update_sql = ', '.join(update_parts)
            sql_upsert = f"""
                INSERT INTO {NOMBRE_TABLA_CONCILIADOS} ({', '.join(columnas_db)})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_sql}
            """

            for index, row in df_conciliaciones.iterrows():
                #rint(f"\rProcesando línea Excel {index + 1}/{lineas_leidas_excel} (Conciliación ID: {row.get('idodoo_conciliacion', 'N/A')})...", end="")
                if row['id_pago'] is None or row['id_factura'] is None or row['idodoo_conciliacion'] is None:
                    continue
                try:
                    valores_tupla = (
                        row.get('id_pago'), row.get('id_factura'), row.get('idodoo_conciliacion'),
                        str(row.get('monto_aplicado', Decimal('0.0'))), str(row.get('Monto_vef', Decimal('0.0'))),
                        str(row.get('tasa', Decimal('0.0'))), row.get('fecha_aplicacion')
                    )
                    cursor.execute(sql_upsert, valores_tupla)
                    conciliaciones_procesadas_bd += 1
                except Exception as e:
                    print(f"\n[ERROR] en fila Excel {index + 2} (Conciliación Odoo: {row.get('idodoo_conciliacion', 'N/A')}): {e}")
                    conciliaciones_con_error_fila += 1

            print(f"\n[INFO] Procesamiento de {num_filas_conciliacion} conciliaciones completado.")

            # 6. COMMIT o ROLLBACK
            if conciliaciones_con_error_fila == 0:
                print("\n[DB] Realizando COMMIT de los cambios en conciliaciones...")
                conexion.commit()
                proceso_exitoso = True
                print("(+) Commit realizado.")
            else:
                print(f"\n[WARN] Hubo {conciliaciones_con_error_fila} errores.")
                print("[DB] Realizando ROLLBACK...")
                conexion.rollback()
                proceso_exitoso = False
                print("(-) Rollback realizado.")
        else: # Si num_filas_conciliacion == 0
            print("[INFO] No hubo conciliaciones válidas que procesar después del filtrado.")
            proceso_exitoso = True # No hubo errores, solo no había datos

# --- Bloques except y finally ---
except Exception as e_general:
    print(f"\n[ERROR] ERROR GENERAL INESPERADO (Importación Conciliaciones): {e_general}")
    proceso_exitoso = False
    if conexion:
        try:
            print("[DB] Intentando realizar ROLLBACK...")
            conexion.rollback()
            print("(-) Rollback realizado.")
        except Exception as rb_err:
            print(f"[WARN] Error durante el rollback: {rb_err}")
finally:
    # 7. MOSTRAR RESUMEN
    def safe_print(var_name, value):
        # Imprime '--' si el valor es None (porque el script falló antes de calcularlo)
        display_value = value if value is not None else '--'
        print(f"{var_name:<40}: {display_value}")

    print("\n--- Resumen Importación Conciliaciones ---")
    safe_print("Total líneas leídas del Excel", locals().get('lineas_leidas_excel'))
    safe_print("Líneas con datos de conciliación", locals().get('num_filas_conciliacion'))
    safe_print("Conciliaciones Insertadas/Actualizadas", locals().get('conciliaciones_procesadas_bd'))
    print("-------------------------------------------")
    safe_print("Líneas Ignoradas (Sin ID Conciliación)", locals().get('conciliaciones_omitidas_no_info'))
    safe_print("Conciliaciones Omitidas (Pago no encontrado)", locals().get('conciliaciones_omitidas_no_pago'))
    safe_print("Conciliaciones Omitidas (Factura no encontrada)", locals().get('conciliaciones_omitidas_no_factura'))
    safe_print("Conciliaciones con Error Procesamiento", locals().get('conciliaciones_con_error_fila'))
    print("===========================================")

    # 8. CERRAR RECURSOS
    if cursor: cursor.close(); print("[DB] Cursor de conciliaciones cerrado.")
    if conexion and conexion.is_connected(): conexion.close(); print("[DB] Conexión a MySQL cerrada.")

# 9. SALIDA FINAL DEL SCRIPT
if 'proceso_exitoso' in locals() and proceso_exitoso:
    print("\n[OK] Script de importación de conciliaciones finalizado correctamente.")
    sys.exit(0)
else:
    print("\n[ERROR] Script de importación de conciliaciones finalizado con errores.")
    sys.exit(1)