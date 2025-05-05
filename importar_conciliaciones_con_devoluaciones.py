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

# Mapeo de columnas: Clave = Nombre EXACTO en Excel, Valor = Nombre interno
COLUMN_MAPPING = {
    "Fecha": "fecha_asiento",
    "Pago/ID": "idodoo_pago",
    "Apuntes contables/Débitos conciliados/ID": "idodoo_conciliacion",
    "Apuntes contables/Débitos conciliados/Importe": "monto_aplicado_str",
    "Apuntes contables/Débitos conciliados/Importe en moneda del haber": "monto_vef_str",
    "Apuntes contables/Débitos conciliados/Movimiento de débito": "num_factura_aplicada_raw",
    "Diario": "diario_asiento",
    "Número": "numero_asiento",
    "Referencia": "referencia_asiento",
    "ID": "id_linea_asiento", # ID de account.move.line
}

# Nombre de las tablas en MySQL
NOMBRE_TABLA_CONCILIADOS = "pago_conciliados"
NOMBRE_TABLA_PAGOS = "pagos"

# --- ¡¡IMPORTANTE!! Define los nombres EXACTOS de los diarios que SÍ quieres procesar ---
# Incluye los diarios de pagos de clientes y el nombre que usaremos para Notas de Crédito
DIARIOS_A_EXCLUIR = [
    "Notas de proveedor",
    "Diario de Inventario", # Ejemplo
    "Notas de Gastos" # Añade aquí los nombres exactos
]
# Excluiremos "Notas de proveedor" y otros implícitamente al no estar en esta lista.

# --- Variables Globales y Contadores ---
conexion = None
cursor = None
proceso_exitoso = False
lineas_leidas_excel = 0
num_filas_conciliacion = None
conciliaciones_insertadas_bd = 0
pagos_ficticios_creados = 0
conciliaciones_omitidas_no_info = 0
conciliaciones_omitidas_diario_invalido = 0 # <-- NUEVO
conciliaciones_omitidas_no_pago_real = 0
conciliaciones_omitidas_no_factura = 0
conciliaciones_con_error_fila = 0
tabla_truncada = False

# --- Funciones Auxiliares ---
def limpiar_decimal_conc(valor):
    if pd.isna(valor): return Decimal('0.0')
    try:
        valor_str = str(valor).replace(',', '.').strip()
        if valor_str.lower() in ["<na>", "nan", "none", "", "#n/a", "false"]: return Decimal('0.0')
        dec_valor = Decimal(valor_str); return dec_valor if dec_valor.is_finite() else Decimal('0.0')
    except: return Decimal('0.0')
    
def calcular_tasa(row):
    monto_vef = row['Monto_vef']
    monto_aplicado = row['monto_aplicado']
    if monto_aplicado is not None and monto_aplicado != Decimal('0.0') and monto_vef is not None:
        try: return (monto_vef / monto_aplicado).quantize(Decimal('0.00000001'))
        except (InvalidOperation, DivisionByZero): return Decimal('0.0')
    return Decimal('0.0')

def limpiar_int_conc(valor):
    if pd.isna(valor): return None
    try: return int(float(valor))
    except: return None

def extraer_num_factura_limpio(valor_raw):
    if pd.isna(valor_raw): return None
    try:
        valor_str = str(valor_raw).strip()
        if not valor_str: return None
        valor_str=valor_str+" "; posicion_espacio = valor_str.find(' ')
        num_factura = valor_str[:posicion_espacio] if posicion_espacio != -1 else valor_str
        return num_factura if num_factura else None
    except: return None

# --- Lógica Principal ---
try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion: raise Exception("No se pudo conectar.")
    cursor = conexion.cursor(dictionary=True)
    print("[OK] Conexión establecida.")

    # --- TRUNCAR TABLA ---
    print(f"[DB] Vaciando tabla '{NOMBRE_TABLA_CONCILIADOS}'...")
    try:
        cursor.execute(f"TRUNCATE TABLE {NOMBRE_TABLA_CONCILIADOS};")
        print(f"[OK] Comando TRUNCATE ejecutado.")
        tabla_truncada = True
    except Exception as e_truncate: raise Exception(f"Fallo al truncar tabla: {e_truncate}")

    # 2. OBTENER MAPEOS
    print("[DB] Obteniendo mapeo de Pagos Reales...")
    cursor.execute(f"SELECT id, idodoo_pago FROM {NOMBRE_TABLA_PAGOS} WHERE idodoo_pago IS NOT NULL AND idodoo_pago > 0")
    pagos_reales_dict = {int(p['idodoo_pago']): p['id'] for p in cursor.fetchall() if p.get('idodoo_pago')}
    print(f"[OK] Mapeo de {len(pagos_reales_dict)} pagos reales obtenido.")
    pagos_ficticios_creados_dict = {} # Para IDs negativos creados en esta ejecución

    print("[DB] Obteniendo mapeo de Facturas...")
    cursor.execute("SELECT id, num_factura FROM facturas WHERE num_factura IS NOT NULL AND num_factura != ''")
    facturas_dict = {f['num_factura'].strip(): f['id'] for f in cursor.fetchall() if f.get('num_factura')}
    print(f"[OK] Mapeo de {len(facturas_dict)} facturas obtenido.")

    # 3. LEER EXCEL
    print(f"[INFO] Leyendo archivo Excel: {ARCHIVO_EXCEL_ASIENTOS}...")
    try:
        df_asientos = pd.read_excel(ARCHIVO_EXCEL_ASIENTOS, sheet_name=NOMBRE_HOJA_EXCEL, engine="openpyxl", dtype=str)
        lineas_leidas_excel = len(df_asientos)
    except FileNotFoundError: raise Exception(f"No se encontró el archivo Excel: {ARCHIVO_EXCEL_ASIENTOS}")
    except Exception as e: raise Exception(f"Fatal al leer Excel: {e}")

    if lineas_leidas_excel == 0:
        print("[INFO] Archivo Excel vacío.")
        proceso_exitoso = True
    else:
        print(f"[INFO] Archivo leído. {lineas_leidas_excel} líneas encontradas.")

        # 4. PREPARAR DATAFRAME
        print("[INFO] Preparando DataFrame...")
        df_asientos = df_asientos.rename(columns=COLUMN_MAPPING)
        # ... (Verificar columnas requeridas - sin cambios) ...
        columnas_requeridas = ['fecha_asiento', 'idodoo_pago', 'idodoo_conciliacion', 'diario_asiento',
                            'monto_aplicado_str', 'monto_vef_str', 'num_factura_aplicada_raw']
        columnas_presentes = df_asientos.columns.tolist()
        columnas_faltantes = [col for col in columnas_requeridas if col not in columnas_presentes]
        if columnas_faltantes: raise ValueError(f"Faltan columnas mapeadas: {', '.join(columnas_faltantes)}.")
        
        # Limpieza inicial y conversión de IDs clave
        for col in df_asientos.columns:
            if df_asientos[col].dtype == 'object':
                df_asientos[col] = df_asientos[col].str.strip().replace(['', '<NA>', 'None', 'nan', 'NaN', 'FALSE', 'False', 'false'], np.nan)
        df_asientos['idodoo_conciliacion'] = df_asientos['idodoo_conciliacion'].apply(limpiar_int_conc)
        df_asientos['idodoo_pago'] = df_asientos['idodoo_pago'].apply(limpiar_int_conc) # Limpiar antes de crear ficticios
        df_asientos['id_linea_asiento'] = df_asientos['id_linea_asiento'].apply(limpiar_int_conc)
                
        # --- Crear ID Ficticio ANTES de fill-down ---
        print("[INFO] Asignando IDs ficticios negativos a Notas de Crédito...")
        ids_ficticios_asignados = 0
        for index, row in df_asientos.iterrows():
            # Condición: Sin ID Pago, PERO con Fecha, Diario y ID Conciliación
            if pd.isna(row['idodoo_pago']) and \
                pd.notna(row['fecha_asiento']) and \
                pd.notna(row['diario_asiento']) and \
                pd.notna(row['id_linea_asiento']):
                #id_concil = row['idodoo_conciliacion']
                id_concil = row['id_linea_asiento']
                df_asientos.loc[index, 'idodoo_pago'] = -abs(id_concil) # Asignar negativo
                ids_ficticios_asignados += 1
        if ids_ficticios_asignados > 0: print(f"[OK] {ids_ficticios_asignados} IDs ficticios asignados en DataFrame.")
        
        # ---------------------------------------------

        # Aplicar Fill-Down (ahora propagará IDs reales y ficticios)
        fill_down_cols = ['diario_asiento', 'fecha_asiento', 'numero_asiento',
                        'referencia_asiento', 'id_linea_asiento', 'idodoo_pago']
        print("[INFO] Aplicando lógica 'fill-down'...")
        for col in fill_down_cols:
            if col in df_asientos.columns: df_asientos[col] = df_asientos[col].ffill()
            else: print(f"[WARN] Columna '{col}' para fill-down no encontrada.")
            
        # --- Filtrar EXCLUYENDO Diarios ---
        print(f"[INFO] Excluyendo diarios no deseados: {DIARIOS_A_EXCLUIR}...")
        # Usamos ~ para negar la condición .isin()
        df_filtrado_diario = df_asientos[~df_asientos['diario_asiento'].isin(DIARIOS_A_EXCLUIR)].copy()
        conciliaciones_omitidas_diario_invalido = len(df_asientos) - len(df_filtrado_diario)
        if conciliaciones_omitidas_diario_invalido > 0: print(f"[INFO] {conciliaciones_omitidas_diario_invalido} líneas ignoradas por pertenecer a diarios excluidos.")
        # ---------------------------------
        
        # Filtrar filas que representan una conciliación válida
        print("[INFO] Filtrando filas con ID de conciliación...")
        df_conciliaciones = df_filtrado_diario.dropna(subset=['idodoo_conciliacion']).copy()
        num_filas_conciliacion = len(df_conciliaciones)
        # Ajustar omitidas_no_info (total leído - omitidas diario - filas con conciliación)
        conciliaciones_omitidas_no_info = lineas_leidas_excel - conciliaciones_omitidas_diario_invalido - num_filas_conciliacion
        print(f"[OK] {num_filas_conciliacion} filas de conciliación válidas encontradas para procesar.")
        if conciliaciones_omitidas_no_info > 0: print(f"[INFO] {conciliaciones_omitidas_no_info} líneas adicionales ignoradas (sin ID conciliación después de otros filtros).")
        
        if num_filas_conciliacion > 0:
            
            # Limpiar y convertir tipos restantes
            print("[INFO] Limpiando tipos de datos restantes...")
            # Asegurarse que los IDs sean enteros después de ffill y filtros
            df_conciliaciones['idodoo_conciliacion'] = df_conciliaciones['idodoo_conciliacion'].apply(limpiar_int_conc)
            df_conciliaciones['idodoo_pago'] = df_conciliaciones['idodoo_pago'].apply(limpiar_int_conc) # Puede ser negativo
            df_conciliaciones['monto_aplicado'] = df_conciliaciones['monto_aplicado_str'].apply(limpiar_decimal_conc)
            df_conciliaciones['Monto_vef'] = df_conciliaciones['monto_vef_str'].apply(limpiar_decimal_conc)
            df_conciliaciones['fecha_aplicacion'] = pd.to_datetime(df_conciliaciones['fecha_asiento'], errors='coerce').dt.date

            print("[INFO] Extrayendo número de factura aplicado...")
            df_conciliaciones['num_factura_aplicada'] = df_conciliaciones['num_factura_aplicada_raw'].apply(extraer_num_factura_limpio)
            
            print("[INFO] Calculando tasa de cambio...")

            df_conciliaciones['tasa'] = df_conciliaciones.apply(calcular_tasa, axis=1)
            
                        
            print("[INFO] Mapeando IDs internos y creando pagos ficticios en BD...")
            df_conciliaciones['id_pago'] = pd.NA # Resetear columna para IDs internos
            df_conciliaciones['id_factura'] = df_conciliaciones['num_factura_aplicada'].map(facturas_dict)
            conciliaciones_omitidas_no_factura = df_conciliaciones['id_factura'].isna().sum()
            
            # Iterar para buscar/crear pagos internos
            for index, row in df_conciliaciones.iterrows():
                idodoo_pago_actual = row.get('idodoo_pago') # Ya es int (positivo o negativo) o None
                id_pago_interno = None

                if idodoo_pago_actual is not None:
                    if idodoo_pago_actual > 0: # Pago Real
                        id_pago_interno = pagos_reales_dict.get(idodoo_pago_actual)
                        if id_pago_interno is None:
                            conciliaciones_omitidas_no_pago_real += 1
                            # print(f"[DEBUG] Pago real {idodoo_pago_actual} no encontrado en pagos_reales_dict.") # Debug
                    else: # Pago Ficticio (idodoo_pago_actual es negativo)
                        id_pago_ficticio_a_usar = None
                        # --- CAMBIO: Buscar primero en BD ---
                        try:
                            cursor.execute(f"SELECT id FROM {NOMBRE_TABLA_PAGOS} WHERE idodoo_pago = %s", (idodoo_pago_actual,))
                            pago_ficticio_existente = cursor.fetchone()

                            if pago_ficticio_existente:
                                # Ya existe en la BD, usar su ID interno
                                id_pago_ficticio_a_usar = pago_ficticio_existente['id']
                                # print(f"[DEBUG] Pago ficticio {idodoo_pago_actual} encontrado en BD con ID: {id_pago_ficticio_a_usar}") # Debug
                            else:
                                # No existe, hay que crearlo
                                # print(f"[DEBUG] Pago ficticio {idodoo_pago_actual} NO encontrado en BD. Intentando INSERT...") # Debug
                                cursor.execute(
                                    f"""INSERT INTO {NOMBRE_TABLA_PAGOS}
                                        (idodoo_pago, fecha_pago, monto, diario, referencia, id_cliente)
                                        VALUES (%s, %s, %s, %s, %s, %s)""",
                                    (idodoo_pago_actual, row.get('fecha_aplicacion'), Decimal('0.0'),
                                    'Nota de Crédito', f'NC Aplicada Línea: {row.get("id_linea_asiento")}', None) # Usar id_linea_asiento en referencia
                                )
                                id_pago_ficticio_a_usar = cursor.lastrowid # O usar SELECT id... si lastrowid falla
                                if id_pago_ficticio_a_usar:
                                    pagos_ficticios_creados += 1
                                    # No necesitamos el diccionario pagos_ficticios_creados_dict ahora
                                    # print(f"[DEBUG] Pago ficticio CREADO. Nuevo id_interno: {id_pago_ficticio_a_usar}") # Debug
                                else:
                                    print(f"[ERROR] ¡No se pudo obtener el ID del pago ficticio recién insertado para idodoo_pago {idodoo_pago_actual}!")

                        except Exception as e_db_ficticio:
                            print(f"\n[ERROR] Error al buscar/crear pago ficticio para idodoo_pago {idodoo_pago_actual}: {e_db_ficticio}")
                        # --- FIN CAMBIO ---
                        id_pago_interno = id_pago_ficticio_a_usar

                # Asignar ID interno (real o ficticio) o mantener NA si falló/omitido
                df_conciliaciones.loc[index, 'id_pago'] = id_pago_interno if id_pago_interno is not None else pd.NA

            df_conciliaciones['id_pago'] = df_conciliaciones['id_pago'].astype('Int64')
            df_conciliaciones['id_factura'] = df_conciliaciones['id_factura'].astype('Int64')
            df_conciliaciones = df_conciliaciones.replace({np.nan: None, pd.NaT: None, pd.NA: None})
            print("[OK] Datos de conciliaciones preparados.")
            # ... (Imprimir warnings de omisiones - sin cambios) ...
            if pagos_ficticios_creados > 0: print(f"[INFO] Se crearon {pagos_ficticios_creados} registros de pago ficticios (NC).")
            if conciliaciones_omitidas_no_pago_real > 0: print(f"[WARN] {conciliaciones_omitidas_no_pago_real} omitidas (Pago real no encontrado).")
            if conciliaciones_omitidas_no_factura > 0: print(f"[WARN] {conciliaciones_omitidas_no_factura} omitidas (Factura no encontrada).")

            # 5. PROCESAR FILAS (SOLO INSERT)
            print(f"[INFO] Procesando {num_filas_conciliacion} conciliaciones para INSERT en '{NOMBRE_TABLA_CONCILIADOS}'...")
            columnas_db = ['id_pago', 'id_factura', 'idodoo_conciliacion',
                        'monto_aplicado', 'Monto_vef', 'tasa', 'fecha_aplicacion']
            placeholders = ', '.join(['%s'] * len(columnas_db))
            sql_insert = f"INSERT INTO {NOMBRE_TABLA_CONCILIADOS} ({', '.join(columnas_db)}) VALUES ({placeholders})"

            print("[informa] exportando dataframe a excel")
            df_conciliaciones.to_excel("reporte_validacion data frame.xlsx", index=False)
            
            for index, row in df_conciliaciones.iterrows():
                print(f"\rProcesando línea Excel {index + 1}/{lineas_leidas_excel}...")
                if row['id_pago'] is None or row['id_factura'] is None or row['idodoo_conciliacion'] is None:
                    print (row['id_pago'],row['id_factura'],row['idodoo_conciliacion'])
                    print ("****** VAMOS por aquí")
                    continue
                try:
                    valores_tupla = (
                        row.get('id_pago'), row.get('id_factura'), row.get('idodoo_conciliacion'),
                        str(row.get('monto_aplicado', Decimal('0.0'))), str(row.get('Monto_vef', Decimal('0.0'))),
                        str(row.get('tasa', Decimal('0.0'))), row.get('fecha_aplicacion')
                    )
                    cursor.execute(sql_insert, valores_tupla)
                    conciliaciones_insertadas_bd += 1
                except Exception as e:
                    print(f"\n[ERROR] en fila Excel {index + 2} (Concil Odoo: {row.get('idodoo_conciliacion', 'N/A')}): {e}")
                    conciliaciones_con_error_fila += 1

            print(f"\n[INFO] Procesamiento de {num_filas_conciliacion} conciliaciones completado.")

            # 6. COMMIT o ROLLBACK FINAL
            if conciliaciones_con_error_fila == 0:
                print("\n[DB] Realizando COMMIT final...")
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
             proceso_exitoso = True

# --- Bloques except y finally ---
except Exception as e_general:
    print(f"\n[ERROR] ERROR GENERAL INESPERADO: {e_general}")
    proceso_exitoso = False
    if conexion:
        try: print("[DB] Intentando ROLLBACK..."); conexion.rollback(); print("(-) Rollback realizado.")
        except Exception as rb_err: print(f"[WARN] Error en rollback: {rb_err}")
finally:
    # 7. MOSTRAR RESUMEN
    def safe_print(var_name, value):
        display_value = value if value is not None else '--'
        print(f"{var_name:<45}: {display_value}") # Ajustado ancho

    print("\n--- Resumen Importación Conciliaciones ---")
    safe_print("Total líneas leídas del Excel", locals().get('lineas_leidas_excel'))
    if locals().get('tabla_truncada'): print(f"{'Tabla Truncada?':<45}: Sí")
    safe_print("Líneas Ignoradas (Diario no válido)", locals().get('conciliaciones_omitidas_diario_invalido'))
    safe_print("Líneas con datos de conciliación (post-filtro)", locals().get('num_filas_conciliacion'))
    safe_print("Pagos Ficticios Creados (NC)", locals().get('pagos_ficticios_creados'))
    safe_print("Conciliaciones Insertadas en BD", locals().get('conciliaciones_insertadas_bd'))
    print("---------------------------------------------")
    safe_print("Líneas Ignoradas (Sin ID Conciliación)", locals().get('conciliaciones_omitidas_no_info'))
    safe_print("Conciliaciones Omitidas (Pago Real no encontrado)", locals().get('conciliaciones_omitidas_no_pago_real'))
    safe_print("Conciliaciones Omitidas (Factura no encontrada)", locals().get('conciliaciones_omitidas_no_factura'))
    safe_print("Conciliaciones con Error de Inserción", locals().get('conciliaciones_con_error_fila'))
    print("=============================================")

    # 8. CERRAR RECURSOS
    if cursor: cursor.close(); print("[DB] Cursor cerrado.")
    if conexion and conexion.is_connected(): conexion.close(); print("[DB] Conexión cerrada.")

# 9. SALIDA FINAL DEL SCRIPT
if 'proceso_exitoso' in locals() and proceso_exitoso:
    print("\n[OK] Script finalizado correctamente.")
    sys.exit(0)
else:
    print("\n[ERROR] Script finalizado con errores.")
    sys.exit(1)