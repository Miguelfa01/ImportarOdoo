# -*- coding: utf-8 -*-
# Guardar como: importar_cliente.py

import pandas as pd
from conexion_mysql import conectar
import sys
import numpy as np # Para reemplazar infinitos/NaN

print("\n--- Script: importar_cliente.py ---")

# --- Configuración ---
ARCHIVO_EXCEL_CLIENTES = "C:/mysql_import/Contacto (res.partner).xlsx" # <-- CONFIRMA RUTA
NOMBRE_HOJA_EXCEL = "Sheet1" # <-- CONFIRMA NOMBRE HOJA

# Mapeo de columnas: Clave = Nombre EXACTO en Excel, Valor = Nombre interno DataFrame
# CORREGIDO: Usar 'idodoo' para coincidir con la tabla
COLUMN_MAPPING = {
    "ID": "idodoo", # <-- CORREGIDO
    "Vendedores": "vendedor_nombre", # Usar nombre distinto para evitar confusión
    "Nombre": "nombre",
    "City": "ciudad",
    "Teléfono": "telefono",
    "Correo electrónico": "correo_electronico",
    # "Comercial": "comercial", # No parece estar en la tabla clientes
    "Dirección completa": "direccion",
    "Estado": "estado",
    "Identificación fiscal": "identificacion_fiscal",
    "Tipo de documento": "tipo_documento",
    "Etiquetas": "etiqueta",
    "Plazo de pago de cliente": "plazos_pago",
    "Creado en": "fecha_creacion",
    "Vendedores/ID":"idodoo_vendedor",
    "Plazo de pago de cliente/ID": "idodoo_plazospago"
}

NOMBRE_TABLA_CLIENTES = "clientes"

# --- Variables Globales y Contadores ---

clientes_leidos_excel = 0
clientes_insertados = 0
clientes_actualizados = 0
clientes_omitidos_sin_nombre = 0
clientes_omitidos_sin_idodoo = 0
clientes_con_error_fila = 0

# --- Funciones Auxiliares ---
def limpiar_int_clientes(valor):
    if pd.isna(valor): return None
    try: return int(float(valor))
    except (ValueError, TypeError): return None

# --- Lógica Principal ---
conexion = None
cursor = None
proceso_exitoso = False

try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion: raise Exception("No se pudo conectar a la base de datos.")
    cursor = conexion.cursor(dictionary=True) # Usar dictionary=True puede ser útil
    print("[OK] Conexión establecida.")

    # 2. OBTENER MAPEO DE VENDEDORES (nombre -> id_vendedor)
    print("[DB] Obteniendo mapeo de Vendedores desde la BD...")
    cursor.execute("SELECT idVendedores, nombre FROM vendedores WHERE nombre IS NOT NULL")
    vendedores_db = cursor.fetchall()
    # Mapeo: nombre en minúsculas -> idVendedores
    vendedores_dict = {str(v['nombre']).lower(): v['idVendedores'] for v in vendedores_db if v.get('nombre')}
    print(f"[OK] Mapeo de {len(vendedores_dict)} vendedores obtenido.")

    # 3. LEER EXCEL DE CLIENTES
    print(f"[INFO] Leyendo archivo Excel de Clientes: {ARCHIVO_EXCEL_CLIENTES} (Hoja: {NOMBRE_HOJA_EXCEL})")
    try:
        # Leer como string inicialmente para controlar mejor la limpieza
        df = pd.read_excel(ARCHIVO_EXCEL_CLIENTES, sheet_name=NOMBRE_HOJA_EXCEL, engine="openpyxl", dtype=str)
        clientes_leidos_excel = len(df)
    except FileNotFoundError:
        print(f"[ERROR] Fatal: No se encontró el archivo Excel: {ARCHIVO_EXCEL_CLIENTES}")
        raise
    except Exception as e:
        print(f"[ERROR] Fatal al leer el archivo Excel de clientes: {e}")
        raise

    if clientes_leidos_excel == 0:
         print("[INFO] El archivo Excel de clientes está vacío. Proceso completado.")
         proceso_exitoso = True
         # Salir limpiamente
    else:
        print(f"[INFO] Archivo leído. {clientes_leidos_excel} clientes encontrados.")

        # 4. PREPARAR DATAFRAME
        print("[INFO] Preparando datos del DataFrame de clientes...")
        df = df.rename(columns=COLUMN_MAPPING)

        # Verificar columnas mapeadas requeridas
        columnas_requeridas = ['idodoo', 'nombre'] # Mínimo necesario
        columnas_presentes = df.columns.tolist()
        columnas_faltantes = [col for col in columnas_requeridas if col not in columnas_presentes]
        if columnas_faltantes:
            msg = f"Faltan columnas mapeadas esenciales: {', '.join(columnas_faltantes)}. Verifica COLUMN_MAPPING."
            print(f"[ERROR] Fatal: {msg}")
            raise ValueError(msg)

        # Limpieza inicial: quitar espacios y reemplazar placeholders comunes con NaN
        for col in df.columns:
             if df[col].dtype == 'object': # Solo para columnas de texto/objeto
                  df[col] = df[col].str.strip().replace(['', '<NA>', 'None', 'nan', 'NaN', 'FALSE', 'False', 'false'], np.nan)

        # Filtrar registros sin nombre o sin idodoo (después de limpiar)
        original_count = len(df)
        df.dropna(subset=['nombre'], inplace=True)
        clientes_omitidos_sin_nombre = original_count - len(df)
        original_count = len(df)
        df.dropna(subset=['idodoo'], inplace=True)
        clientes_omitidos_sin_idodoo = original_count - len(df)

        if clientes_omitidos_sin_nombre > 0: print(f"[INFO] {clientes_omitidos_sin_nombre} filas omitidas por nombre vacío.")
        if clientes_omitidos_sin_idodoo > 0: print(f"[INFO] {clientes_omitidos_sin_idodoo} filas omitidas por idodoo vacío.")

        # Convertir tipos y limpiar datos específicos
        print("[INFO] Limpiando y convirtiendo tipos de datos...")
        df['idodoo'] = df['idodoo'].apply(limpiar_int_clientes)
        df['idodoo_vendedor'] = df['idodoo_vendedor'].apply(limpiar_int_clientes)
        df['idodoo_plazospago'] = df['idodoo_plazospago'].apply(limpiar_int_clientes)

        # Mapear id_vendedor (nombre tabla DB) usando el diccionario
        def buscar_id_vendedor(nombre_vendedor):
            if pd.isna(nombre_vendedor): return None
            return vendedores_dict.get(str(nombre_vendedor).lower()) # Busca nombre en minúsculas
        # CORREGIDO: Crear columna 'id_vendedor' que coincide con la tabla
        df['id_vendedor'] = df['vendedor_nombre'].apply(buscar_id_vendedor)

        # Limpiar otros campos
        df['telefono'] = df['telefono'].astype(str).str.slice(0, 20) # Truncar a 20
        df['fecha_creacion'] = pd.to_datetime(df['fecha_creacion'], errors='coerce').dt.date

        # Convertir todo lo que queda como NaN/NaT a None para SQL
        df = df.replace({np.nan: None, pd.NaT: None})
        print("[OK] Datos de clientes preparados.")

        # 5. OBTENER IDs EXISTENTES EN DB
        print("[DB] Verificando clientes existentes en la BD...")
        cursor.execute(f"SELECT idodoo FROM {NOMBRE_TABLA_CLIENTES} WHERE idodoo IS NOT NULL")
        # Asegurarse de convertir a int al crear el set
        ids_existentes = {int(row['idodoo']) for row in cursor.fetchall() if row.get('idodoo') is not None}
        print(f"[OK] {len(ids_existentes)} IDs existentes encontrados.")

        # 6. PROCESAR FILAS (INSERT / UPDATE)
        print(f"[INFO] Procesando {len(df)} clientes para INSERT/UPDATE en '{NOMBRE_TABLA_CLIENTES}'...")

        # Nombres de columnas en la tabla 'clientes' (¡VERIFICAR CON TU TABLA EXACTA!)
        # CORREGIDO: Usar nombres de columna de la BD
        columnas_db_insert = [
            'idodoo', 'id_vendedor', 'vendedor', 'nombre', 'ciudad', 'telefono',
            'correo_electronico', 'direccion', 'estado', 'identificacion_fiscal',
            'tipo_documento', 'etiqueta', 'plazos_pago', 'fecha_creacion',
            'idodoo_vendedor', 'idodoo_plazospago'
        ]
        columnas_db_update = [ # Excluir idodoo de la actualización
            'id_vendedor', 'vendedor', 'nombre', 'ciudad', 'telefono',
            'correo_electronico', 'direccion', 'estado', 'identificacion_fiscal',
            'tipo_documento', 'etiqueta', 'plazos_pago', 'fecha_creacion',
            'idodoo_vendedor', 'idodoo_plazospago'
        ]

        placeholders_insert = ', '.join(['%s'] * len(columnas_db_insert))
        update_set_parts = [f"{col} = %s" for col in columnas_db_update]
        sql_insert = f"INSERT INTO {NOMBRE_TABLA_CLIENTES} ({', '.join(columnas_db_insert)}) VALUES ({placeholders_insert})"
        sql_update = f"UPDATE {NOMBRE_TABLA_CLIENTES} SET {', '.join(update_set_parts)} WHERE idodoo = %s"

        for index, row in df.iterrows():
            # Asegurarse que idodoo sea int para la comparación
            idodoo_actual = row.get('idodoo')
            if idodoo_actual is None: # Ya deberian estar filtrados, pero por si acaso
                 continue

            print(f"\rProcesando cliente Excel {index + 1}/{clientes_leidos_excel} (ID Odoo: {idodoo_actual})...", end="")

            # Preparar tupla de valores en el orden de columnas_db_insert/update
            # CORREGIDO: Usar nombres de columna correctos y simplificar manejo de None
            valores = {
                'idodoo': row.get('idodoo'),
                'id_vendedor': row.get('id_vendedor'), # Ya es None o int
                'vendedor': row.get('vendedor_nombre'), # El nombre original del vendedor
                'nombre': row.get('nombre'),
                'ciudad': row.get('ciudad'),
                'telefono': row.get('telefono'),
                'correo_electronico': row.get('correo_electronico'),
                'direccion': row.get('direccion'),
                'estado': row.get('estado'),
                'identificacion_fiscal': row.get('identificacion_fiscal'),
                'tipo_documento': row.get('tipo_documento'),
                'etiqueta': row.get('etiqueta'),
                'plazos_pago': row.get('plazos_pago'),
                'fecha_creacion': row.get('fecha_creacion'),
                'idodoo_vendedor': row.get('idodoo_vendedor'),
                'idodoo_plazospago': row.get('idodoo_plazospago')
            }

            try:
                if idodoo_actual in ids_existentes:
                    # UPDATE
                    valores_update = [valores[col] for col in columnas_db_update]
                    valores_update.append(idodoo_actual) # Añadir idodoo para el WHERE
                    cursor.execute(sql_update, tuple(valores_update))
                    clientes_actualizados += 1
                else:
                    # INSERT
                    valores_insert = [valores[col] for col in columnas_db_insert]
                    cursor.execute(sql_insert, tuple(valores_insert))
                    clientes_insertados += 1
            except Exception as e:
                print(f"\n[ERROR] en fila Excel {index + 2} (ID Odoo: {idodoo_actual}): {e}")
                # print("      Datos:", valores) # Descomentar para depurar
                clientes_con_error_fila += 1
                # Continuar con la siguiente fila

        print(f"\n[INFO] Procesamiento de {len(df)} clientes de Excel completado.")

        # 7. COMMIT o ROLLBACK
        if clientes_con_error_fila == 0:
            print("\n[DB] Realizando COMMIT de los cambios en clientes...")
            conexion.commit()
            proceso_exitoso = True
            print("(+) Commit realizado.")
        else:
            print(f"\n[WARN] Hubo {clientes_con_error_fila} errores durante el procesamiento.")
            print("[DB] Realizando ROLLBACK...")
            conexion.rollback()
            proceso_exitoso = False
            print("(-) Rollback realizado.")

# --- Bloques except y finally ---
except Exception as e_general:
    print(f"\n[ERROR] ERROR GENERAL INESPERADO (Importación Clientes): {e_general}")
    proceso_exitoso = False
    if conexion:
        try:
            print("[DB] Intentando realizar ROLLBACK...")
            conexion.rollback()
            print("(-) Rollback realizado.")
        except Exception as rb_err:
            print(f"[WARN] Error durante el rollback: {rb_err}")
finally:
    # 8. MOSTRAR RESUMEN FINAL
    def safe_print(var_name, value):
        display_value = value if value is not None else '--'
        print(f"{var_name:<40}: {display_value}")

    print("\n--- Resumen Importación Clientes ---")
    safe_print("Total clientes leídos del Excel", locals().get('clientes_leidos_excel'))
    safe_print("Clientes Insertados", locals().get('clientes_insertados'))
    safe_print("Clientes Actualizados", locals().get('clientes_actualizados'))
    print("-------------------------------------------")
    safe_print("Clientes Omitidos (Sin Nombre)", locals().get('clientes_omitidos_sin_nombre'))
    safe_print("Clientes Omitidos (Sin ID Odoo)", locals().get('clientes_omitidos_sin_idodoo'))
    safe_print("Clientes con Error de Procesamiento", locals().get('clientes_con_error_fila'))
    print("===========================================")

    # 9. CERRAR RECURSOS
    if cursor: cursor.close(); print("[DB] Cursor de clientes cerrado.")
    if conexion and conexion.is_connected(): conexion.close(); print("[DB] Conexión a MySQL cerrada.")

# 10. SALIDA FINAL DEL SCRIPT
if 'proceso_exitoso' in locals() and proceso_exitoso:
    print("\n[OK] Script de importación de clientes finalizado correctamente.")
    sys.exit(0)
else:
    print("\n[ERROR] Script de importación de clientes finalizado con errores.")
    sys.exit(1)