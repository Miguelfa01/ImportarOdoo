# -*- coding: utf-8 -*-
# Guardar como: importar_facturas.py

import pandas as pd
from datetime import date
from conexion_mysql import conectar
import subprocess
import sys
import os
import numpy as np # Para manejar NaN

print("--- Script: importar_facturas.py ---")

# --- Funciones Auxiliares ---
def limpiar_float(valor):
    # ... (sin cambios) ...
    try:
        if pd.isna(valor) or str(valor).strip().lower() in ["<na>", "nan", "none", "", "#n/a"]: return 0.0
        # Intentar convertir directamente, manejando comas
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError): return 0.0

def limpiar_int_facturas(valor):
    # Nueva función auxiliar para limpiar IDs
    if pd.isna(valor): return None
    try: return int(float(str(valor).replace(',', '.'))) # Manejar comas y .0
    except (ValueError, TypeError): return None

# --- Variables ---
archivo_excel = "C:/mysql_import/Asiento contable (account.move).xlsx" # <- CONFIRMA RUTA
script_cuotas = "generar_cuotas.py"
conexion = None
cursor = None
importacion_exitosa = False
commit_realizado = False

# --- Contadores ---
registros_insertados = 0
registros_actualizados = 0
registros_eliminados = 0 # <-- NUEVO CONTADOR
registros_omitidos_sin_idodoo = 0
registros_omitidos_otro_estado = 0 # <-- NUEVO CONTADOR
registros_con_error_fila = 0
total_filas_excel = 0

try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion: raise Exception("No se pudo conectar a la base de datos.")
    cursor = conexion.cursor(dictionary=True)
    print("[OK] Conexión establecida.")

    # 2. LEER EXCEL
    print(f"[INFO] Leyendo archivo Excel: {archivo_excel}")
    try:
        # Leer como string inicialmente
        df = pd.read_excel(archivo_excel, sheet_name="Sheet1", engine="openpyxl", dtype=str)
        total_filas_excel = len(df)
    except FileNotFoundError:
        print(f"[ERROR] Fatal: No se encontró el archivo Excel: {archivo_excel}")
        raise
    except Exception as e:
        print(f"[ERROR] Fatal al leer el archivo Excel: {e}")
        raise

    if total_filas_excel == 0:
        print("[INFO] Archivo Excel vacío.")
        proceso_exitoso = True
        # Salir limpiamente
    else:
        print(f"[INFO] Archivo leído. {total_filas_excel} filas encontradas.")

        # 3. RENOMBRAR Y PREPARAR DATAFRAME INICIAL
        print("[INFO] Preparando datos del DataFrame (Paso 1: Renombrar y Limpiar Estado/ID)...")
        column_mapping = {
            "Identificación": "rif",
            "Nombre de la empresa a mostrar en la factura": "cliente",
            "Dirección de entrega": "direccion",
            "Número": "num_factura",
            "Diario": "tipo_documento",
            "Fecha de Factura/Recibo": "fecha_factura",
            "Fecha de Recepción": "fecha_entrega",
            "Fecha de vencimiento": "fecha_vencimiento",
            "Total con signo": "total_factura",
            "Plazos de pago": "plazos_pago",
            "Estado de pago": "estado_pago", # Estado del PAGO (puede ser diferente al estado de la factura)
            "Estado": "estado_odoo", # <-- AÑADIDO: Estado de la FACTURA ('Publicado', 'Borrador', 'Cancelada')
            "Vendedor": "vendedor",
            "Vendedor/ID": "idodoo_vendedor",
            "ID": "idodoo", # ID de la Factura Odoo
            "Empresa/ID": "idodoo_clientes",
            "Plazos de pago/ID": "idodoo_plazospago",
            "Importe adeudado con signo": "pendiente_cobrar"
        }
        df = df.rename(columns=column_mapping)

        # Verificar columnas esenciales (incluyendo estado_odoo y idodoo)
        columnas_esenciales = ['idodoo', 'estado_odoo', 'idodoo_clientes', 'idodoo_vendedor', 'total_factura', 'pendiente_cobrar', 'fecha_factura', 'fecha_vencimiento']
        columnas_faltantes = [col for col in columnas_esenciales if col not in df.columns]
        if columnas_faltantes:
            msg = f"Faltan columnas esenciales en Excel: {', '.join(columnas_faltantes)}. Verifica nombres y mapeo."
            print(f"[ERROR] Fatal: {msg}")
            raise ValueError(msg)

        # Limpieza inicial de Estado e ID (necesarios para borrar/filtrar)
        df['estado_odoo'] = df['estado_odoo'].fillna('').astype(str).str.strip().str.lower()
        df['idodoo'] = df['idodoo'].apply(limpiar_int_facturas) # Limpiar ID Odoo

        # Filtrar filas sin ID Odoo antes de continuar
        original_count = len(df)
        df.dropna(subset=['idodoo'], inplace=True)
        registros_omitidos_sin_idodoo = original_count - len(df)
        if registros_omitidos_sin_idodoo > 0: print(f"[INFO] {registros_omitidos_sin_idodoo} filas ignoradas por idodoo vacío.")

        # 4. IDENTIFICAR Y EJECUTAR BORRADOS
        estados_a_borrar = ['borrador', 'cancelada']
        ids_a_borrar = df.loc[df['estado_odoo'].isin(estados_a_borrar), 'idodoo'].tolist()

        if ids_a_borrar:
            # Asegurarse de que sean enteros válidos
            ids_a_borrar = [int(id_val) for id_val in ids_a_borrar if id_val is not None]
            if ids_a_borrar: # Proceder solo si hay IDs válidos para borrar
                print(f"[DB] Intentando eliminar {len(ids_a_borrar)} facturas con estado 'Borrador' o 'Cancelada'...")
                placeholders = ', '.join(['%s'] * len(ids_a_borrar))
                sql_delete = f"DELETE FROM facturas WHERE idodoo IN ({placeholders})"
                try:
                    cursor.execute(sql_delete, tuple(ids_a_borrar))
                    registros_eliminados = cursor.rowcount
                    print(f"[OK] {registros_eliminados} facturas eliminadas de la BD.")
                    # Hacer commit de los borrados inmediatamente podría ser una opción,
                    # o esperar al commit final si todo va bien. Esperemos por ahora.
                except Exception as e_del:
                    print(f"[ERROR] Error durante la eliminación de facturas: {e_del}")
                    # Decidir si continuar o detener todo. Por ahora, lanzamos error para rollback.
                    raise Exception(f"Error al eliminar facturas: {e_del}")
            else:
                 print("[INFO] No se encontraron IDs válidos para eliminar con estado 'Borrador' o 'Cancelada'.")
        else:
            print("[INFO] No se encontraron facturas para eliminar (estado 'Borrador' o 'Cancelada') en el Excel.")

        # 5. FILTRAR DATAFRAME PARA PROCESAR (SOLO PUBLICADO)
        print("[INFO] Filtrando facturas con estado 'publicado' para procesar...")
        df_procesar = df[df['estado_odoo'] == 'publicado'].copy()
        total_filas_a_procesar = len(df_procesar)
        registros_omitidos_otro_estado = len(df) - len(ids_a_borrar) - total_filas_a_procesar
        print(f"[OK] {total_filas_a_procesar} facturas 'publicadas' encontradas para insertar/actualizar.")
        if registros_omitidos_otro_estado > 0: print(f"[INFO] {registros_omitidos_otro_estado} filas ignoradas por tener otros estados o ser inválidas.")

        if total_filas_a_procesar == 0:
            print("[INFO] No hay facturas 'publicadas' para procesar.")
            # Si no hubo errores de borrado, podemos marcar como éxito
            # (asumiendo que no hubo excepciones antes)
            proceso_exitoso = True
            # Hacer commit si hubo borrados exitosos
            if registros_eliminados > 0:
                 print("[DB] Realizando COMMIT de las eliminaciones...")
                 conexion.commit()
                 commit_realizado = True
        else:
            # 6. PREPARAR DATAFRAME FINAL (Clientes, Vendedores, Fechas, etc.)
            print("[INFO] Preparando datos restantes del DataFrame filtrado...")
            # Agregar campos faltantes (si no existen ya en df_procesar)
            if "almacen" not in df_procesar.columns: df_procesar["almacen"] = "Principal"
            if "dias_credito" not in df_procesar.columns: df_procesar["dias_credito"] = None
            if "dias_cuotas" not in df_procesar.columns: df_procesar["dias_cuotas"] = None
            if "cant_cuotas" not in df_procesar.columns: df_procesar["cant_cuotas"] = None

            # Obtener mapeos de Clientes y Vendedores (solo para las facturas a procesar)
            cursor.execute("SELECT id, idodoo FROM clientes WHERE idodoo IS NOT NULL")
            clientes_dict = {str(row["idodoo"]): row["id"] for row in cursor.fetchall() if row["idodoo"] is not None}
            def map_cliente(idodoo_cliente):
                if pd.isna(idodoo_cliente): return None
                try: return clientes_dict.get(str(int(float(idodoo_cliente))))
                except (ValueError, TypeError): return None
            df_procesar["id_cliente"] = df_procesar["idodoo_clientes"].apply(map_cliente)

            cursor.execute("SELECT idVendedores, nombre FROM vendedores WHERE nombre IS NOT NULL")
            vendedores_dict = {str(row["nombre"]).lower(): row["idVendedores"] for row in cursor.fetchall() if row["nombre"] is not None}
            def map_vendedor(nombre_vendedor):
                if pd.isna(nombre_vendedor): return None
                try: return vendedores_dict.get(str(nombre_vendedor).lower())
                except (ValueError, TypeError): return None
            df_procesar["id_vendedor"] = df_procesar["vendedor"].apply(map_vendedor)

            # Limpiar Fechas y convertir NaN a None
            df_procesar["fecha_factura"] = pd.to_datetime(df_procesar["fecha_factura"], errors="coerce").dt.date
            df_procesar["fecha_entrega"] = pd.to_datetime(df_procesar["fecha_entrega"], errors="coerce").dt.date
            df_procesar["fecha_vencimiento"] = pd.to_datetime(df_procesar["fecha_vencimiento"], errors="coerce").dt.date
            # Convertir NaN/NaT a None al final de la preparación
            df_procesar = df_procesar.replace({np.nan: None, pd.NaT: None})
            print("[OK] Datos preparados para insertar/actualizar.")

            # 7. PROCESAR FILAS (INSERT/UPDATE - SOLO PUBLICADAS)
            print(f"[INFO] Procesando {total_filas_a_procesar} filas de facturas para INSERT/UPDATE...")
            # Obtener IDs existentes para la lógica INSERT/UPDATE
            cursor.execute("SELECT idodoo FROM facturas WHERE idodoo IS NOT NULL")
            ids_existentes = {int(row['idodoo']) for row in cursor.fetchall() if row.get('idodoo') is not None}

            for index, row in df_procesar.iterrows():
                idodoo_actual = row.get('idodoo') # Ya debería ser un int limpio o None
                if idodoo_actual is None: continue # Doble check

                print(f"\rProcesando fila {index + 1}/{total_filas_excel} (ID Odoo: {idodoo_actual})...", end="")
                try:
                    # Obtener datos de plazos de pago (lógica sin cambios)
                    cant_cuotas, dias_cuota, dias_credito = None, 0, 0
                    id_plazospago_seguro = limpiar_int_facturas(row.get("idodoo_plazospago"))
                    if id_plazospago_seguro:
                        cursor.execute("SELECT dias_credito, cant_cuotas, dias_cuota FROM plazos_pago WHERE idodoo = %s", (id_plazospago_seguro,))
                        resultado_plazo = cursor.fetchone()
                        if resultado_plazo:
                            dias_credito = resultado_plazo.get('dias_credito', 0) if resultado_plazo.get('dias_credito') is not None else 0
                            cant_cuotas = resultado_plazo.get('cant_cuotas')
                            dias_cuota = resultado_plazo.get('dias_cuota', 0) if resultado_plazo.get('dias_cuota') is not None else 0
                    else:
                        cant_cuotas = 1
                        f_factura = row.get("fecha_factura")
                        f_vencimiento = row.get("fecha_vencimiento")
                        if f_factura and f_vencimiento and isinstance(f_factura, date) and isinstance(f_vencimiento, date):
                            try: dias_credito = (f_vencimiento - f_factura).days
                            except TypeError: dias_credito = 0
                        else: dias_credito = 0
                        dias_cuota = dias_credito

                    # Limpiar otros IDs y valores numéricos
                    id_cliente_seguro = row.get("id_cliente") # Ya debería ser int o None
                    id_vendedor_seguro = row.get("id_vendedor") # Ya debería ser int o None
                    idodoo_vendedor_externo = limpiar_int_facturas(row.get("idodoo_vendedor"))
                    idodoo_clientes_externo = limpiar_int_facturas(row.get("idodoo_clientes")) # Aunque ya usamos id_cliente, mantenemos por si acaso
                    plazos_pago_seguro = row.get("plazos_pago") # Ya debería ser string o None

                    # Limpiar montos (usando la función auxiliar mejorada)
                    total_factura = limpiar_float(row.get("total_factura"))
                    pendiente_cobrar_excel = limpiar_float(row.get("pendiente_cobrar")) # Pendiente según Excel
                    # IMPORTANTE: El total_cobrado y pendiente_cobrar REAL se calculará DESPUÉS con el script de actualización
                    # Aquí solo guardamos los valores iniciales o una estimación simple
                    total_cobrado_inicial = round(total_factura - pendiente_cobrar_excel, 2)
                    total_factura_db = total_factura if pd.notna(row.get("total_factura")) else None # Guardar como float o None
                    pendiente_cobrar_db = total_factura_db # Inicialmente, el pendiente es el total
                    total_cobrado_db = 0.0 # Inicialmente, el cobrado es cero

                    # Preparar datos comunes para SQL
                    datos_factura_dict = {
                        'rif': row.get("rif"), 'id_cliente': id_cliente_seguro, 'cliente': row.get("cliente"),
                        'direccion': row.get("direccion"), 'num_factura': row.get("num_factura"),
                        'tipo_documento': row.get("tipo_documento"), 'almacen': row.get("almacen"),
                        'fecha_factura': row.get("fecha_factura"), 'vendedor': row.get("vendedor"),
                        'id_vendedor': id_vendedor_seguro, 'fecha_entrega': row.get("fecha_entrega"),
                        'fecha_vencimiento': row.get("fecha_vencimiento"), 'total_factura': total_factura_db,
                        'total_cobrado': total_cobrado_db, 'pendiente_cobrar': pendiente_cobrar_db,
                        'plazos_pago': plazos_pago_seguro, 'dias_credito': dias_credito,
                        'dias_cuotas': dias_cuota, 'cant_cuotas': cant_cuotas,
                        'estado_pago': 'Pendiente', # Estado inicial, se actualizará después
                        'idodoo_vendedor': idodoo_vendedor_externo, 'idodoo_clientes': idodoo_clientes_externo,
                        'idodoo_plazospago': id_plazospago_seguro, 'idodoo': idodoo_actual
                    }

                    # Ejecutar INSERT o UPDATE
                    if idodoo_actual in ids_existentes:
                        # UPDATE
                        update_cols = [col for col in datos_factura_dict.keys() if col != 'idodoo']
                        set_clause = ', '.join([f"{col} = %s" for col in update_cols])
                        sql_update = f"UPDATE facturas SET {set_clause} WHERE idodoo = %s"
                        valores_update = [datos_factura_dict[col] for col in update_cols]
                        valores_update.append(idodoo_actual)
                        cursor.execute(sql_update, tuple(valores_update))
                        registros_actualizados += 1
                    else:
                        # INSERT
                        insert_cols = list(datos_factura_dict.keys())
                        placeholders = ', '.join(['%s'] * len(insert_cols))
                        sql_insert = f"INSERT INTO facturas ({', '.join(insert_cols)}) VALUES ({placeholders})"
                        valores_insert = [datos_factura_dict[col] for col in insert_cols]
                        cursor.execute(sql_insert, tuple(valores_insert))
                        registros_insertados += 1

                except Exception as e:
                    print(f"\n[ERROR] en fila Excel {index + 2} (ID Odoo: {idodoo_actual}): {e}")
                    # print("      Datos:", datos_factura_dict) # Descomentar para depurar
                    registros_con_error_fila += 1

            print(f"\n[INFO] Procesamiento de {total_filas_a_procesar} facturas 'publicadas' completado.")

            # 8. COMMIT o ROLLBACK FINAL
            if registros_con_error_fila == 0:
                print("\n[DB] Realizando COMMIT final (incluye borrados e inserciones/actualizaciones)...")
                conexion.commit()
                commit_realizado = True
                importacion_exitosa = True # Marcamos como éxito para llamar a cuotas
                print("(+) Commit realizado.")
            else:
                print(f"\n[WARN] Hubo {registros_con_error_fila} errores durante la inserción/actualización.")
                print("[DB] Realizando ROLLBACK para deshacer todos los cambios (incluidos borrados)...")
                conexion.rollback()
                importacion_exitosa = False
                print("(-) Rollback realizado.")

# --- Bloques except y finally ---
except Exception as e_general:
    print(f"\n[ERROR] ERROR GENERAL INESPERADO (Importación Facturas): {e_general}")
    importacion_exitosa = False
    if conexion:
        try:
            print("[DB] Intentando realizar ROLLBACK...")
            conexion.rollback()
            print("(-) Rollback realizado.")
        except Exception as rb_err:
            print(f"[WARN] Error durante el rollback: {rb_err}")
finally:
    # 9. MOSTRAR RESUMEN DE IMPORTACIÓN
    def safe_print(var_name, value):
        display_value = value if value is not None else '--'
        print(f"{var_name:<40}: {display_value}")

    print("\n--- Resumen Importación Facturas ---")
    safe_print("Total filas leídas del Excel", locals().get('total_filas_excel'))
    safe_print("Registros Eliminados (Borrador/Cancelado)", locals().get('registros_eliminados'))
    safe_print("Registros Nuevos Insertados (Publicado)", locals().get('registros_insertados'))
    safe_print("Registros Existentes Actualizados (Publicado)", locals().get('registros_actualizados'))
    print("-------------------------------------------")
    total_procesados_bd = (locals().get('registros_insertados', 0) or 0) + \
                          (locals().get('registros_actualizados', 0) or 0) + \
                          (locals().get('registros_eliminados', 0) or 0)
    safe_print("Total Registros Afectados BD", total_procesados_bd)
    print("-------------------------------------------")
    safe_print("Filas Omitidas (Sin ID Odoo)", locals().get('registros_omitidos_sin_idodoo'))
    safe_print("Filas Omitidas (Otro Estado)", locals().get('registros_omitidos_otro_estado'))
    safe_print("Filas con Error Procesamiento", locals().get('registros_con_error_fila'))
    print("===========================================")

    if commit_realizado: print("Estado: Cambios GUARDADOS en la BD.")
    else: print("Estado: Cambios DESHECHOS (Rollback) o no hubo cambios / hubo errores.")

    # 10. CERRAR RECURSOS
    if cursor: cursor.close(); print("[DB] Cursor de facturas cerrado.")
    if conexion and conexion.is_connected(): conexion.close(); print("[DB] Conexión a MySQL cerrada.")

# 11. LLAMAR AL SCRIPT DE CUOTAS (SOLO SI LA IMPORTACIÓN FUE EXITOSA)
# (Lógica sin cambios)
proceso_cuotas_exitoso = False
if importacion_exitosa:
    # ... (código existente para llamar a generar_cuotas.py) ...
    print("\n----------------------------------------------------")
    print(f">> Ejecutando script de generación de cuotas: '{script_cuotas}'...")
    print("----------------------------------------------------")
    try:
        child_env = os.environ.copy()
        child_env['PYTHONIOENCODING'] = 'utf-8'
        resultado_subprocess = subprocess.run(
            [sys.executable, script_cuotas], check=False, capture_output=True, text=True,
            encoding='utf-8', errors='replace', env=child_env
        )
        print(f"\n--- Salida del Script '{script_cuotas}' ---")
        print(resultado_subprocess.stdout)
        if resultado_subprocess.stderr: print(f"\n--- Errores Impresos por '{script_cuotas}' ---\n{resultado_subprocess.stderr}")

        if resultado_subprocess.returncode == 0:
            print(f"\n[OK] Script '{script_cuotas}' ejecutado exitosamente.")
            proceso_cuotas_exitoso = True
        else:
             raise subprocess.CalledProcessError(
                 returncode=resultado_subprocess.returncode, cmd=resultado_subprocess.args,
                 output=resultado_subprocess.stdout, stderr=resultado_subprocess.stderr
             )
    except FileNotFoundError: print(f"[ERROR] FATAL: No se encontró el script '{script_cuotas}'.")
    except subprocess.CalledProcessError as e: print(f"[ERROR] FATAL: Script '{script_cuotas}' falló (rc={e.returncode}).\n--- Salida ---\n{e.output}\n--- Error ---\n{e.stderr}")
    except Exception as e_subproc: print(f"[ERROR] FATAL inesperado al ejecutar '{script_cuotas}': {e_subproc}")

else:
    print("\n----------------------------------------------------")
    print("[WARN] Importación de facturas fallida o incompleta. Se OMITE la ejecución de cuotas.")
    print("----------------------------------------------------")

# 12. SALIDA FINAL DEL SCRIPT PRINCIPAL
# (Lógica sin cambios)
print("\n====================================================")
if importacion_exitosa and proceso_cuotas_exitoso: print(">>> PROCESO COMPLETO (Facturas y Cuotas) FINALIZADO EXITOSAMENTE <<<"); sys.exit(0)
elif importacion_exitosa and not proceso_cuotas_exitoso: print(">>> PROCESO INCOMPLETO: Facturas importadas, pero falló la generación de Cuotas. <<<"); sys.exit(1)
else: print(">>> PROCESO FALLIDO: La importación de Facturas falló. No se generaron cuotas. <<<"); sys.exit(1)