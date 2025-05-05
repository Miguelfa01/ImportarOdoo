# -*- coding: utf-8 -*-
# Guardar como: importar_facturas.py

import pandas as pd
from datetime import date # Necesario para el cálculo de dias_credito
from conexion_mysql import conectar  # Usamos tu conexión centralizada

# --- Módulos para llamar al segundo script ---
import subprocess
import sys
import os
# ------------------------------------------

print("--- Script: importar_facturas.py ---")

# --- Funciones Auxiliares ---
def limpiar_float(valor):
    """Limpia y convierte un valor a float, devolviendo 0.0 en caso de error o NaN/None."""
    try:
        if pd.isna(valor) or str(valor).strip().lower() in ["<na>", "nan", "none", "", "#n/a"]:
            return 0.0
        return float(valor)
    except (ValueError, TypeError):
        return 0.0

# --- Variables ---
archivo_excel = "C:/mysql_import/Asiento contable (account.move).xlsx" # <- CONFIRMA RUTA
script_cuotas = "generar_cuotas.py" # <- CONFIRMA NOMBRE DEL SEGUNDO SCRIPT
conexion = None
cursor = None
importacion_exitosa = False # Bandera para saber si se ejecuta el segundo script
commit_realizado = False

# --- Contadores ---
registros_insertados = 0
registros_actualizados = 0
registros_omitidos_sin_idodoo = 0
registros_con_error_fila = 0 # Errores procesando filas individuales
total_filas_excel = 0

try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion:
        print("[ERROR] Fatal: No se pudo conectar a la base de datos.")
        sys.exit(1) # Salir si no hay conexión

    cursor = conexion.cursor(dictionary=True) # Usar dictionary=True es útil
    print("[OK] Conexión establecida.")

    # 2. LEER EXCEL
    print(f"[INFO] Leyendo archivo Excel: {archivo_excel}")
    try:
        df = pd.read_excel(archivo_excel, sheet_name="Sheet1", engine="openpyxl")
        total_filas_excel = len(df)
        print(f"[INFO] Archivo leído. {total_filas_excel} filas encontradas.")
    except FileNotFoundError:
        print(f"[ERROR] Fatal: No se encontró el archivo Excel: {archivo_excel}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Fatal al leer el archivo Excel: {e}")
        sys.exit(1)

    # 3. RENOMBRAR Y PREPARAR DATAFRAME
    print("[INFO] Preparando datos del DataFrame...")
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
        "Estado de pago": "estado_pago", 
        "Vendedor": "vendedor",
        "Vendedor/ID": "idodoo_vendedor", 
        "ID": "idodoo",
        "Empresa/ID": "idodoo_clientes",
        "Plazos de pago/ID": "idodoo_plazospago", 
        "Importe adeudado con signo": "pendiente_cobrar"
    }
    df = df.rename(columns=column_mapping)

    # Verificar columnas esenciales
    columnas_esenciales = ['idodoo', 'idodoo_clientes', 'idodoo_vendedor', 'total_factura', 'pendiente_cobrar', 'fecha_factura', 'fecha_vencimiento']
    columnas_faltantes = [col for col in columnas_esenciales if col not in df.columns]
    if columnas_faltantes:
        msg = f"Faltan columnas esenciales en Excel: {', '.join(columnas_faltantes)}"
        print(f"[ERROR] Fatal: {msg}")
        sys.exit(1)

    # Agregar campos faltantes
    if "almacen" not in df.columns: df["almacen"] = "Principal"
    if "dias_credito" not in df.columns: df["dias_credito"] = None
    if "dias_cuotas" not in df.columns: df["dias_cuotas"] = None
    if "cant_cuotas" not in df.columns: df["cant_cuotas"] = None

    # Obtener mapeos de Clientes y Vendedores
    cursor.execute("SELECT id, idodoo FROM clientes")
    clientes_dict = {str(row["idodoo"]): row["id"] for row in cursor.fetchall() if row["idodoo"] is not None}
    #print (clientes_dict) # Revisar clientes
    
    def map_cliente(idodoo_cliente):
        if pd.isna(idodoo_cliente): return None
        try: return clientes_dict.get(str(int(float(idodoo_cliente))))
        except (ValueError, TypeError): return None
        
    df["id_cliente"] = df["idodoo_clientes"].apply(map_cliente)

    #cursor.execute("SELECT idVendedores, nombre FROM vendedores")
    #vendedores_dict = {str(nombre).lower(): id for id, nombre in cursor.fetchall() if nombre is not None}
    
    cursor.execute("SELECT idVendedores, nombre FROM vendedores")
    vendedores_dict = {str(row["nombre"]).lower(): row["idVendedores"] for row in cursor.fetchall() if row["nombre"] is not None}

    def map_vendedor(nombre_vendedor):
        if pd.isna(nombre_vendedor): return None
        try: return vendedores_dict.get(str(nombre_vendedor).lower())
        except (ValueError, TypeError): return None
    df["id_vendedor"] = df["vendedor"].apply(map_vendedor)

    # Limpiar Fechas y convertir NaN a None
    df["fecha_factura"] = pd.to_datetime(df["fecha_factura"], errors="coerce").dt.date
    df["fecha_entrega"] = pd.to_datetime(df["fecha_entrega"], errors="coerce").dt.date
    df["fecha_vencimiento"] = pd.to_datetime(df["fecha_vencimiento"], errors="coerce").dt.date
    df = df.astype(object).where(pd.notna(df), None)
    print("[OK] Datos preparados.")

    # 4. PROCESAR FILAS (INSERT/UPDATE)
    print("[INFO] Procesando filas de facturas para INSERT/UPDATE...")
    for index, row in df.iterrows():
        #print(f"\rProcesando fila {index + 1}/{total_filas_excel}...", end="")
        try:
            idodoo_seguro = None
            if pd.notna(row.get("idodoo")) and str(row.get("idodoo")).strip() not in ["<NA>", "nan", "None", ""]:
                try: idodoo_seguro = int(float(row["idodoo"]))
                except (ValueError, TypeError): idodoo_seguro = None

            if idodoo_seguro is None:
                registros_omitidos_sin_idodoo += 1
                continue # Saltar esta fila

            # Obtener datos de plazos de pago
            cant_cuotas, dias_cuota, dias_credito = None, 0, 0
            id_plazospago_seguro = None
            if pd.notna(row.get("idodoo_plazospago")):
                try: id_plazospago_seguro = int(float(row["idodoo_plazospago"]))
                except (ValueError, TypeError): id_plazospago_seguro = None

            if id_plazospago_seguro:
                cursor.execute("SELECT dias_credito, cant_cuotas, dias_cuota FROM plazos_pago WHERE idodoo = %s", (id_plazospago_seguro,))
                resultado_plazo = cursor.fetchone() # fetchone devuelve dict si cursor es dictionary=True
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
            id_cliente_seguro = int(row["id_cliente"]) if pd.notna(row.get("id_cliente")) else None
            id_vendedor_seguro = int(row["id_vendedor"]) if pd.notna(row.get("id_vendedor")) else None
            idodoo_vendedor_externo = int(float(row["idodoo_vendedor"])) if pd.notna(row.get("idodoo_vendedor")) else None
            idodoo_clientes_externo = int(float(row["idodoo_clientes"])) if pd.notna(row.get("idodoo_clientes")) else None
            plazos_pago_seguro = row.get("plazos_pago") if pd.notna(row.get("plazos_pago")) else None

            total_factura = limpiar_float(row.get("total_factura"))
            pendiente_cobrar = limpiar_float(row.get("pendiente_cobrar"))
            total_cobrado = round(total_factura - pendiente_cobrar, 2)
            total_factura_db = total_factura if pd.notna(row.get("total_factura")) else None
            pendiente_cobrar_db = pendiente_cobrar if pd.notna(row.get("pendiente_cobrar")) else None
            total_cobrado_db = float(total_cobrado)

            # Consultar si existe
            cursor.execute("SELECT id FROM facturas WHERE idodoo = %s", (idodoo_seguro,))
            resultado = cursor.fetchone()

            # Preparar datos comunes
            datos_factura = (
                row.get("rif"), id_cliente_seguro, row.get("cliente"), row.get("direccion"), row.get("num_factura"),
                row.get("tipo_documento"), row.get("almacen"), row.get("fecha_factura"), row.get("vendedor"),
                id_vendedor_seguro, row.get("fecha_entrega"), row.get("fecha_vencimiento"),
                total_factura_db, total_cobrado_db, pendiente_cobrar_db, plazos_pago_seguro, dias_credito,
                dias_cuota, cant_cuotas, row.get("estado_pago"),
                idodoo_vendedor_externo, idodoo_clientes_externo, id_plazospago_seguro
            )

            # Ejecutar INSERT o UPDATE
            if resultado is None:
                sql = """INSERT INTO facturas (rif, id_cliente, cliente, direccion, num_factura, tipo_documento, almacen, fecha_factura, vendedor, id_vendedor, fecha_entrega, fecha_vencimiento, total_factura, total_cobrado, pendiente_cobrar, plazos_pago, dias_credito, dias_cuotas, cant_cuotas, estado_pago, idodoo_vendedor, idodoo_clientes, idodoo_plazospago, idodoo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql, datos_factura + (idodoo_seguro,))
                registros_insertados += 1
            else:
                sql = """UPDATE facturas SET rif = %s, id_cliente = %s, cliente = %s, direccion = %s, num_factura = %s, tipo_documento = %s, almacen = %s, fecha_factura = %s, vendedor = %s, id_vendedor = %s, fecha_entrega = %s, fecha_vencimiento = %s, total_factura = %s, total_cobrado = %s, pendiente_cobrar = %s, plazos_pago = %s, dias_credito = %s, dias_cuotas = %s, cant_cuotas = %s, estado_pago = %s, idodoo_vendedor = %s, idodoo_clientes = %s, idodoo_plazospago = %s WHERE idodoo = %s"""
                cursor.execute(sql, datos_factura + (idodoo_seguro,))
                registros_actualizados += 1

        except Exception as e:
            # Error procesando una fila específica
            print(f"\n[ERROR] en fila Excel {index + 2} (idodoo: {row.get('idodoo', 'N/A')}): {e}")
            registros_con_error_fila += 1
            # Puedes imprimir `row.to_dict()` aquí si necesitas depurar esa fila
            # Decidimos continuar con las siguientes filas

    print("\n[INFO] Procesamiento de filas de facturas completado.")

    # 5. COMMIT (si no hubo errores graves y hay cambios)
    # Commit si menos del 50% de las filas tuvieron errores individuales (ajustar si es necesario)
    commit_threshold_met = registros_con_error_fila < (total_filas_excel * 0.5) if total_filas_excel > 0 else True
    if commit_threshold_met:
        print("\n[DB] Realizando COMMIT de los cambios de facturas...")
        conexion.commit()
        commit_realizado = True
        importacion_exitosa = True # Marcamos como exitoso para llamar al script de cuotas
        print("(+) Commit realizado.")
    else:
        print("\n[WARN] Demasiados errores procesando filas. No se realizó COMMIT.")
        print("[DB] Realizando ROLLBACK...")
        conexion.rollback()
        importacion_exitosa = False # Marcamos como fallido
        print("(-) Rollback realizado.")


except Exception as e_general:
    # Error general fuera del bucle de filas
    print(f"\n[ERROR] ERROR GENERAL INESPERADO (Importación Facturas): {e_general}")
    importacion_exitosa = False
    if conexion: # Si hubo conexión, intentar rollback
        try:
            print("[DB] Intentando realizar ROLLBACK debido a error general...")
            conexion.rollback()
            print("(-) Rollback realizado.")
        except Exception as rb_err:
            print(f"[WARN] Error durante el rollback: {rb_err}")


finally:
    # 6. MOSTRAR RESUMEN DE IMPORTACIÓN
    print("\n--- Resumen Importación Facturas ---")
    print(f"Total filas leídas del Excel : {total_filas_excel}")
    print(f"Registros Nuevos Insertados    : {registros_insertados}")
    print(f"Registros Existentes Actualizados: {registros_actualizados}")
    print(f"---------------------------------")
    total_importados = registros_insertados + registros_actualizados
    print(f"Total Registros Procesados BD  : {total_importados}")
    print(f"---------------------------------")
    print(f"Registros Omitidos (sin idodoo): {registros_omitidos_sin_idodoo}")
    print(f"Filas con Error (omitidas BD)  : {registros_con_error_fila}")
    print(f"---------------------------------")
    total_omitidos_o_error = registros_omitidos_sin_idodoo + registros_con_error_fila
    print(f"Total Filas No Procesadas BD : {total_omitidos_o_error}")
    print("=================================")
    # Verificación de conteo
    if total_filas_excel == total_importados + total_omitidos_o_error:
        print("[OK] Verificación: Total filas Excel coincide con Procesados BD + No Procesados BD.")
    else:
        # Puede pasar si hay filas vacías al final del Excel o si hay errores no contados
        print(f"[WARN] Verificación: Suma ({total_importados + total_omitidos_o_error}) no coincide con Total Excel ({total_filas_excel}).")

    if commit_realizado:
        print("Estado: Cambios de Facturas GUARDADOS en la BD.")
    else:
        print("Estado: Cambios de Facturas DESHECHOS (Rollback) o no hubo cambios / hubo errores graves.")

    # 7. CERRAR RECURSOS DE ESTE SCRIPT
    if cursor:
        cursor.close()
        print("[DB] Cursor de facturas cerrado.")
    if conexion and conexion.is_connected():
        conexion.close()
        print("[DB] Conexión a MySQL cerrada para facturas.")


# 8. LLAMAR AL SCRIPT DE CUOTAS (SOLO SI LA IMPORTACIÓN FUE EXITOSA)
proceso_cuotas_exitoso = False
if importacion_exitosa:
    print("\n----------------------------------------------------")
    print(f">> Ejecutando script de generación de cuotas: '{script_cuotas}'...")
    print("----------------------------------------------------")
    try:
        child_env = os.environ.copy()
        child_env['PYTHONIOENCODING'] = 'utf-8'
        # Usamos sys.executable para asegurarnos de usar el mismo intérprete de Python
        # check=True hará que lance una excepción si el script de cuotas falla (retorna != 0)
        resultado_subprocess = subprocess.run(
            [sys.executable, script_cuotas],
            check=False,          # Lanza excepción si el script falla
            text=True,           # Codifica stdout/stderr como texto (usando encoding por defecto)
            capture_output=True, # Captura la salida para mostrarla
            encoding='utf-8',    # Intentar decodificar la salida como UTF-8 (puede ayudar)
            errors='replace',     # Reemplaza caracteres no decodificables en la salida capturada
            env=child_env
        )
        print(f"\n--- Salida del Script '{script_cuotas}' ---")
        print(resultado_subprocess.stdout) # Muestra la salida normal del script de cuotas
        if resultado_subprocess.stderr: # Muestra si hubo errores impresos por el script de cuotas
            print(f"\n--- Errores Impresos por '{script_cuotas}' ---")
            print(resultado_subprocess.stderr)
        print(f"\n[OK] Script '{script_cuotas}' ejecutado exitosamente.")
        proceso_cuotas_exitoso = True

    except FileNotFoundError:
        print(f"[ERROR] FATAL: No se encontró el script '{script_cuotas}'.")
        print(f"   Asegúrate de que el archivo exista y el nombre sea correcto.")
    except subprocess.CalledProcessError as e:
        # Esto se activa si el script de cuotas termina con sys.exit(1) u otro error
        print(f"[ERROR] FATAL: El script '{script_cuotas}' falló durante su ejecución.")
        print(f"   Código de retorno: {e.returncode}")
        print(f"\n--- Salida del Script '{script_cuotas}' antes de fallar (stdout) ---")
        print(e.stdout)
        print(f"\n--- Errores del Script '{script_cuotas}' (stderr) ---")
        print(e.stderr)
    except Exception as e_subproc:
        print(f"[ERROR] FATAL inesperado al intentar ejecutar '{script_cuotas}': {e_subproc}")

else:
    print("\n----------------------------------------------------")
    print("[WARN] La importación de facturas NO fue exitosa o tuvo errores graves.")
    print(f"   Se OMITE la ejecución del script '{script_cuotas}'.")
    print("----------------------------------------------------")


# 9. SALIDA FINAL DEL SCRIPT PRINCIPAL
print("\n====================================================")
if importacion_exitosa and proceso_cuotas_exitoso:
    print(">>> PROCESO COMPLETO (Facturas y Cuotas) FINALIZADO EXITOSAMENTE <<<")
    sys.exit(0) # Código 0 indica éxito total
elif importacion_exitosa and not proceso_cuotas_exitoso:
    print(">>> PROCESO INCOMPLETO: Facturas importadas, pero falló la generación de Cuotas. <<<")
    sys.exit(1) # Código 1 indica error parcial
else:
    print(">>> PROCESO FALLIDO: La importación de Facturas falló. No se generaron cuotas. <<<")
    sys.exit(1) # Código 1 indica error