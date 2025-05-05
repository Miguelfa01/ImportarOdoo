# -*- coding: utf-8 -*-
# Guardar como: generar_cuotas.py

from datetime import timedelta, date, datetime
from conexion_mysql import conectar  # Usa la misma conexión
import sys # Para sys.exit()

print("\n--- Script: generar_cuotas.py ---")

# --- Variables ---
conexion = None
cursor = None
proceso_exitoso = False # Bandera de éxito para este script

# --- Contadores ---
facturas_leidas = 0
facturas_omitidas_data = 0
facturas_procesadas = 0
cuotas_generadas_total = 0
cuotas_pagadas = 0
cuotas_pendientes = 0
cuotas_parciales = 0
cuotas_vencidas = 0
cuotas_por_vencer = 0
errores_calculo_fecha = 0
cuotas_eliminadas = 0 # Para contar las borradas con DELETE

try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos (para cuotas)...")
    conexion = conectar()
    if not conexion:
        print("[ERROR] Fatal: No se pudo conectar a la base de datos.")
        sys.exit(1) # Salir si no hay conexión

    cursor = conexion.cursor(dictionary=True)
    print("[OK] Conexión establecida.")

    # 2. OBTENER FECHA Y FACTURAS ELEGIBLES
    hoy = date.today()
    print(f"[INFO] Fecha actual para comparación de vencimiento: {hoy}")

    print("[INFO] Obteniendo facturas elegibles de la base de datos...")
    sql_select_facturas = """
        SELECT id AS id_factura, id_cliente, num_factura, total_factura,
               total_cobrado, cant_cuotas, dias_cuotas,
               COALESCE(fecha_entrega, fecha_factura) AS fecha_base, id_vendedor
        FROM facturas
        WHERE cant_cuotas IS NOT NULL AND cant_cuotas > 0
          AND COALESCE(fecha_entrega, fecha_factura) IS NOT NULL
          AND dias_cuotas IS NOT NULL AND dias_cuotas >= 0
    """
    cursor.execute(sql_select_facturas)
    facturas = cursor.fetchall()
    facturas_leidas = len(facturas)
    print(f"[INFO] {facturas_leidas} facturas encontradas para generar cuotas.")

    if facturas_leidas == 0:
        print("[INFO] No hay facturas elegibles para generar cuotas. Proceso de cuotas completado.")
        proceso_exitoso = True # Se considera éxito si no había nada que hacer
        # No necesitamos hacer commit ni rollback si no hicimos nada
    else:
        # 3. LIMPIAR TABLA CUOTAS
        print("[DB] Limpiando tabla 'cuotas' existente...")
        cursor.execute("DELETE FROM cuotas")
        cuotas_eliminadas = cursor.rowcount # Obtener número de filas borradas
        print(f"[OK] Tabla 'cuotas' limpiada ({cuotas_eliminadas} registros eliminados).")

        # 4. GENERAR CUOTAS
        print(f"[INFO] Procesando {facturas_leidas} facturas para generar cuotas...")
        for i, factura in enumerate(facturas):
            #print(f"\rProcesando factura {i+1}/{facturas_leidas} (ID: {factura.get('id_factura', 'N/A')})...", end="")

            # Validar datos de la factura
            fecha_base_raw = factura.get("fecha_base")
            try:
                # Validar datos clave antes de procesar
                if not all([
                    factura.get("total_factura") is not None,
                    factura.get("cant_cuotas") and int(factura["cant_cuotas"]) > 0,
                    fecha_base_raw,
                    factura.get("dias_cuotas") is not None
                ]):
                    facturas_omitidas_data += 1
                    continue # Saltar a la siguiente factura

                # Procesar factura válida
                facturas_procesadas += 1
                monto_total_factura = float(factura["total_factura"])
                num_cuotas = int(factura["cant_cuotas"])
                dias_intervalo = int(factura["dias_cuotas"])
                total_ya_cobrado = float(factura.get("total_cobrado", 0) or 0)

                if num_cuotas == 0: continue # Seguridad extra

                monto_cuota_base = round(monto_total_factura / num_cuotas, 2)
                monto_total_calculado_excepto_ultima = monto_cuota_base * (num_cuotas - 1)
                restante_a_aplicar = total_ya_cobrado

                # Validar y convertir fecha_base (más robusto)
                fecha_base_dt = None
                if isinstance(fecha_base_raw, datetime): fecha_base_dt = fecha_base_raw.date()
                elif isinstance(fecha_base_raw, date): fecha_base_dt = fecha_base_raw
                elif isinstance(fecha_base_raw, str):
                    try: fecha_base_dt = date.fromisoformat(fecha_base_raw.split()[0])
                    except ValueError:
                        try: fecha_base_dt = datetime.strptime(fecha_base_raw.split()[0], '%Y-%m-%d').date()
                        except ValueError: raise ValueError(f"Formato fecha no reconocido: {fecha_base_raw}")
                else: raise ValueError("Tipo fecha base no reconocido")

                # Generar cada cuota
                for nro in range(1, num_cuotas + 1):
                    fecha_vencimiento_dt = fecha_base_dt # Default
                    try:
                        dias_a_sumar = nro * dias_intervalo
                        fecha_vencimiento_dt = fecha_base_dt + timedelta(days=dias_a_sumar)
                    except OverflowError: errores_calculo_fecha += 1
                    except Exception: errores_calculo_fecha += 1

                    estado_vencimiento = "Por vencer"
                    if fecha_vencimiento_dt < hoy:
                        estado_vencimiento = f"Vencido {fecha_vencimiento_dt.year}"
                        cuotas_vencidas += 1
                    else:
                        cuotas_por_vencer += 1

                    monto_cuota_actual = monto_cuota_base
                    if nro == num_cuotas:
                        monto_cuota_actual = round(monto_total_factura - monto_total_calculado_excepto_ultima, 2)

                    monto_cobrado_cuota = max(0, min(monto_cuota_actual, restante_a_aplicar))
                    pendiente_cobrar_cuota = max(0, round(monto_cuota_actual - monto_cobrado_cuota, 2))
                    restante_a_aplicar = max(0, restante_a_aplicar - monto_cobrado_cuota)

                    estado_pago_cuota = "Pendiente"
                    tolerancia = 0.005
                    if pendiente_cobrar_cuota <= tolerancia:
                        estado_pago_cuota = "Pagada"
                        pendiente_cobrar_cuota = 0.0
                        cuotas_pagadas += 1
                    elif monto_cobrado_cuota > tolerancia:
                        estado_pago_cuota = "Parcial"
                        cuotas_parciales += 1
                    else:
                        monto_cobrado_cuota = 0.0
                        cuotas_pendientes += 1

                    # Insertar cuota
                    cursor.execute("""
                        INSERT INTO cuotas (id_factura, id_cliente, num_factura, nro_cuota, monto_cuota, monto_cobrado, pendiente_cobrar, estado, fecha_vencimiento, id_vendedor, estado_vencimiento)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        factura["id_factura"], factura.get("id_cliente"), factura.get("num_factura"), nro,
                        monto_cuota_actual, monto_cobrado_cuota, pendiente_cobrar_cuota, estado_pago_cuota,
                        fecha_vencimiento_dt, factura.get("id_vendedor"), estado_vencimiento
                    ))
                    cuotas_generadas_total += 1

            except Exception as e_factura:
                # Error procesando una factura específica y sus cuotas
                print(f"\n[ERROR] procesando cuotas para factura ID {factura.get('id_factura', 'N/A')}: {e_factura}")
                facturas_omitidas_data += 1 # Contar como omitida por error
                # Revertir el conteo si ya se había incrementado facturas_procesadas
                if facturas_procesadas > 0 and 'monto_total_factura' in locals(): # Chequeo simple
                    facturas_procesadas -= 1


        print("\n[INFO] Procesamiento de generación de cuotas completado.")

        # 5. COMMIT (si no hubo errores graves)
        # Decidimos hacer commit incluso si algunas facturas fallaron, pero las que sí se procesaron se guardan.
        # Si quieres ser más estricto (rollback si *alguna* falló), cambia esta lógica.
        print("\n[DB] Realizando COMMIT de los cambios de cuotas...")
        conexion.commit()
        proceso_exitoso = True # Marcamos éxito si llegamos aquí y hicimos commit
        print("(+) Commit de cuotas realizado.")

except Exception as e_general_cuotas:
    # Error general fuera del bucle de facturas
    print(f"\n[ERROR] ERROR GENERAL INESPERADO (Generación Cuotas): {e_general_cuotas}")
    proceso_exitoso = False
    if conexion: # Intentar rollback si hubo conexión
        try:
            print("[DB] Intentando realizar ROLLBACK debido a error general...")
            conexion.rollback()
            print("(-) Rollback realizado.")
        except Exception as rb_err:
            print(f"[WARN] Error durante el rollback: {rb_err}")

finally:
    # 6. MOSTRAR RESUMEN DE CUOTAS
    print("\n--- Resumen Generación Cuotas ---")
    print(f"Facturas leídas BD elegibles : {facturas_leidas}")
    print(f"Facturas omitidas (datos/err): {facturas_omitidas_data}")
    print(f"Facturas procesadas cuotas   : {facturas_procesadas}")
    print("-----------------------------------")
    print(f"Registros de cuotas eliminados: {cuotas_eliminadas}")
    print(f"Registros de cuotas generados : {cuotas_generadas_total}")
    print("-----------------------------------")
    print(f"  Cuotas Pagadas    : {cuotas_pagadas}")
    print(f"  Cuotas Pendientes : {cuotas_pendientes}")
    print(f"  Cuotas Parciales  : {cuotas_parciales}")
    print(f"  (Suma estados: {cuotas_pagadas + cuotas_pendientes + cuotas_parciales})")
    print("-----------------------------------")
    print(f"  Cuotas Vencidas   : {cuotas_vencidas}")
    print(f"  Cuotas Por Vencer : {cuotas_por_vencer}")
    print(f"  (Suma vencimiento: {cuotas_vencidas + cuotas_por_vencer})")
    if errores_calculo_fecha > 0:
        print(f"Errores cálculo fecha : {errores_calculo_fecha}")
    print("===================================")

    # 7. CERRAR RECURSOS DE ESTE SCRIPT
    if cursor:
        cursor.close()
        print("[DB] Cursor de cuotas cerrado.")
    if conexion and conexion.is_connected():
        conexion.close()
        print("[DB] Conexión a MySQL cerrada para cuotas.")

# 8. SALIDA FINAL DEL SCRIPT DE CUOTAS (para subprocess)
if proceso_exitoso:
    print("\n[OK] Script de generación de cuotas finalizado correctamente.")
    sys.exit(0) # Código 0 indica éxito
else:
    print("\n[ERROR] Script de generación de cuotas finalizado con errores.")
    sys.exit(1) # Código 1 indica error