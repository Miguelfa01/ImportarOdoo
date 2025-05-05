# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP # Usar Decimal para precisión monetaria y especificar redondeo
import mysql.connector # Importar para manejar errores específicos y usar ping

# --- Importar función de conexión ---
try:
    from conexion_mysql import conectar
except ImportError:
    print("Error: No se pudo encontrar el archivo 'conexion_mysql.py' o la función 'conectar'.")
    def conectar():
        print("ADVERTENCIA: Usando conexión dummy.")
        return None
    # exit()

# --- Constantes y Configuración ---
# Fechas actualizadas según tu solicitud anterior
FECHA_INICIO_QUINCENA = datetime(2025, 4, 16)
FECHA_FIN_QUINCENA = datetime(2025, 4, 30, 23, 59, 59)
ARCHIVO_SALIDA_EXCEL = f"reporte_comisiones_{FECHA_INICIO_QUINCENA.strftime('%Y%m%d')}_{FECHA_FIN_QUINCENA.strftime('%Y%m%d')}.xlsx"

# --- Funciones Auxiliares ---

def obtener_reglas_comision(cursor):
    """Obtiene las reglas de comisión de la base de datos."""
    print("[DB] Obteniendo reglas de comisión...")
    query = "SELECT id, dias_desde, dias_hasta, porcentaje, descripcion FROM comision_por_antiguedad ORDER BY dias_desde"
    cursor.execute(query)
    reglas = cursor.fetchall()
    if not reglas:
        raise ValueError("No se encontraron reglas de comisión en la tabla 'comision_por_antiguedad'.")
    print(f"[OK] {len(reglas)} reglas de comisión obtenidas.")
    for regla in reglas:
        regla['porcentaje'] = Decimal(regla['porcentaje']) / Decimal(100)
    return reglas

def buscar_tasa_comision(dias_vencido, reglas_comision):
    """Encuentra la tasa de comisión aplicable según los días de vencimiento."""
    for regla in reglas_comision:
        dias_desde = regla['dias_desde'] if regla['dias_desde'] is not None else -float('inf')
        dias_hasta = regla['dias_hasta'] if regla['dias_hasta'] is not None else float('inf')
        if dias_desde <= dias_vencido <= dias_hasta:
            return regla['porcentaje'], regla.get('descripcion', f"{dias_desde} a {dias_hasta} días")
    # Si no se encuentra ninguna regla (debería haber una que cubra todos los casos)
    # Ajustamos el mensaje para que sea más informativo si esto ocurre
    print(f"ADVERTENCIA: No se encontró regla de comisión aplicable para {dias_vencido} días. Verifique la tabla 'comision_por_antiguedad'. Se asignará 0%.")
    return Decimal(0), "Sin Regla Aplicable"

# --- Nueva Función ---

def obtener_detalle_pagos_periodo(fecha_inicio, fecha_fin, conexion):
    """Obtiene los detalles de los pagos del período desde la tabla pagos."""
    cursor = None
    df_pagos = pd.DataFrame() # Inicializar DataFrame vacío

    try:
        cursor_opts = {'dictionary': True, 'buffered': True}
        # Ping antes de crear el cursor
        try:
            print("[DB] Verificando conexión para detalle de pagos...")
            conexion.ping(reconnect=True, attempts=3, delay=1)
        except mysql.connector.Error as err:
            print(f"Error de conexión antes de obtener detalle de pagos: {err}")
            raise # Relanzar si falla el ping aquí
        cursor = conexion.cursor(**cursor_opts)
        print("[DB] Obteniendo detalle de pagos del período...")

        query = """
            SELECT
                p.id AS ID_Pago, p.fecha_pago AS Fecha_Pago, p.monto AS Monto_Pago,
                p.diario AS Diario, p.id_cliente AS ID_Cliente, c.nombre AS Nombre_Cliente
            FROM pagos p LEFT JOIN clientes c ON p.id_cliente = c.id
            WHERE p.fecha_pago BETWEEN %s AND %s
            ORDER BY p.fecha_pago ASC, p.id ASC;
        """
        cursor.execute(query, (fecha_inicio, fecha_fin))
        pagos_data = cursor.fetchall()
        print(f"[OK] {len(pagos_data)} registros de pago encontrados en el período.")

        if pagos_data:
            df_pagos = pd.DataFrame(pagos_data)
            df_pagos['Fecha_Pago'] = pd.to_datetime(df_pagos['Fecha_Pago']).dt.date
            df_pagos['Monto_Pago'] = pd.to_numeric(df_pagos['Monto_Pago'], errors='coerce').fillna(0)
            total_monto = df_pagos['Monto_Pago'].sum()
            total_row = pd.DataFrame([{'ID_Pago': '', 'Fecha_Pago': '', 'Diario': '', 'ID_Cliente': '',
                                       'Nombre_Cliente': 'TOTAL', 'Monto_Pago': total_monto}])
            total_row = total_row[df_pagos.columns] # Asegurar orden
            df_pagos = pd.concat([df_pagos, total_row], ignore_index=True)
            if 'Monto_Pago' in df_pagos.columns:
                 df_pagos['Monto_Pago'] = df_pagos['Monto_Pago'].apply(lambda x: float(x) if isinstance(x, Decimal) else x).astype(float)

    except Exception as e:
        print(f"\n--- ERROR AL OBTENER DETALLE DE PAGOS ---")
        import traceback
        traceback.print_exc()
    finally:
        if cursor:
            try: cursor.close(); print("[DB] Cursor de detalle de pagos cerrado.")
            except Exception: pass
    return df_pagos


# --- Función Principal ---

def calcular_comisiones(fecha_inicio, fecha_fin):
    """Calcula las comisiones para la quincena especificada."""
    conexion = None
    cursor = None
    resultados_comision = []

    # IDs para depuración específica
    ID_PAGO_INTERES = 228
    ID_FACTURA_INTERES = 36601

    try:
        print("[DB] Conectando a la base de datos...")
        conexion = conectar()
        if not conexion: raise Exception("No se pudo conectar a la base de datos.")
        cursor_opts = {'dictionary': True, 'buffered': True}
        cursor = conexion.cursor(**cursor_opts)
        print("[OK] Conexión establecida.")

        reglas_comision = obtener_reglas_comision(cursor) # Primera consulta

        print(f"[DB] Obteniendo pagos comisionables entre {fecha_inicio.date()} y {fecha_fin.date()}...")
        query_pagos = """
            SELECT p.id, p.fecha_pago, p.monto AS monto_total_pago, p.id_cliente,
                   p.diario, d.es_comisionable
            FROM pagos p JOIN diarios d ON p.diario = d.nombre
            WHERE p.fecha_pago BETWEEN %s AND %s AND d.es_comisionable = 1
            ORDER BY p.fecha_pago ASC;
        """
        try:
            print("[DB] Verificando conexión antes de obtener pagos...")
            conexion.ping(reconnect=True, attempts=3, delay=1)
        except mysql.connector.Error as err: print(f"Error de conexión antes de obtener pagos: {err}"); raise
        cursor.close(); cursor = conexion.cursor(**cursor_opts)
        print("[DB] Cursor recreado antes de obtener pagos.")
        cursor.execute(query_pagos, (fecha_inicio, fecha_fin))
        pagos_periodo = cursor.fetchall()
        print(f"[OK] {len(pagos_periodo)} pagos comisionables encontrados en el período.")

        if not pagos_periodo: print("No hay pagos comisionables en el período seleccionado."); return pd.DataFrame()

        ids_pagos_periodo = {p['id'] for p in pagos_periodo}
        pagos_dict = {p['id']: {'fecha_pago': p['fecha_pago'], 'monto_total_pago': p['monto_total_pago']} for p in pagos_periodo}

        # --- DEBUG: Verificar si el pago de interés está en el set ---
        if ID_PAGO_INTERES in ids_pagos_periodo:
            print(f"DEBUG: Pago ID {ID_PAGO_INTERES} SÍ está en ids_pagos_periodo.")
        else:
            print(f"DEBUG: Pago ID {ID_PAGO_INTERES} NO está en ids_pagos_periodo. (Verificar fecha o diario comisionable)")
        # --- FIN DEBUG ---


        print("[DB] Obteniendo conciliaciones de pagos...")
        if not ids_pagos_periodo: print("No hay IDs de pago para buscar conciliaciones."); return pd.DataFrame()
        placeholders = ', '.join(['%s'] * len(ids_pagos_periodo))
        query_conciliaciones = f"""
            SELECT pc.id AS id_conciliacion, pc.id_pago, pc.id_factura, pc.monto_aplicado,
                   pc.fecha_aplicacion, f.id_vendedor, f.num_factura, f.id_cliente AS id_cliente_factura
            FROM pago_conciliados pc JOIN facturas f ON pc.id_factura = f.id
            WHERE pc.id_pago IN ({placeholders})
            ORDER BY pc.id_factura, pc.fecha_aplicacion ASC;
        """
        try:
            print("[DB] Verificando conexión antes de obtener conciliaciones...")
            conexion.ping(reconnect=True, attempts=3, delay=1)
        except mysql.connector.Error as err: print(f"Error de conexión antes de obtener conciliaciones: {err}"); raise
        cursor.close(); cursor = conexion.cursor(**cursor_opts)
        print("[DB] Cursor recreado antes de obtener conciliaciones.")
        cursor.execute(query_conciliaciones, tuple(ids_pagos_periodo))
        conciliaciones = cursor.fetchall()
        print(f"[OK] {len(conciliaciones)} conciliaciones encontradas.")

        if not conciliaciones: print("No se encontraron conciliaciones para los pagos del período."); return pd.DataFrame()

        ids_facturas_involucradas = {c['id_factura'] for c in conciliaciones}
        if not ids_facturas_involucradas: print("No hay facturas involucradas."); return pd.DataFrame()

        print("[DB] Obteniendo cuotas de las facturas involucradas...")
        placeholders_facturas = ', '.join(['%s'] * len(ids_facturas_involucradas))
        query_cuotas = f"""
            SELECT id, id_factura, nro_cuota, fecha_vencimiento, monto_cuota, pendiente_cobrar
            FROM cuotas WHERE id_factura IN ({placeholders_facturas})
            ORDER BY id_factura, nro_cuota ASC;
        """
        try:
            print("[DB] Verificando conexión antes de obtener cuotas...")
            conexion.ping(reconnect=True, attempts=3, delay=1)
        except mysql.connector.Error as err: print(f"Error de conexión antes de obtener cuotas: {err}"); raise
        cursor.close(); cursor = conexion.cursor(**cursor_opts)
        print("[DB] Cursor recreado antes de obtener cuotas.")
        cursor.execute(query_cuotas, tuple(ids_facturas_involucradas))
        cuotas_list = cursor.fetchall()
        print(f"[OK] {len(cuotas_list)} cuotas obtenidas.")

        cuotas_por_factura = {}
        for c in cuotas_list:
            id_factura = c['id_factura']
            if id_factura not in cuotas_por_factura: cuotas_por_factura[id_factura] = {}
            c['pendiente_actual'] = Decimal(c['monto_cuota'] or '0.0')
            c['monto_cuota'] = Decimal(c['monto_cuota'] or '0.0')
            if isinstance(c['fecha_vencimiento'], datetime): c['fecha_vencimiento'] = c['fecha_vencimiento'].date()
            elif c['fecha_vencimiento'] is None: print(f"ADVERTENCIA: Cuota ID {c.get('id','N/A')} FacID {c['id_factura']} F Venc NULA."); continue
            cuotas_por_factura[id_factura][c['nro_cuota']] = c

        print("[DB] Obteniendo historial de pagos para simular saldos...")
        query_hist_conciliaciones = f"""
            SELECT pc.id_pago, pc.id_factura, pc.monto_aplicado, p.fecha_pago
            FROM pago_conciliados pc JOIN pagos p ON pc.id_pago = p.id
            WHERE pc.id_factura IN ({placeholders_facturas})
            ORDER BY p.fecha_pago ASC, pc.id_pago ASC, pc.id ASC;
        """
        try:
            print("[DB] Verificando conexión antes de obtener historial...")
            conexion.ping(reconnect=True, attempts=3, delay=1)
        except mysql.connector.Error as err: print(f"Error de conexión antes de obtener historial: {err}"); raise
        cursor.close(); cursor = conexion.cursor(**cursor_opts)
        print("[DB] Cursor recreado antes de obtener historial de conciliaciones.")
        cursor.execute(query_hist_conciliaciones, tuple(ids_facturas_involucradas))
        historial_conciliaciones = cursor.fetchall()
        print(f"[OK] {len(historial_conciliaciones)} registros de historial de conciliación obtenidos.")

        # --- Bucle Principal de Procesamiento ---
        if ID_FACTURA_INTERES in cuotas_por_factura:
            print(f"\nDEBUG: Estado INICIAL Cuotas Factura {ID_FACTURA_INTERES}:")
            for n_cuota, c_data in sorted(cuotas_por_factura[ID_FACTURA_INTERES].items()):
                print(f"  Cuota {n_cuota}: Monto={c_data['monto_cuota']}, PendienteInicial={c_data['pendiente_actual']:.2f} (Desde BD: {c_data['pendiente_cobrar']})")
         
        print("[PROCESS] Procesando pagos y calculando comisiones...")
        for pago_info in historial_conciliaciones:
            id_pago_hist = pago_info['id_pago']
            id_factura_hist = pago_info['id_factura']
            monto_aplicado_hist = Decimal(pago_info['monto_aplicado'] or '0.0')
            fecha_pago_hist = pago_info['fecha_pago']

            # --- INICIO DEBUG ESPECÍFICO ---
            is_target_payment = (id_pago_hist == ID_PAGO_INTERES and id_factura_hist == ID_FACTURA_INTERES)
            if is_target_payment:
                print(f"\nDEBUG: Procesando Registro Historial: PagoID={id_pago_hist}, FacturaID={id_factura_hist}, Fecha={fecha_pago_hist}, MontoAplicadoHist={monto_aplicado_hist:.2f}")
            # --- FIN DEBUG ESPECÍFICO ---

            if isinstance(fecha_pago_hist, datetime): fecha_pago_hist = fecha_pago_hist.date()
            elif fecha_pago_hist is None: continue

            if monto_aplicado_hist <= 0: continue
            if id_factura_hist not in cuotas_por_factura: continue

            cuotas_factura_actual = sorted(cuotas_por_factura[id_factura_hist].values(), key=lambda x: x['nro_cuota'])
            monto_restante_pago = monto_aplicado_hist

            for cuota in cuotas_factura_actual:
                if is_target_payment:
                     print(f"  DEBUG: Evaluando Cuota {cuota['nro_cuota']} (Vence: {cuota['fecha_vencimiento']}) - Pendiente ANTES: {cuota['pendiente_actual']:.2f}")

                if monto_restante_pago <= 0: break
                pendiente_cuota = cuota['pendiente_actual']

                if pendiente_cuota > 0:
                    monto_a_aplicar_a_cuota = min(monto_restante_pago, pendiente_cuota)

                    if is_target_payment:
                        print(f"    DEBUG: Aplicando a esta cuota: {monto_a_aplicar_a_cuota:.2f}")

                    # Verificar si el pago pertenece al período y es comisionable
                    if id_pago_hist in ids_pagos_periodo:
                        if is_target_payment:
                            print(f"      DEBUG: Pago ID {id_pago_hist} ESTÁ en ids_pagos_periodo. Calculando comisión...")

                        conciliacion_actual = next((c for c in conciliaciones if c['id_pago'] == id_pago_hist and c['id_factura'] == id_factura_hist), None)
                        if not conciliacion_actual:
                             print(f"Error interno: No se encontró la conciliación original para pago {id_pago_hist} y factura {id_factura_hist}")
                             continue # Saltar este cálculo si no hay conciliación

                        fecha_vencimiento_cuota = cuota['fecha_vencimiento']
                        dias_vencido = (fecha_pago_hist - fecha_vencimiento_cuota).days
                        porcentaje_comision, desc_rango = buscar_tasa_comision(dias_vencido, reglas_comision)
                        comision_generada = Decimal(0)
                        if porcentaje_comision > 0:
                            comision_generada = (monto_a_aplicar_a_cuota * porcentaje_comision).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                        resultado = {
                            'ID Vendedor': conciliacion_actual['id_vendedor'], 'ID Cliente': conciliacion_actual['id_cliente_factura'],
                            'ID Pago': id_pago_hist, 'Fecha Pago': fecha_pago_hist,
                            'Monto Total Pago': pagos_dict[id_pago_hist]['monto_total_pago'],
                            'ID Factura': id_factura_hist, 'Numero Factura': conciliacion_actual['num_factura'],
                            'Nro Cuota': cuota['nro_cuota'], 'Fecha Vencimiento Cuota': fecha_vencimiento_cuota,
                            'Monto Total Cuota': cuota['monto_cuota'], 'Monto Aplicado a Cuota': monto_a_aplicar_a_cuota,
                            'Dias Vencido al Pago': dias_vencido, 'Rango Comision Aplicado': desc_rango,
                            'Porcentaje Comision': porcentaje_comision, 'Comision Generada': comision_generada,
                            'ID Conciliacion': conciliacion_actual['id_conciliacion']
                        }
                        if is_target_payment:
                            print(f"        DEBUG: Comisión Generada: {resultado['Comision Generada']:.2f} (Porc: {resultado['Porcentaje Comision']}) - Añadiendo a resultados.")
                        resultados_comision.append(resultado) # Añadir a la lista

                    elif is_target_payment:
                         print(f"      DEBUG: Pago ID {id_pago_hist} NO está en ids_pagos_periodo. NO se calcula comisión.")

                    # Actualizar estado en memoria para la simulación
                    cuota['pendiente_actual'] -= monto_a_aplicar_a_cuota
                    monto_restante_pago -= monto_a_aplicar_a_cuota

                    if is_target_payment:
                         print(f"    DEBUG: Pendiente DESPUÉS: {cuota['pendiente_actual']:.2f} | Monto Restante Pago: {monto_restante_pago:.2f}")
                elif is_target_payment and pendiente_cuota <= 0:
                     print(f"  DEBUG: Evaluando Cuota {cuota['nro_cuota']} - Pendiente ANTES era CERO o negativo. Saltando aplicación.")
        # --- Fin Bucle Principal ---
        print(f"[OK] Procesamiento de pagos completado. {len(resultados_comision)} registros de comisión generados.")

        if resultados_comision:
            df_resultados = pd.DataFrame(resultados_comision)

            cols_to_float = ['Monto Aplicado a Cuota', 'Comision Generada', 'Monto Total Pago', 'Monto Total Cuota', 'Porcentaje Comision']
            for col in cols_to_float:
                 if col in df_resultados.columns:
                    df_resultados[col] = df_resultados[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x).astype(float)

            ids_vendedores = df_resultados['ID Vendedor'].dropna().unique()
            ids_vendedores = [int(vid) for vid in ids_vendedores if str(vid).isdigit()]
            ids_clientes = df_resultados['ID Cliente'].dropna().unique()
            ids_clientes = [int(cid) for cid in ids_clientes if str(cid).isdigit()]

            # --- Obtener Nombres Vendedores ---
            mapa_vendedores = {}
            if ids_vendedores:
                print("[DB] Obteniendo nombres de vendedores...")
                placeholders_vendedores = ', '.join(['%s'] * len(ids_vendedores))
                query_vendedores = f"SELECT idVendedores, nombre FROM vendedores WHERE idVendedores IN ({placeholders_vendedores})"
                try:
                    print("[DB] Verificando conexión antes de obtener vendedores...")
                    conexion.ping(reconnect=True, attempts=3, delay=1)
                except mysql.connector.Error as err: print(f"Error de conexión antes de obtener vendedores: {err}")
                cursor.close(); cursor = conexion.cursor(**cursor_opts)
                print("[DB] Cursor recreado antes de obtener vendedores.")
                try:
                    cursor.execute(query_vendedores, tuple(ids_vendedores))
                    vendedores_data = cursor.fetchall()
                    mapa_vendedores = {v['idVendedores']: v['nombre'] for v in vendedores_data}
                    print("[OK] Nombres de vendedores obtenidos.")
                except Exception as db_err:
                    print(f"--- ERROR AL OBTENER VENDEDORES ---"); print(f"Consulta: {getattr(cursor, 'statement', 'N/A')}"); print(f"Error: {db_err}"); pass
            df_resultados['Nombre Vendedor'] = df_resultados['ID Vendedor'].map(mapa_vendedores)

            # --- Obtener Nombres Clientes ---
            mapa_clientes = {}
            if ids_clientes:
                print("[DB] Obteniendo nombres de clientes...")
                placeholders_clientes = ', '.join(['%s'] * len(ids_clientes))
                query_clientes = f"SELECT id, nombre FROM clientes WHERE id IN ({placeholders_clientes})"
                try:
                    print("[DB] Verificando conexión antes de obtener clientes...")
                    conexion.ping(reconnect=True, attempts=3, delay=1)
                except mysql.connector.Error as err: print(f"Error de conexión antes de obtener clientes: {err}")
                cursor.close(); cursor = conexion.cursor(**cursor_opts)
                print("[DB] Cursor recreado antes de obtener clientes.")
                try:
                    cursor.execute(query_clientes, tuple(ids_clientes))
                    clientes_data = cursor.fetchall()
                    mapa_clientes = {c['id']: c['nombre'] for c in clientes_data}
                    print("[OK] Nombres de clientes obtenidos.")
                except Exception as db_err:
                    print(f"--- ERROR AL OBTENER CLIENTES ---"); print(f"Consulta: {getattr(cursor, 'statement', 'N/A')}"); print(f"Error: {db_err}"); pass
            df_resultados['Nombre Cliente'] = df_resultados['ID Cliente'].map(mapa_clientes)

            # --- Finalizar DataFrame ---
            columnas_finales = [
                'ID Vendedor', 'Nombre Vendedor', 'ID Cliente', 'Nombre Cliente', 'ID Pago', 'Fecha Pago',
                'Numero Factura', 'Nro Cuota', 'Fecha Vencimiento Cuota', 'Monto Aplicado a Cuota',
                'Dias Vencido al Pago', 'Rango Comision Aplicado', 'Porcentaje Comision', 'Comision Generada',
                'Monto Total Pago', 'Monto Total Cuota', 'ID Conciliacion', 'ID Factura'
            ]
            columnas_existentes = [col for col in columnas_finales if col in df_resultados.columns]
            df_resultados = df_resultados[columnas_existentes]
            if 'Fecha Pago' in df_resultados.columns: df_resultados['Fecha Pago'] = pd.to_datetime(df_resultados['Fecha Pago']).dt.date
            if 'Fecha Vencimiento Cuota' in df_resultados.columns: df_resultados['Fecha Vencimiento Cuota'] = pd.to_datetime(df_resultados['Fecha Vencimiento Cuota']).dt.date
            return df_resultados
        else:
            print("No se generaron comisiones en el período."); return pd.DataFrame()

    except Exception as e:
        print(f"\n--- ERROR DURANTE EL PROCESO ---"); import traceback; traceback.print_exc(); return pd.DataFrame()

    finally:
        if cursor:
             try: cursor.close(); print("[DB] Cursor final cerrado.")
             except Exception as cur_err: print(f"Advertencia: Error al intentar cerrar el cursor final: {cur_err}")
        if conexion and conexion.is_connected():
            conexion.close(); print("[DB] Conexión cerrada.")

# --- Ejecución del Script ---
if __name__ == "__main__":
    print("--- INICIO DEL SCRIPT DE CÁLCULO DE COMISIONES ---")
    print(f"Procesando quincena: {FECHA_INICIO_QUINCENA.strftime('%d/%m/%Y')} - {FECHA_FIN_QUINCENA.strftime('%d/%m/%Y')}")

    # 1. Calcular Comisiones
    df_comisiones = calcular_comisiones(FECHA_INICIO_QUINCENA, FECHA_FIN_QUINCENA)

    # 2. Obtener Detalle de Pagos
    df_pagos_detalle = pd.DataFrame()
    conexion_pagos = None
    try:
        print("\n[DB] Conectando para obtener detalle de pagos...")
        conexion_pagos = conectar()
        if conexion_pagos and conexion_pagos.is_connected():
            print("[OK] Conexión para detalle de pagos establecida.")
            df_pagos_detalle = obtener_detalle_pagos_periodo(FECHA_INICIO_QUINCENA, FECHA_FIN_QUINCENA, conexion_pagos)
        else: print("[ERROR] No se pudo conectar para obtener detalle de pagos.")
    except Exception as e: print(f"[ERROR] Falló la obtención de detalle de pagos: {e}")
    finally:
        if conexion_pagos and conexion_pagos.is_connected(): conexion_pagos.close(); print("[DB] Conexión para detalle de pagos cerrada.")

    # 3. Escribir en Excel
    if not df_comisiones.empty or not df_pagos_detalle.empty:
        try:
            print(f"\n[OUTPUT] Guardando resultados en '{ARCHIVO_SALIDA_EXCEL}'...")
            with pd.ExcelWriter(ARCHIVO_SALIDA_EXCEL, engine='openpyxl', date_format='YYYY-MM-DD', datetime_format='YYYY-MM-DD') as writer:
                if not df_comisiones.empty:
                    df_comisiones.to_excel(writer, index=False, sheet_name='Comisiones'); print("[OK] Hoja 'Comisiones' preparada.")
                else: print("[INFO] No hay datos de comisiones para escribir.")
                if not df_pagos_detalle.empty:
                    df_pagos_detalle.to_excel(writer, index=False, sheet_name='Pagos Periodo'); print("[OK] Hoja 'Pagos Periodo' preparada.")
                else: print("[INFO] No hay datos de detalle de pagos para escribir.")
            print(f"[OK] Reporte Excel '{ARCHIVO_SALIDA_EXCEL}' guardado exitosamente.")
        except Exception as e: print(f"\n--- ERROR AL GUARDAR EXCEL ---"); print(f"Error: {e}"); print("Los datos calculados no se pudieron guardar.")
    else: print("\nNo se generaron datos de comisión ni detalles de pago para guardar.")

    print("\n--- FIN DEL SCRIPT ---")