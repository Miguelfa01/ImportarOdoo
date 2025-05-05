# -*- coding: utf-8 -*-
# Guardar como: reporte_cuotas_pendientes_fecha.py

import sys
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
from conexion_mysql import conectar

print("\n--- Script: Reporte de Cuotas Pendientes a Fecha de Corte ---")

# --- Configuración ---
# Pequeña tolerancia para considerar una cuota como pagada
TOLERANCIA_PENDIENTE = Decimal('0.01')

# --- Funciones Auxiliares ---
def obtener_fecha_corte():
    """Solicita y valida la fecha de corte al usuario."""
    while True:
        fecha_str = input("Ingrese la fecha de corte (YYYY-MM-DD): ")
        try:
            fecha_dt = datetime.strptime(fecha_str, "%Y-%m-%d").date()
            return fecha_dt
        except ValueError:
            print("Formato de fecha inválido. Use YYYY-MM-DD.")

def formatear_decimal(valor):
    """Formatea un Decimal a string con 2 decimales."""
    if valor is None:
        return "0.00"
    # Asegurarse que es Decimal para cuantificar
    valor_decimal = Decimal(valor) if not isinstance(valor, Decimal) else valor
    return str(valor_decimal.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


# --- Variables ---
conexion = None
cursor = None
fecha_corte = None
resultados_pendientes = []

# --- Contadores / Totales ---
total_monto_cuota_pendiente = Decimal('0.0')
total_monto_pagado_fecha_pendiente = Decimal('0.0')
total_monto_pendiente_fecha = Decimal('0.0')
num_cuotas_pendientes = 0

try:
    # 1. OBTENER FECHA DE CORTE
    fecha_corte = obtener_fecha_corte()
    print(f"[INFO] Generando reporte para cuotas pendientes hasta el: {fecha_corte}")

    # 2. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion:
        raise Exception("No se pudo conectar a la base de datos.")
    # Usar dictionary=True es conveniente aquí
    cursor = conexion.cursor(dictionary=True)
    print("[OK] Conexión establecida.")

    # 3. OBTENER PAGOS CONCILIADOS HASTA LA FECHA DE CORTE
    print(f"[DB] Obteniendo pagos conciliados hasta {fecha_corte}...")
    sql_pagos = """
        SELECT
            pc.id_factura,
            SUM(pc.monto_aplicado) AS total_pagado_fecha_corte
        FROM
            pago_conciliados pc
        WHERE
            pc.fecha_aplicacion <= %(fecha_corte)s
        GROUP BY
            pc.id_factura;
    """
    cursor.execute(sql_pagos, {'fecha_corte': fecha_corte})
    pagos_por_factura = {
        p['id_factura']: Decimal(p['total_pagado_fecha_corte'] or 0)
        for p in cursor.fetchall()
    }
    print(f"[INFO] {len(pagos_por_factura)} facturas con pagos encontrados hasta la fecha.")

    # 4. OBTENER DEFINICIONES DE CUOTAS Y DATOS RELACIONADOS
    print("[DB] Obteniendo definiciones de cuotas y datos relacionados...")
    # Optimizamos para traer todo en una consulta si es posible
    sql_cuotas_info = """
        SELECT
            c.id AS id_cuota,
            c.id_factura,
            c.nro_cuota,
            c.monto_cuota,
            c.fecha_vencimiento AS fecha_vencimiento_cuota,
            f.num_factura,
            f.fecha_factura,
            f.id_cliente,
            f.id_vendedor,
            cli.nombre AS nombre_cliente,
            ven.nombre AS nombre_vendedor
        FROM
            cuotas c
        INNER JOIN facturas f ON c.id_factura = f.id
        LEFT JOIN clientes cli ON f.id_cliente = cli.id
        LEFT JOIN vendedores ven ON f.id_vendedor = ven.idVendedores
        ORDER BY
            c.id_factura, c.nro_cuota;
    """
    cursor.execute(sql_cuotas_info)
    todas_las_cuotas = cursor.fetchall()
    print(f"[INFO] {len(todas_las_cuotas)} registros de cuotas encontrados en total.")

    if not todas_las_cuotas:
        print("[INFO] No se encontraron cuotas definidas en la base de datos.")
        sys.exit(0)

    # 5. PROCESAR CUOTAS Y CALCULAR PENDIENTES A LA FECHA DE CORTE
    print("[INFO] Calculando saldos de cuotas a la fecha de corte...")
    current_factura_id = None
    monto_pagado_factura_a_distribuir = Decimal('0.0')
    cuotas_factura_actual = []

    for cuota in todas_las_cuotas:
        factura_id = cuota['id_factura']

        # Si cambiamos de factura, procesamos la anterior y reiniciamos
        if factura_id != current_factura_id and current_factura_id is not None:
            # Distribuir pago acumulado en las cuotas de la factura ANTERIOR
            restante_a_aplicar = monto_pagado_factura_a_distribuir
            for c_proc in cuotas_factura_actual:
                monto_cuota_actual = Decimal(c_proc['monto_cuota'] or 0)
                # Asegurarse que monto_cuota_actual no sea negativo
                monto_cuota_actual = max(Decimal('0.0'), monto_cuota_actual)

                pagado_esta_cuota = max(Decimal('0.0'), min(monto_cuota_actual, restante_a_aplicar))
                pendiente_esta_cuota = max(Decimal('0.0'), monto_cuota_actual - pagado_esta_cuota)

                # Aplicar tolerancia
                if pendiente_esta_cuota <= TOLERANCIA_PENDIENTE:
                    pendiente_esta_cuota = Decimal('0.0')
                    # Ajustar pagado si redondeo lo hizo "pagado"
                    pagado_esta_cuota = monto_cuota_actual

                restante_a_aplicar = max(Decimal('0.0'), restante_a_aplicar - pagado_esta_cuota)

                # Si la cuota queda pendiente, añadirla al resultado
                if pendiente_esta_cuota > Decimal('0.0'):
                    resultados_pendientes.append({
                        "Vendedor": c_proc.get('nombre_vendedor', 'N/A'),
                        "Cliente": c_proc.get('nombre_cliente', 'N/A'),
                        "Factura": c_proc.get('num_factura', 'N/A'),
                        "Fecha Factura": c_proc.get('fecha_factura'),
                        "Nro Cuota": c_proc.get('nro_cuota'),
                        "Fecha Vencimiento Cuota": c_proc.get('fecha_vencimiento_cuota'),
                        "Monto Cuota": monto_cuota_actual,
                        "Monto Pagado (a fecha corte)": pagado_esta_cuota,
                        "Monto Pendiente (a fecha corte)": pendiente_esta_cuota
                    })
                    # Acumular totales
                    total_monto_cuota_pendiente += monto_cuota_actual
                    total_monto_pagado_fecha_pendiente += pagado_esta_cuota
                    total_monto_pendiente_fecha += pendiente_esta_cuota
                    num_cuotas_pendientes += 1

            # Reiniciar para la nueva factura
            cuotas_factura_actual = []

        # Actualizar factura actual y obtener su pago acumulado
        if factura_id != current_factura_id:
             current_factura_id = factura_id
             monto_pagado_factura_a_distribuir = pagos_por_factura.get(current_factura_id, Decimal('0.0'))

        # Añadir cuota actual a la lista de la factura
        cuotas_factura_actual.append(cuota)

    # Procesar la ÚLTIMA factura después de salir del bucle
    if current_factura_id is not None and cuotas_factura_actual:
        restante_a_aplicar = monto_pagado_factura_a_distribuir
        for c_proc in cuotas_factura_actual:
            monto_cuota_actual = Decimal(c_proc['monto_cuota'] or 0)
            monto_cuota_actual = max(Decimal('0.0'), monto_cuota_actual) # Asegurar no negativo
            pagado_esta_cuota = max(Decimal('0.0'), min(monto_cuota_actual, restante_a_aplicar))
            pendiente_esta_cuota = max(Decimal('0.0'), monto_cuota_actual - pagado_esta_cuota)

            if pendiente_esta_cuota <= TOLERANCIA_PENDIENTE:
                 pendiente_esta_cuota = Decimal('0.0')
                 pagado_esta_cuota = monto_cuota_actual # Ajustar pagado

            restante_a_aplicar = max(Decimal('0.0'), restante_a_aplicar - pagado_esta_cuota)

            if pendiente_esta_cuota > Decimal('0.0'):
                resultados_pendientes.append({
                    "Vendedor": c_proc.get('nombre_vendedor', 'N/A'),
                    "Cliente": c_proc.get('nombre_cliente', 'N/A'),
                    "Factura": c_proc.get('num_factura', 'N/A'),
                    "Fecha Factura": c_proc.get('fecha_factura'),
                    "Nro Cuota": c_proc.get('nro_cuota'),
                    "Fecha Vencimiento Cuota": c_proc.get('fecha_vencimiento_cuota'),
                    "Monto Cuota": monto_cuota_actual,
                    "Monto Pagado (a fecha corte)": pagado_esta_cuota,
                    "Monto Pendiente (a fecha corte)": pendiente_esta_cuota
                })
                # Acumular totales
                total_monto_cuota_pendiente += monto_cuota_actual
                total_monto_pagado_fecha_pendiente += pagado_esta_cuota
                total_monto_pendiente_fecha += pendiente_esta_cuota
                num_cuotas_pendientes += 1

    print("[INFO] Cálculo de saldos completado.")

    # 6. MOSTRAR RESULTADOS
    if not resultados_pendientes:
        print(f"\n[INFO] No se encontraron cuotas pendientes de pago a la fecha de corte: {fecha_corte}")
    else:
        print(f"\n--- Reporte de Cuotas Pendientes al {fecha_corte} ---")
        df_resultados = pd.DataFrame(resultados_pendientes)

        # Formatear columnas de fecha y decimales para visualización
        if 'Fecha Factura' in df_resultados.columns:
             df_resultados['Fecha Factura'] = pd.to_datetime(df_resultados['Fecha Factura']).dt.strftime('%Y-%m-%d')
        if 'Fecha Vencimiento Cuota' in df_resultados.columns:
             df_resultados['Fecha Vencimiento Cuota'] = pd.to_datetime(df_resultados['Fecha Vencimiento Cuota']).dt.strftime('%Y-%m-%d')

        # Aplicar formato a columnas Decimal usando la función auxiliar
        cols_decimal = ["Monto Cuota", "Monto Pagado (a fecha corte)", "Monto Pendiente (a fecha corte)"]
        for col in cols_decimal:
            if col in df_resultados.columns:
                df_resultados[col] = df_resultados[col].apply(formatear_decimal)


        # Mostrar DataFrame sin el índice
        print(df_resultados.to_string(index=False))

        # Mostrar Totales
        print("\n--- Totales Generales (Cuotas Pendientes Listadas) ---")
        print(f"Número de Cuotas Pendientes : {num_cuotas_pendientes}")
        print(f"Suma Monto Cuota            : {formatear_decimal(total_monto_cuota_pendiente)}")
        print(f"Suma Monto Pagado (a fecha) : {formatear_decimal(total_monto_pagado_fecha_pendiente)}")
        print(f"Suma Monto Pendiente(a fecha): {formatear_decimal(total_monto_pendiente_fecha)}")
        print("----------------------------------------------------")


except Exception as e:
    print(f"\n[ERROR] Ocurrió un error inesperado: {e}")
    import traceback
    traceback.print_exc() # Imprime más detalles del error
    sys.exit(1)

finally:
    # 7. CERRAR RECURSOS
    if cursor:
        cursor.close()
        print("[DB] Cursor cerrado.")
    if conexion and conexion.is_connected():
        conexion.close()
        print("[DB] Conexión cerrada.")

print("\n[OK] Script finalizado.")
sys.exit(0)