# -*- coding: utf-8 -*-
# Guardar como: reporte_cuotas_html_fecha.py

import sys
import os
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
from conexion_mysql import conectar
import webbrowser # Para abrir el HTML automáticamente

print("\n--- Script: Reporte HTML Interactivo de Cuotas Pendientes a Fecha de Corte ---")

# --- Configuración ---
TOLERANCIA_PENDIENTE = Decimal('0.01')
NOMBRE_ARCHIVO_HTML_BASE = "reporte_cuotas_pendientes"

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
    """Formatea un Decimal a string con 2 decimales para HTML."""
    if valor is None:
        return "0.00"
    valor_decimal = Decimal(valor) if not isinstance(valor, Decimal) else valor
    return str(valor_decimal.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def formatear_fecha(fecha_obj):
    """Formatea un objeto date/datetime a YYYY-MM-DD, o devuelve vacío."""
    if isinstance(fecha_obj, (date, datetime)):
        return fecha_obj.strftime('%Y-%m-%d')
    return "" # O 'N/A' si prefieres

def generar_html_reporte(datos_pendientes, totales, fecha_corte, filename):
    """Genera el archivo HTML con la tabla interactiva."""
    print(f"[INFO] Generando archivo HTML: {filename}")

    # Convertir datos a DataFrame para facilitar la manipulación si es necesario
    df = pd.DataFrame(datos_pendientes)

    # --- Inicio del HTML ---
    html = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte Cuotas Pendientes al {formatear_fecha(fecha_corte)}</title>
    <!-- DataTables CSS -->
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <!-- Bootstrap CSS (Opcional, para mejor estilo) -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
    <style>
        body {{ padding: 20px; }}
        .totals-section {{ margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; background-color: #f9f9f9; border-radius: 5px; }}
        /* Ajuste para números en la tabla */
        td.numero {{ text-align: right; }}
        th {{ text-align: center; }}
    </style>
</head>
<body>
    <div class="container-fluid">
        <h1 class="mb-4">Reporte de Cuotas Pendientes al {formatear_fecha(fecha_corte)}</h1>

        <!-- Sección de Totales -->
        <div class="totals-section">
            <h4>Totales Generales (Cuotas Pendientes Listadas)</h4>
            <p><strong>Número de Cuotas Pendientes:</strong> {totales['num_cuotas']}</p>
            <p><strong>Suma Monto Cuota:</strong> {formatear_decimal(totales['total_monto_cuota'])}</p>
            <p><strong>Suma Monto Pagado (a fecha):</strong> {formatear_decimal(totales['total_monto_pagado'])}</p>
            <p><strong>Suma Monto Pendiente (a fecha):</strong> {formatear_decimal(totales['total_monto_pendiente'])}</p>
        </div>

        <!-- Tabla de Datos -->
        <table id="reporteTabla" class="table table-striped table-bordered table-hover" style="width:100%">
            <thead>
                <tr>
                    <th>Vendedor</th>
                    <th>Cliente</th>
                    <th>Factura</th>
                    <th>Fecha Factura</th>
                    <th>Nro Cuota</th>
                    <th>Fecha Venc. Cuota</th>
                    <th>Monto Cuota</th>
                    <th>Monto Pagado (a fecha)</th>
                    <th>Monto Pendiente (a fecha)</th>
                </tr>
            </thead>
            <tbody>
    """

    # --- Filas de la tabla ---
    if not df.empty:
        for index, row in df.iterrows():
            html += f"""
                <tr>
                    <td>{row.get('Vendedor', '')}</td>
                    <td>{row.get('Cliente', '')}</td>
                    <td>{row.get('Factura', '')}</td>
                    <td>{formatear_fecha(row.get('Fecha Factura'))}</td>
                    <td class="numero">{row.get('Nro Cuota', '')}</td>
                    <td>{formatear_fecha(row.get('Fecha Vencimiento Cuota'))}</td>
                    <td class="numero">{formatear_decimal(row.get('Monto Cuota'))}</td>
                    <td class="numero">{formatear_decimal(row.get('Monto Pagado (a fecha corte)'))}</td>
                    <td class="numero"><strong>{formatear_decimal(row.get('Monto Pendiente (a fecha corte)'))}</strong></td>
                </tr>
            """
    else:
         html += '<tr><td colspan="9" style="text-align:center;">No se encontraron cuotas pendientes para esta fecha.</td></tr>'


    # --- Fin de la tabla y scripts JS ---
    html += """
            </tbody>
        </table>
    </div>

    <!-- jQuery -->
    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <!-- DataTables JS -->
    <script type="text/javascript" src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <!-- Bootstrap JS (Opcional, si usas componentes JS de Bootstrap) -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>

    <!-- Inicialización de DataTables -->
    <script>
        $(document).ready(function() {
            $('#reporteTabla').DataTable({
                "language": {
                    "url": "//cdn.datatables.net/plug-ins/1.13.6/i18n/es-ES.json" // Traducción al español
                },
                "pageLength": 25, // Mostrar 25 filas por página inicialmente
                 "lengthMenu": [ [10, 25, 50, 100, -1], [10, 25, 50, 100, "Todos"] ], // Opciones de longitud
                 "order": [[ 1, "asc" ], [ 5, "asc" ]] // Ordenar por Cliente y luego Fecha Vencimiento por defecto
            });
        });
    </script>
</body>
</html>
    """

    # --- Guardar archivo HTML ---
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"[OK] Reporte HTML guardado como: {filename}")
        return True
    except IOError as e:
        print(f"[ERROR] No se pudo guardar el archivo HTML: {e}")
        return False

# --- Flujo Principal ---
conexion = None
cursor = None
fecha_corte = None
resultados_pendientes = []
totales = {
    'num_cuotas': 0,
    'total_monto_cuota': Decimal('0.0'),
    'total_monto_pagado': Decimal('0.0'),
    'total_monto_pendiente': Decimal('0.0')
}

try:
    # 1. OBTENER FECHA DE CORTE
    fecha_corte = obtener_fecha_corte()
    print(f"[INFO] Generando reporte para cuotas pendientes hasta el: {fecha_corte}")

    # 2. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion:
        raise Exception("No se pudo conectar a la base de datos.")
    cursor = conexion.cursor(dictionary=True)
    print("[OK] Conexión establecida.")

    # 3. OBTENER PAGOS CONCILIADOS HASTA LA FECHA DE CORTE
    print(f"[DB] Obteniendo pagos conciliados hasta {fecha_corte}...")
    sql_pagos = """
        SELECT pc.id_factura, SUM(pc.monto_aplicado) AS total_pagado_fecha_corte
        FROM pago_conciliados pc
        WHERE pc.fecha_aplicacion <= %(fecha_corte)s
        GROUP BY pc.id_factura;
    """
    cursor.execute(sql_pagos, {'fecha_corte': fecha_corte})
    pagos_por_factura = {
        p['id_factura']: Decimal(p['total_pagado_fecha_corte'] or 0)
        for p in cursor.fetchall()
    }
    print(f"[INFO] {len(pagos_por_factura)} facturas con pagos encontrados hasta la fecha.")

    # 4. OBTENER DEFINICIONES DE CUOTAS Y DATOS RELACIONADOS
    print("[DB] Obteniendo definiciones de cuotas y datos relacionados...")
    sql_cuotas_info = """
        SELECT
            c.id AS id_cuota, c.id_factura, c.nro_cuota, c.monto_cuota,
            c.fecha_vencimiento AS fecha_vencimiento_cuota,
            f.num_factura, f.fecha_factura, f.id_cliente, f.id_vendedor,
            cli.nombre AS nombre_cliente, ven.nombre AS nombre_vendedor
        FROM cuotas c
        INNER JOIN facturas f ON c.id_factura = f.id
        LEFT JOIN clientes cli ON f.id_cliente = cli.id
        LEFT JOIN vendedores ven ON f.id_vendedor = ven.idVendedores
        ORDER BY c.id_factura, c.nro_cuota;
    """
    cursor.execute(sql_cuotas_info)
    todas_las_cuotas = cursor.fetchall()
    print(f"[INFO] {len(todas_las_cuotas)} registros de cuotas encontrados en total.")

    if not todas_las_cuotas:
        print("[INFO] No se encontraron cuotas definidas en la base de datos.")
        # Generar HTML vacío igualmente
        nombre_archivo = f"{NOMBRE_ARCHIVO_HTML_BASE}_{fecha_corte.strftime('%Y%m%d')}.html"
        if generar_html_reporte([], totales, fecha_corte, nombre_archivo):
             try: webbrowser.open(f'file://{os.path.realpath(nombre_archivo)}')
             except Exception as e_open: print(f"[WARN] No se pudo abrir el navegador automáticamente: {e_open}")
        sys.exit(0)


    # 5. PROCESAR CUOTAS Y CALCULAR PENDIENTES A LA FECHA DE CORTE
    print("[INFO] Calculando saldos de cuotas a la fecha de corte...")
    # (Misma lógica de procesamiento que el script anterior)
    current_factura_id = None
    monto_pagado_factura_a_distribuir = Decimal('0.0')
    cuotas_factura_actual = []

    for cuota in todas_las_cuotas:
        factura_id = cuota['id_factura']
        if factura_id != current_factura_id and current_factura_id is not None:
            restante_a_aplicar = monto_pagado_factura_a_distribuir
            for c_proc in cuotas_factura_actual:
                monto_cuota_actual = Decimal(c_proc['monto_cuota'] or 0)
                monto_cuota_actual = max(Decimal('0.0'), monto_cuota_actual)
                pagado_esta_cuota = max(Decimal('0.0'), min(monto_cuota_actual, restante_a_aplicar))
                pendiente_esta_cuota = max(Decimal('0.0'), monto_cuota_actual - pagado_esta_cuota)
                if pendiente_esta_cuota <= TOLERANCIA_PENDIENTE:
                    pendiente_esta_cuota = Decimal('0.0')
                    pagado_esta_cuota = monto_cuota_actual
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
                    totales['num_cuotas'] += 1
                    totales['total_monto_cuota'] += monto_cuota_actual
                    totales['total_monto_pagado'] += pagado_esta_cuota
                    totales['total_monto_pendiente'] += pendiente_esta_cuota
            cuotas_factura_actual = []
        if factura_id != current_factura_id:
             current_factura_id = factura_id
             monto_pagado_factura_a_distribuir = pagos_por_factura.get(current_factura_id, Decimal('0.0'))
        cuotas_factura_actual.append(cuota)

    # Procesar la ÚLTIMA factura
    if current_factura_id is not None and cuotas_factura_actual:
        restante_a_aplicar = monto_pagado_factura_a_distribuir
        for c_proc in cuotas_factura_actual:
            monto_cuota_actual = Decimal(c_proc['monto_cuota'] or 0)
            monto_cuota_actual = max(Decimal('0.0'), monto_cuota_actual)
            pagado_esta_cuota = max(Decimal('0.0'), min(monto_cuota_actual, restante_a_aplicar))
            pendiente_esta_cuota = max(Decimal('0.0'), monto_cuota_actual - pagado_esta_cuota)
            if pendiente_esta_cuota <= TOLERANCIA_PENDIENTE:
                 pendiente_esta_cuota = Decimal('0.0')
                 pagado_esta_cuota = monto_cuota_actual
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
                totales['num_cuotas'] += 1
                totales['total_monto_cuota'] += monto_cuota_actual
                totales['total_monto_pagado'] += pagado_esta_cuota
                totales['total_monto_pendiente'] += pendiente_esta_cuota

    print("[INFO] Cálculo de saldos completado.")

    # 6. GENERAR Y ABRIR HTML
    nombre_archivo = f"{NOMBRE_ARCHIVO_HTML_BASE}_{fecha_corte.strftime('%Y%m%d')}.html"
    if generar_html_reporte(resultados_pendientes, totales, fecha_corte, nombre_archivo):
        # Intentar abrir el archivo en el navegador por defecto
        try:
            webbrowser.open(f'file://{os.path.realpath(nombre_archivo)}')
            print("[INFO] Intentando abrir el reporte en el navegador...")
        except Exception as e_open:
            print(f"[WARN] No se pudo abrir el navegador automáticamente: {e_open}")
            print(f"      Puedes abrir el archivo manualmente: {os.path.realpath(nombre_archivo)}")


except Exception as e:
    print(f"\n[ERROR] Ocurrió un error inesperado: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    # 7. CERRAR RECURSOS
    if cursor: cursor.close(); print("[DB] Cursor cerrado.")
    if conexion and conexion.is_connected(): conexion.close(); print("[DB] Conexión cerrada.")

print("\n[OK] Script finalizado.")
sys.exit(0)