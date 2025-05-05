# -*- coding: utf-8 -*-
# Guardar como: actualizar_saldos_y_cuotas.py

from conexion_mysql import conectar
import sys
import subprocess
import os
from decimal import Decimal # Para la tolerancia

print("\n--- Script: actualizar_saldos_y_cuotas.py ---")
print("Actualizando saldos de facturas y regenerando cuotas...")

# --- Configuración ---
NOMBRE_SCRIPT_GENERAR_CUOTAS = "generar_cuotas.py" # <-- Nombre exacto del script a llamar
# Tolerancia para considerar una factura como pagada en el UPDATE
TOLERANCIA_SALDO_UPDATE = Decimal('0.01')

# --- Variables ---
conexion = None
cursor = None
proceso_exitoso_actualizacion = False
proceso_exitoso_cuotas = False

# --- Consulta SQL para actualizar Facturas ---
sql_update_facturas = """
UPDATE facturas f
LEFT JOIN
    (SELECT
         pc.id_factura,
         SUM(pc.monto_aplicado) AS total_pagado_calculado
     FROM
         pago_conciliados pc -- Asegúrate que el nombre de tabla sea correcto
     GROUP BY
         pc.id_factura
    ) AS PagosSumados ON f.id = PagosSumados.id_factura
SET
    f.total_cobrado = COALESCE(PagosSumados.total_pagado_calculado, 0.0),
    f.pendiente_cobrar = COALESCE(f.total_factura, 0.0) - COALESCE(PagosSumados.total_pagado_calculado, 0.0),
    f.estado_pago = CASE
                        WHEN (COALESCE(f.total_factura, 0.0) - COALESCE(PagosSumados.total_pagado_calculado, 0.0)) <= %(tolerancia)s THEN 'Pagada'
                        WHEN COALESCE(PagosSumados.total_pagado_calculado, 0.0) > 0.0 THEN 'Parcial'
                        ELSE 'Pendiente'
                    END;
"""

try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion:
        raise Exception("No se pudo conectar a la base de datos.")
    cursor = conexion.cursor() # No necesitamos dictionary=True para UPDATE
    print("[OK] Conexión establecida.")

    # 2. EJECUTAR ACTUALIZACIÓN DE FACTURAS
    print("[DB] Actualizando total_cobrado, pendiente_cobrar y estado_pago en la tabla 'facturas'...")
    # Pasamos la tolerancia como parámetro
    cursor.execute(sql_update_facturas, {'tolerancia': TOLERANCIA_SALDO_UPDATE})
    num_filas_afectadas = cursor.rowcount # MySQL devuelve filas 'matched' no necesariamente 'changed'
    print(f"[OK] Consulta UPDATE ejecutada. Filas encontradas/afectadas: {num_filas_afectadas}")

    # 3. COMMIT de la actualización de facturas
    print("[DB] Realizando COMMIT de la actualización de facturas...")
    conexion.commit()
    print("(+) Commit realizado.")
    proceso_exitoso_actualizacion = True

except Exception as e_update:
    print(f"\n[ERROR] Error durante la actualización de saldos de facturas: {e_update}")
    proceso_exitoso_actualizacion = False
    if conexion:
        try:
            print("[DB] Intentando realizar ROLLBACK...")
            conexion.rollback()
            print("(-) Rollback realizado.")
        except Exception as rb_err:
            print(f"[WARN] Error durante el rollback: {rb_err}")

finally:
    # Cerrar cursor y conexión temporalmente antes de llamar al subprocess
    if cursor: cursor.close(); print("[DB] Cursor cerrado.")
    if conexion and conexion.is_connected(): conexion.close(); print("[DB] Conexión cerrada.")


# 4. LLAMAR AL SCRIPT DE GENERAR CUOTAS (si la actualización fue exitosa)
if proceso_exitoso_actualizacion:
    print("\n----------------------------------------------------")
    print(f">> Ejecutando script de generación de cuotas: '{NOMBRE_SCRIPT_GENERAR_CUOTAS}'...")
    print("----------------------------------------------------")
    script_full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), NOMBRE_SCRIPT_GENERAR_CUOTAS)

    if not os.path.exists(script_full_path):
        print(f"[ERROR] FATAL: No se encontró el script '{NOMBRE_SCRIPT_GENERAR_CUOTAS}' en {script_full_path}.")
        proceso_exitoso_cuotas = False
    else:
        try:
            child_env = os.environ.copy()
            child_env['PYTHONIOENCODING'] = 'utf-8'
            resultado_subprocess = subprocess.run(
                [sys.executable, script_full_path],
                check=True, # Lanzará excepción si generar_cuotas falla
                capture_output=True, text=True,
                encoding='utf-8', errors='replace', env=child_env
            )
            print(f"\n--- Salida del Script '{NOMBRE_SCRIPT_GENERAR_CUOTAS}' ---")
            print(resultado_subprocess.stdout)
            if resultado_subprocess.stderr:
                 print(f"\n--- Errores Impresos por '{NOMBRE_SCRIPT_GENERAR_CUOTAS}' ---")
                 print(resultado_subprocess.stderr)
            print(f"\n[OK] Script '{NOMBRE_SCRIPT_GENERAR_CUOTAS}' ejecutado exitosamente.")
            proceso_exitoso_cuotas = True

        except subprocess.CalledProcessError as e:
            print(f"[ERROR] FATAL: El script '{NOMBRE_SCRIPT_GENERAR_CUOTAS}' falló durante su ejecución.")
            print(f"   Código de retorno: {e.returncode}")
            print(f"\n--- Salida de '{NOMBRE_SCRIPT_GENERAR_CUOTAS}' (stdout) ---")
            print(e.stdout)
            print(f"\n--- Errores de '{NOMBRE_SCRIPT_GENERAR_CUOTAS}' (stderr) ---")
            print(e.stderr)
            proceso_exitoso_cuotas = False
        except Exception as e_subproc:
            print(f"[ERROR] FATAL inesperado al intentar ejecutar '{NOMBRE_SCRIPT_GENERAR_CUOTAS}': {e_subproc}")
            proceso_exitoso_cuotas = False

else:
    print("\n----------------------------------------------------")
    print("[WARN] La actualización de saldos de facturas falló.")
    print(f"   Se OMITE la ejecución del script '{NOMBRE_SCRIPT_GENERAR_CUOTAS}'.")
    print("----------------------------------------------------")
    proceso_exitoso_cuotas = False # Marcar como fallo si la actualización previa falló


# 5. SALIDA FINAL DEL SCRIPT
print("\n====================================================")
if proceso_exitoso_actualizacion and proceso_exitoso_cuotas:
    print(">>> PROCESO COMPLETO (Actualización Saldos y Regeneración Cuotas) FINALIZADO EXITOSAMENTE <<<")
    sys.exit(0)
elif proceso_exitoso_actualizacion and not proceso_exitoso_cuotas:
    print(">>> PROCESO INCOMPLETO: Saldos de facturas actualizados, pero falló la regeneración de Cuotas. <<<")
    sys.exit(1)
else:
    print(">>> PROCESO FALLIDO: La actualización de saldos de facturas falló. No se regeneraron cuotas. <<<")
    sys.exit(1)