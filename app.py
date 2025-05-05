# -*- coding: utf-8 -*-
# Guardar como: app.py

# Importar session explícitamente si no está
from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, session
from flask_session import Session
import subprocess
import sys
import os
# Quitar threading y queue si no los usas

# --- Configuración ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AVAILABLE_SCRIPTS = {
    "importar_clientes": {
        "name": "1. Importar Clientes", # <-- Nombre para el botón
        "script_path": "importar_cliente.py"  # <-- Nombre exacto del archivo .py
    },
    # --- FIN NUEVA ENTRADA ---
    "importar_facturas": {
        "name": "2. Importar/Actualizar Facturas (Cabeceras)", # <-- Número actualizado
        "script_path": "importar_facturas.py"
    },
    "importar_detalles": {
        "name": "3. Importar Detalles de Factura (Líneas)", # <-- Número actualizado
        "script_path": "importar_detalle_facturas.py"
    },
    "importar_pagos": {
        "name": "4. Importar Pagos (Registros)", # <-- Número actualizado
        "script_path": "importar_pagos.py"
    },
    "importar_conciliaciones": {
        "name": "5. Importar Aplicaciones de Pago (Conciliaciones)", # <-- Número actualizado
        "script_path": "importar_conciliaciones.py"
    },
    "generar_cuotas": {
        "name": "6. Generar/Actualizar Cuotas (Depende de Facturas y Pagos)", # <-- Número actualizado
        "script_path": "generar_cuotas.py"
    },
        "actualizar_saldos": {
        "name": "7. Actualizar saldos y cuotas (Ejecutar después de importar pagos/concil.)",
        "script_path": "actualizar_saldos_y_cuotas.py"
    }
}

# --- Inicialización de Flask y Flask-Session ---
app = Flask(__name__)
app.config["SECRET_KEY"] = b'_5#y2L"F4Q8z\n\xec]/'
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_USE_SIGNER"] = True
Session(app)

# --- Rutas ---

@app.route('/')
def index():
    """Muestra la página principal y los resultados si existen."""
    # Recuperar resultados de la sesión (si los hay)
    script_result = session.pop('script_result', None) # pop borra el resultado después de leerlo
    return render_template('index.html', scripts=AVAILABLE_SCRIPTS, script_result=script_result)

@app.route('/run_script', methods=['POST'])
def run_script_route():
    """Ejecuta el script y guarda el resultado en la sesión."""
    script_key = request.form.get('script_key')

    if not script_key or script_key not in AVAILABLE_SCRIPTS:
        flash("Error: Script no válido.", "error")
        return redirect(url_for('index'))

    script_info = AVAILABLE_SCRIPTS[script_key]
    script_name = script_info["name"]
    script_file = script_info["script_path"]
    script_full_path = os.path.join(BASE_DIR, script_file)

    if not os.path.exists(script_full_path):
        flash(f"Error: No se encontró el archivo '{script_file}'.", "error")
        return redirect(url_for('index'))

    print(f"--- Ejecutando: {script_name} ({script_file}) ---")
    flash(f"Iniciando ejecución de: {script_name}...", "info") # Flash corto sí funciona

    result_data = {
        "script_name": script_name,
        "stdout": "(No se ejecutó)",
        "stderr": "",
        "returncode": -1,
        "success": False
    }

    try:
        child_env = os.environ.copy()
        child_env['PYTHONIOENCODING'] = 'utf-8'
        process = subprocess.run(
            [sys.executable, script_full_path],
            check=False, capture_output=True, text=True,
            encoding='utf-8', errors='replace', env=child_env
        )

        # Guardar toda la info del resultado
        result_data["stdout"] = process.stdout if process.stdout else "(Sin salida estándar)"
        result_data["stderr"] = process.stderr if process.stderr else ""
        result_data["returncode"] = process.returncode
        result_data["success"] = (process.returncode == 0)

        # Imprimir en consola Flask para depuración
        print(f"--- Salida de {script_file} ---")
        print(result_data["stdout"])
        if result_data["stderr"]: print(f"--- Errores de {script_file} ---\n{result_data['stderr']}")
        print(f"--- Código de retorno: {result_data['returncode']} ---")

    except Exception as e:
        print(f"[ERROR] Excepción al ejecutar subproceso para '{script_file}': {e}")
        result_data["stderr"] = f"Error interno del servidor al ejecutar el script: {e}"
        result_data["success"] = False

    # --- CAMBIO: Guardar resultado en sesión en lugar de flash ---
    session['script_result'] = result_data
    # Añadir un flash corto para indicar finalización
    if result_data["success"]:
        flash(f"'{script_name}' finalizó.", "success")
    else:
        flash(f"'{script_name}' finalizó con errores.", "error")
    # -----------------------------------------------------------

    return redirect(url_for('index')) # Redirige a la misma página principal

# --- Ejecutar la aplicación ---
if __name__ == '__main__':
    # ... (código para crear directorio de sesión y app.run como antes) ...
    print("Iniciando servidor Flask con Flask-Session...")
    session_dir = app.config.get("SESSION_FILE_DIR", os.path.join(BASE_DIR, 'flask_session'))
    if not os.path.exists(session_dir):
        try:
            os.makedirs(session_dir)
            print(f"Directorio de sesión creado en: {session_dir}")
        except OSError as e:
            print(f"Advertencia: No se pudo crear el directorio de sesión '{session_dir}': {e}")
    print("Accede desde tu navegador a: http://127.0.0.1:5000")
    app.run(debug=True, host='0.0.0.0')