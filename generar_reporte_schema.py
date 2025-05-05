# -*- coding: utf-8 -*-
# Guardar como: generar_reporte_schema.py

from fpdf import FPDF
from conexion_mysql import conectar
import sys
import os
from datetime import datetime

print("\n--- Script: generar_reporte_schema.py ---")
print("Generando reporte PDF de la estructura de la base de datos...")

# --- Configuración ---
# Lista de las tablas que quieres incluir en el reporte
TABLAS_A_DOCUMENTAR = [
    "clientes",
    "vendedores",
    "ven_supervisor", # Nombre actualizado
    "plazos_pago",
    "facturas",
    "factura_detalle", # Nombre actualizado
    "pagos",
    "pago_conciliados", # Nombre actualizado
    "cuotas",
    "usuarios" # Incluida del diagrama
]

# Nombre del archivo PDF de salida (se añadirá fecha y hora)
NOMBRE_BASE_PDF = "reporte_schema_db"
DIRECTORIO_SALIDA = "C:/mysql_import/" # Directorio donde guardar el PDF

# --- Variables ---
conexion = None
cursor = None
proceso_exitoso = False

# --- Clase PDF personalizada (opcional, para cabecera/pie) ---
class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'Reporte de Estructura de Base de Datos', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        # Fecha y número de página
        fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.cell(0, 10, f'Generado: {fecha_actual} - Página {self.page_no()}/{{nb}}', 0, 0, 'C')

# --- Lógica Principal ---
try:
    # 1. CONECTAR A DB
    print("[DB] Conectando a la base de datos...")
    conexion = conectar()
    if not conexion:
        raise Exception("No se pudo conectar a la base de datos.")
    # Usar cursor normal, no necesariamente dictionary
    cursor = conexion.cursor()
    print("[OK] Conexión establecida.")

    # 2. INICIALIZAR PDF
    pdf = PDF()
    pdf.alias_nb_pages() # Habilitar numeración total de páginas {nb}
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15) # Salto de página automático

    # Título del reporte
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, 'Esquema de Tablas Principales', ln=1, align='C')
    pdf.ln(10)

    # 3. OBTENER Y ESCRIBIR ESTRUCTURA DE CADA TABLA
    for nombre_tabla in TABLAS_A_DOCUMENTAR:
        print(f"[INFO] Obteniendo estructura para tabla: '{nombre_tabla}'...")
        try:
            cursor.execute(f"SHOW CREATE TABLE {nombre_tabla};")
            resultado = cursor.fetchone() # Devuelve una tupla (nombre_tabla, create_statement)

            if resultado and len(resultado) >= 2:
                create_statement = resultado[1] # El segundo elemento es el CREATE TABLE

                # Añadir al PDF
                pdf.set_font('Arial', 'B', 14)
                pdf.cell(0, 10, f"Tabla: {nombre_tabla}", ln=1)

                pdf.set_font('Courier', '', 9) # Fuente monoespaciada para el código SQL
                # Usar multi_cell para manejar saltos de línea y texto largo
                pdf.multi_cell(0, 5, create_statement)
                pdf.ln(10) # Espacio extra entre tablas

            else:
                print(f"[WARN] No se pudo obtener la estructura para la tabla '{nombre_tabla}'. ¿Existe?")
                pdf.set_font('Arial', 'BI', 12)
                pdf.set_text_color(255, 0, 0) # Color rojo para advertencia
                pdf.cell(0, 10, f"Tabla: {nombre_tabla} - ¡No encontrada o sin estructura!", ln=1)
                pdf.set_text_color(0, 0, 0) # Restaurar color negro
                pdf.ln(10)

        except Exception as e_table:
            print(f"[ERROR] Error al obtener estructura para tabla '{nombre_tabla}': {e_table}")
            pdf.set_font('Arial', 'BI', 12)
            pdf.set_text_color(255, 0, 0)
            pdf.cell(0, 10, f"Tabla: {nombre_tabla} - ¡Error al obtener estructura!", ln=1)
            pdf.set_text_color(0, 0, 0)
            pdf.ln(10)

    # 4. GUARDAR EL PDF
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nombre_archivo_pdf = f"{NOMBRE_BASE_PDF}_{timestamp}.pdf"
    ruta_completa_pdf = os.path.join(DIRECTORIO_SALIDA, nombre_archivo_pdf)

    print(f"[INFO] Guardando reporte PDF en: {ruta_completa_pdf}")
    pdf.output(ruta_completa_pdf, 'F')
    print("[OK] Archivo PDF generado exitosamente.")
    proceso_exitoso = True

except Exception as e_general:
    print(f"\n[ERROR] ERROR GENERAL INESPERADO: {e_general}")
    proceso_exitoso = False

finally:
    # 5. CERRAR RECURSOS
    if cursor: cursor.close(); print("[DB] Cursor cerrado.")
    if conexion and conexion.is_connected(): conexion.close(); print("[DB] Conexión a MySQL cerrada.")

# 6. SALIDA FINAL
if proceso_exitoso:
    print("\n[OK] Script de generación de reporte PDF finalizado correctamente.")
    sys.exit(0)
else:
    print("\n[ERROR] Script de generación de reporte PDF finalizado con errores.")
    sys.exit(1)