# -*- coding: utf-8 -*-
"""
Este script se conecta a una base de datos de SQL Server y a una instancia
de Odoo para actualizar los vendedores asignados a los clientes.
Se ha añadido una interfaz gráfica básica para seleccionar el vendedor
a procesar.
"""

# Importaciones necesarias para la conexión y la GUI
import pyodbc
import xmlrpc.client
import pandas as pd
import tkinter as tk
from tkinter import messagebox, scrolledtext
import sys

# ======================================================================
# --- CONFIGURACIÓN ---
# --- Reemplaza el contenido de abajo con tu configuración real de Odoo y SQL Server.
# ---
# from odoo_config import url, db, username, password
# from sqlserver_config import sql_server

from odoo_config import url, db, username, password
from sqlserver_config import sql_server

# ======================================================================

# Mapeo de IDs de vendedor a nombres para la interfaz
# El "value" del menú será el ID, y la etiqueta será el nombre.
VENDEDORES_MAP = {
    "003": "ADRIAN L.",
    "030": "Benjamin De La Vega",
    "019": "DANIEL R.",
    "001": "LEONARDO C.",
    "024": "Maira L.",
    "026": "Maria L.",
    "017": "RODRIGO M.",
    "1": "Tomas Alevatto",
    "008": "JOSE D.",
    "032": "Jorge Nicolas Barreta",
    "033": "Fernando Quintero",
    "027": "Carlos H."
}

def actualizar_vendedor():
    """
    Función principal para ejecutar la actualización de vendedores.
    Se conecta a las bases de datos y actualiza los registros
    basándose en el ID de vendedor seleccionado en la GUI.
    """
    # Se obtiene el string completo del OptionMenu (ej. "024 - María López")
    vendedor_seleccionado_str = vendedor_var.get()
    
    # Se extrae solo el ID (la parte antes de " - ")
    vendedor_id_str = vendedor_seleccionado_str.split(" - ")[0].strip()
    vendedor_nombre = VENDEDORES_MAP.get(vendedor_id_str, "Desconocido")
    
    # Limpiar el área de log y mostrar el estado inicial
    log_area.delete("1.0", tk.END)
    log_area.insert(tk.END, f"Iniciando actualización para el vendedor: {vendedor_nombre} (ID: {vendedor_id_str})...\n\n")
    root.update_idletasks()

    try:
        # Conexión a Odoo
        log_area.insert(tk.END, "Conectando a Odoo...\n")
        root.update_idletasks()
        common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
        uid = common.authenticate(db, username, password, {})
        models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
        log_area.insert(tk.END, "✅ Conexión a Odoo exitosa.\n")
        root.update_idletasks()

        # Conexión a SQL Server
        log_area.insert(tk.END, "Conectando a SQL Server...\n")
        root.update_idletasks()
        sql_conn = pyodbc.connect(
            f"DRIVER={sql_server['driver']};"
            f"SERVER={sql_server['server']};"
            f"DATABASE={sql_server['database']};"
            f"UID={sql_server['user']};"
            f"PWD={sql_server['password']}"
        )
        cursor = sql_conn.cursor()
        log_area.insert(tk.END, "✅ Conexión a SQL Server exitosa.\n\n")
        root.update_idletasks()

        # Consulta SQL dinámica, filtrando por el ID de vendedor
        consulta_sql = f'''
        SELECT
            VTV.E_Mail, VTV.IdVendedor, VTV.Nombre,
            VC.*
        FROM
            Vt_Clientes AS VC
        INNER JOIN
            V_TA_VENDEDORES AS VTV ON VC.IdVendedor = VTV.IdVendedor
        WHERE
            VC.TipoVista = 'CL'
            AND VTV.IdVendedor = {vendedor_id_str};
        '''

        # Ejecutar la consulta
        log_area.insert(tk.END, f"Consultando clientes en SQL Server para ID: {vendedor_id_str}...\n")
        root.update_idletasks()
        cursor.execute(consulta_sql)
        rows = cursor.fetchall()
        columnas = [column[0] for column in cursor.description]

        log_area.insert(tk.END, f"Se encontraron {len(rows)} clientes. Iniciando el bucle de actualización.\n\n")
        root.update_idletasks()

        if not rows:
            log_area.insert(tk.END, f"⚠️ No se encontraron clientes para el vendedor ID: {vendedor_id_str}.\n")
            cursor.close()
            sql_conn.close()
            return

        errores = []
        for i, row in enumerate(rows):
            registro = dict(zip(columnas, row))
            codigo = str(registro.get("CODIGO", "")).strip()
            email_vendedor = str(registro.get("E_Mail", "")).strip()

            log_area.insert(tk.END, f"Procesando cliente {i+1}/{len(rows)}: Código {codigo}...\n")
            root.update_idletasks()
            
            try:
                # Buscar el contacto en Odoo por su referencia (código)
                log_area.insert(tk.END, f"  - Buscando cliente {codigo} en Odoo...\n")
                root.update_idletasks()
                partner_ids = models.execute_kw(
                    db, uid, password,
                    'res.partner', 'search',
                    [[['ref', '=', codigo]]]
                )

                if not partner_ids:
                    log_area.insert(tk.END, f"  ⚠️ Cliente con código {codigo} no encontrado en Odoo. Saltando.\n")
                    continue

                partner_id = partner_ids[0]

                # Buscar el usuario interno por email
                log_area.insert(tk.END, f"  - Buscando usuario con email {email_vendedor} en Odoo...\n")
                root.update_idletasks()
                user_ids = models.execute_kw(
                    db, uid, password,
                    'res.users', 'search',
                    [[['login', '=', email_vendedor]]]
                )

                if not user_ids:
                    log_area.insert(tk.END, f"  ⚠️ Usuario con email {email_vendedor} no encontrado en Odoo.\n")
                    errores.append({'codigo': codigo, 'error': f"Usuario {email_vendedor} no encontrado"})
                    continue

                user_id = user_ids[0]

                # Actualizar el partner con el vendedor asignado
                log_area.insert(tk.END, f"  - Actualizando cliente {codigo} con el nuevo vendedor...\n")
                root.update_idletasks()
                models.execute_kw(
                    db, uid, password,
                    'res.partner', 'write',
                    [[partner_id], {'user_id': user_id}]
                )
                log_area.insert(tk.END, f"  ✅ [ACTUALIZADO] {codigo} -> Vendedor asignado: {email_vendedor}\n\n")
                root.update_idletasks()

            except Exception as e:
                log_area.insert(tk.END, f"  ❌ Error al procesar {codigo}: {str(e)}\n\n")
                errores.append({'codigo': codigo, 'error': str(e)})
                root.update_idletasks()

        cursor.close()
        sql_conn.close()
        
        # Guardar errores si los hay
        if errores:
            pd.DataFrame(errores).to_csv("C:\\MIGRACION_ODOO\\vendedor_errores.csv", index=False)
            log_area.insert(tk.END, "\n⚠️ Algunos registros fallaron. Revisá: C:\\MIGRACION_ODOO\\vendedor_errores.csv\n")
        
        log_area.insert(tk.END, "\n✅ Asignación de vendedores completada.\n")
        root.update_idletasks()
        
    except Exception as e:
        messagebox.showerror("Error", f"Ocurrió un error inesperado:\n{str(e)}")
        log_area.insert(tk.END, f"❌ Fallo al conectar o procesar: {str(e)}\n")
        root.update_idletasks()
        sys.exit()

# --- Configuración de la interfaz gráfica ---
root = tk.Tk()
root.title("Actualizar Clientes - Odoo")
root.geometry("600x450")
root.configure(bg="#f0f0f0")

main_frame = tk.Frame(root, padx=20, pady=20, bg="#f0f0f0")
main_frame.pack(expand=True, fill="both")

# Título
title_label = tk.Label(main_frame, text="Actualización de Vendedores en Odoo", font=("Arial", 16, "bold"), bg="#f0f0f0")
title_label.pack(pady=(0, 10))

# Instrucciones y menú desplegable
instruction_frame = tk.Frame(main_frame, bg="#f0f0f0")
instruction_frame.pack(pady=10)

instruction_label = tk.Label(instruction_frame, text="Selecciona un vendedor para actualizar:", bg="#f0f0f0", font=("Arial", 10))
instruction_label.pack(side="left", padx=(0, 10))

vendedor_var = tk.StringVar(root)
# Creamos la lista de opciones para el menú desplegable
opciones_desplegable = [f"{id} - {nombre}" for id, nombre in VENDEDORES_MAP.items()]
vendedor_var.set(opciones_desplegable[0])

# El menú desplegable mostrará el nombre y el ID, pero la variable `vendedor_var` guardará el valor
vendedor_menu = tk.OptionMenu(instruction_frame, vendedor_var, *opciones_desplegable)
vendedor_menu.config(width=25, font=("Arial", 10))
vendedor_menu.pack(side="left")

# Botón de actualización
update_button = tk.Button(main_frame, text="Iniciar Actualización", command=actualizar_vendedor,
                         font=("Arial", 12, "bold"), bg="#4CAF50", fg="white", activebackground="#45a049",
                         relief="raised", bd=3)
update_button.pack(pady=20, ipadx=10, ipady=5)

# Área de log con barra de desplazamiento
log_frame = tk.Frame(main_frame)
log_frame.pack(expand=True, fill="both")

log_area = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state="normal", font=("Consolas", 9),
                                     bg="#2c2c2c", fg="#f0f0f0", insertbackground="white")
log_area.pack(expand=True, fill="both")

# Iniciar la interfaz
root.mainloop()
