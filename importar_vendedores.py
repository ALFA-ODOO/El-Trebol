# -*- coding: utf-8 -*-

import pyodbc
import xmlrpc.client
import pandas as pd

from odoo_config import url, db, username, password
from sqlserver_config import sql_server

# === Conexiones ===
print("Conectando a Odoo y SQL Server...")
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

sql_conn = pyodbc.connect(
    f"DRIVER={sql_server['driver']};"
    f"SERVER={sql_server['server']};"
    f"DATABASE={sql_server['database']};"
    f"UID={sql_server['user']};"
    f"PWD={sql_server['password']}"
)
cursor = sql_conn.cursor()

# === Consulta de vendedores desde SQL Server ===
print("Consultando vendedores desde SQL Server...")
cursor.execute("""
    SELECT
        IdVendedor,
        Nombre,
        ISNULL(E_Mail, '') AS E_Mail,
        Domicilio, 
        Localidad, 
        IdProvincia, 
        CodigoPostal, 
        IdTipoDocumento, 
        Telefono, 
        IdLista
    FROM V_TA_VENDEDORES
    WHERE E_Mail IN (
        'tomasdisteltrebol@gmail.com',
        'cumillancaleonardo@hotmail.com',
        'Adrian_luciano@hotmail.com',
        'rodri.mudia@gmail.com',
        'cordaniel.rodriguez@gmail.com',
        'may_cl_92@hotmail.com',
        'marialaura.eltreboltools@gmail.com',
        'maquinasyherramientas77@gmail.com',
        'betinamar86@gmail.com',
        'carlavioletaparedes@gmail.com',
        'Jorgebaretta9@gmail.com',
        'cwiewiorka@gmail.com'
    )
    ORDER BY Nombre;
""")
rows = cursor.fetchall()
columnas = [column[0] for column in cursor.description]

# === Procesar vendedores ===
print(f"Total vendedores a procesar: {len(rows)}")

errores = []

for idx, row in enumerate(rows, start=1):
    vendedor = dict(zip(columnas, row))
    id_vendedor = str(vendedor['IdVendedor']).strip()
    nombre = str(vendedor['Nombre']).strip()
    email = str(vendedor['E_Mail']).strip()
    telefono = str(vendedor.get('Telefono') or '').strip()
    calle = str(vendedor.get('Domicilio') or '').strip()
    ciudad = str(vendedor.get('Localidad') or '').strip()
    provincia = str(vendedor.get('IdProvincia') or '').strip()
    cod_postal = str(vendedor.get('CodigoPostal') or '').strip()

    print(f"Procesando vendedor {idx}/{len(rows)}: {nombre} - {email}")

    if not email:
        print(f"[SALTEADO] {nombre} no tiene email, no se puede crear usuario.")
        continue

    try:
        # Buscar si el usuario ya existe por el email (campo 'login' en res.users)
        # Se cambió la comparación de 'ref' a 'login' (que es el email del usuario)
        user_ids = models.execute_kw(db, uid, password, 'res.users', 'search', [[['login', '=', email]]])
        
        if user_ids:
            user_id = user_ids[0]
            print(f"[EXISTE] Usuario: {nombre} ({email})")
        else:
            user_vals = {
                'name': nombre,
                'login': email,
                'email': email,
                'phone': telefono,
                'street': calle,
                'city': ciudad,
                'zip': cod_postal,
                'notification_type': 'inbox',
                'active': True,
                'company_id': 1,
                'ref': id_vendedor,  # ID de vendedor guardado como referencia
            }
            # Se ha eliminado cualquier referencia al campo x_studio_vendedor_asignado
            user_id = models.execute_kw(db, uid, password, 'res.users', 'create', [user_vals])
            print(f"[CREADO] Usuario: {nombre} ({email})")

        # --- Se elimina la asignación de grupos para evitar errores ---

        # Asignar vendedor al partner correspondiente por ref = IdVendedor
        # Esta parte sigue buscando por 'ref' en res.partner para vincular el usuario creado.
        partner_ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['ref', '=', id_vendedor]]])
        if partner_ids:
            models.execute_kw(db, uid, password, 'res.partner', 'write', [[partner_ids[0]], {
                'user_id': user_id
            }])
            print(f"  └─ Asignado como vendedor al cliente con ref: {id_vendedor}")
        else:
            print(f"  ⚠️ No se encontró partner con ref = {id_vendedor}")

    except Exception as e:
        errores.append({"vendedor": nombre, "email": email, "error": str(e)})
        continue

# === Cierre de conexiones ===
cursor.close()
sql_conn.close()

# === Reporte de errores ===
if errores:
    df_errores = pd.DataFrame(errores)
    ruta_errores = r"C:\\MIGRACION_ODOO\\errores_vendedores.csv"
    df_errores.to_csv(ruta_errores, index=False)
    print(f"\n⚠️ Algunos vendedores fallaron. Revisá: {ruta_errores}")

print("\n✅ Importación de vendedores finalizada.")
