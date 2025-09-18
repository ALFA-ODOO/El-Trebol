# -*- coding: utf-8 -*-

import pyodbc
import xmlrpc.client
import pandas as pd

from odoo_config import url, db, username, password
from sqlserver_config import sql_server

# Conexión a Odoo
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

# Conexión a SQL Server
sql_conn = pyodbc.connect(
    f"DRIVER={sql_server['driver']};"
    f"SERVER={sql_server['server']};"
    f"DATABASE={sql_server['database']};"
    f"UID={sql_server['user']};"
    f"PWD={sql_server['password']}"
)
cursor = sql_conn.cursor()

# Consulta SQL
consulta_sql = '''
SELECT
    VTV.E_Mail, VTV.IdVendedor, VTV.Nombre,
    VC.*
FROM
    Vt_Clientes AS VC
INNER JOIN
    V_TA_VENDEDORES AS VTV ON VC.IdVendedor = VTV.IdVendedor
WHERE
    VC.TipoVista = 'CL'
    AND VC.IdVendedor IN (003, 030, 019, 031, 001, 024, 026, 017, 1, 002, 008, 032, 033);
'''

cursor.execute(consulta_sql)
rows = cursor.fetchall()
columnas = [column[0] for column in cursor.description]

errores = []

for row in rows:
    registro = dict(zip(columnas, row))
    codigo = str(registro.get("CODIGO")).strip()
    email_vendedor = str(registro.get("E_Mail")).strip()

    try:
        # Buscar el contacto en Odoo
        partner_ids = models.execute_kw(
            db, uid, password,
            'res.partner', 'search',
            [[['ref', '=', codigo]]]
        )

        if not partner_ids:
            print(f"⚠️ Cliente con código {codigo} no encontrado en Odoo.")
            continue

        partner_id = partner_ids[0]

        # Buscar el usuario interno por email
        user_ids = models.execute_kw(
            db, uid, password,
            'res.users', 'search',
            [[['login', '=', email_vendedor]]]
        )

        if not user_ids:
            print(f"⚠️ Usuario con email {email_vendedor} no encontrado en Odoo.")
            errores.append({'codigo': codigo, 'error': f"Usuario {email_vendedor} no encontrado"})
            continue

        user_id = user_ids[0]

        # Actualizar el partner con el vendedor asignado
        models.execute_kw(
            db, uid, password,
            'res.partner', 'write',
            [[partner_id], {'user_id': user_id}]
        )
        print(f"[ACTUALIZADO] {codigo} -> user_id asignado por {email_vendedor}")

    except Exception as e:
        errores.append({'codigo': codigo, 'error': str(e)})

cursor.close()
sql_conn.close()

# Guardar errores
if errores:
    pd.DataFrame(errores).to_csv("C:\\MIGRACION_ODOO\\vendedor_errores.csv", index=False)
    print("\n⚠️ Algunos registros fallaron. Revisá: C:\\MIGRACION_ODOO\\vendedor_errores.csv")

print("\n✅ Asignación de vendedores completada.")
