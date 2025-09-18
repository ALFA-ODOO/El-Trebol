# -*- coding: utf-8 -*-
import xmlrpc.client
import pyodbc
from odoo_config import url, db, username, password
from sqlserver_config import sql_server

# Conexión a Odoo
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

# Contadores
archivados = 0
omitidos = 0
errores = []
total_productos = 0
productos_procesados = 0

print("🔍 Procesando productos a archivar desde la base de datos SQL...")

try:
    # Establecer conexión a la base de datos SQL Server
    cnxn_str = (
        f"DRIVER={sql_server['driver']};"
        f"SERVER={sql_server['server']};"
        f"DATABASE={sql_server['database']};"
        f"UID={sql_server['user']};"
        f"PWD={sql_server['password']}"
    )
    cnxn = pyodbc.connect(cnxn_str)
    cursor = cnxn.cursor()

    # Ejecutar la consulta SQL y obtener el total de productos a procesar
    cursor.execute("SELECT COUNT(a.IdArticulo) FROM V_MA_ARTICULOS a WHERE NOT EXISTS (SELECT 1 FROM V_MA_Precios p WHERE p.IDARTICULO = a.IDARTICULO AND p.TipoLista = 'V')")
    total_productos = cursor.fetchone()[0]
    print(f"⏳ Total de productos a procesar: {total_productos}")

    # Ejecutar la consulta SQL para obtener los productos
    cursor.execute('''
    SELECT a.IdArticulo
    FROM V_MA_ARTICULOS a
    WHERE NOT EXISTS (
        SELECT 1
        FROM V_MA_Precios p
        WHERE p.IDARTICULO = a.IDARTICULO AND p.TipoLista = 'V'
    )
    UNION ALL
    SELECT a.IdArticulo
    FROM V_MA_ARTICULOS a
    WHERE a.SUSPENDIDO = 1;
    ''')
    rows = cursor.fetchall()

    for row_sql in rows:
        codigo_articulo = row_sql[0].strip()  # El IdArticulo de SQL está aquí

        try:
            productos = models.execute_kw(db, uid, password, "product.template", "search_read",
                                        [[["default_code", "=", codigo_articulo]]], {"fields": ["id", "active"], "limit": 1})

            if productos:
                producto = productos[0]
                if producto["active"]:
                    models.execute_kw(db, uid, password, "product.template", "write",
                                        [[producto["id"]], {"active": False}])
                    archivados += 1
                    print(f"📂 Archivado {productos_procesados}/{total_productos} (IdArticulo: {codigo_articulo}, Odoo ID: {producto['id']})...", end='\r')
                else:
                    omitidos += 1
                    print(f"🟡 Ya estaba archivado {productos_procesados}/{total_productos} (IdArticulo: {codigo_articulo}, Odoo ID: {producto['id']})...", end='\r')
            else:
                errores.append({"idarticulo": codigo_articulo, "error": "No encontrado en Odoo"})
                print(f"❌ Error {productos_procesados}/{total_productos} (IdArticulo: {codigo_articulo}): No encontrado en Odoo...", end='\r')

        except Exception as e:
            errores.append({"idarticulo": codigo_articulo, "error": str(e)})
            print(f"🔥 Error {productos_procesados}/{total_productos} (IdArticulo: {codigo_articulo}): {e}...", end='\r')

        productos_procesados += 1

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    errores.append({"error_sql": f"Error de conexión SQL: {sqlstate}"})
    print(f"\n💔 Error de conexión SQL: {sqlstate}")
finally:
    # Cerrar la conexión SQL
    if cnxn:
        cnxn.close()

# Reporte final
print("\n\n📊 Resultado final:")
print(f"✔️ Productos archivados: {archivados}")
print(f"⏭️ Productos omitidos (ya estaban archivados): {omitidos}")
print(f"❌ Errores: {len(errores)}")

# Guardar errores si hubo
if errores:
    import pandas as pd
    pd.DataFrame(errores).to_csv("errores_archivar.csv", index=False)
    print("⚠️ Archivo de errores guardado como errores_archivar.csv")