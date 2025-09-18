# -*- coding: utf-8 -*-
import pyodbc
import xmlrpc.client
import pandas as pd
import datetime
from itertools import groupby
from operator import itemgetter

from odoo_config import url, db, username, password
from sqlserver_config import sql_server

inicio_proceso = datetime.datetime.now()
print(f"\n✨ Inicio de actualización de reglas de precios: {inicio_proceso.strftime('%Y-%m-%d %H:%M:%S')}")

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

def limpiar(valor):
    if pd.isna(valor) or valor is None:
        return ""
    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto

# Obtener tasas de conversión y verificar si se actualizaron hoy
print("\n💰 Obteniendo tasas de conversión...")
cursor.execute("SELECT TOP 1 MONEDA2, MONEDA3, MONEDA4, MONEDA5, FECHA_HORA FROM TA_COTIZACION ORDER BY ID DESC")
tasas_row = cursor.fetchone()

cotizacion_modificada_hoy = False
tasas_conversion = {"2": 1.0, "3": 1.0, "4": 1.0, "5": 1.0}

if tasas_row:
    tasas_conversion = {
        "2": float(tasas_row[0] or 1),
        "3": float(tasas_row[1] or 1),
        "4": float(tasas_row[2] or 1),
        "5": float(tasas_row[3] or 1)
    }
    fecha_cotizacion = tasas_row[4]
    if fecha_cotizacion.date() == datetime.date.today():
        cotizacion_modificada_hoy = True

print(f" -> Tasas de conversión: {tasas_conversion}")
print(f" -> ¿Cotización modificada hoy?: {'Sí' if cotizacion_modificada_hoy else 'No'}")

# --- Nueva lógica: solo procesar si hubo cambio de cotización ---
#if not cotizacion_modificada_hoy:
#    print("\n🎯 No se encontraron cambios en la cotización. Proceso finalizado.")
#    cursor.close()
#    sql_conn.close()
#    fin_proceso = datetime.datetime.now()
#    duracion = fin_proceso - inicio_proceso
#    print(f"Tiempo total: {duracion}")
#    exit()

# Obtener todos los productos con moneda extranjera
print("🔍 Obteniendo productos con moneda extranjera para actualizar...")
cursor.execute("""
    SELECT LTRIM(RTRIM(IDARTICULO))
    FROM v_ma_articulos
    WHERE Moneda IN ('   2', '   3', '   4', '   5') ;   
""")
id_articulos_a_procesar = [row[0] for row in cursor.fetchall()]
print(f" -> Se encontraron {len(id_articulos_a_procesar)} productos con moneda extranjera para procesar.")

# Si no hay artículos para procesar (por ejemplo, si no hay productos con esas monedas), terminamos el script
if not id_articulos_a_procesar:
    print("\n🎯 No se encontraron productos con monedas extranjeras. Proceso finalizado.")
    cursor.close()
    sql_conn.close()
    fin_proceso = datetime.datetime.now()
    duracion = fin_proceso - inicio_proceso
    print(f"Tiempo total: {duracion}")
    exit()


# Obtener los precios de todos los artículos que se deben procesar
print(f"🔍 Se procesarán {len(id_articulos_a_procesar)} artículos en total.")
placeholders = ', '.join(['?'] * len(id_articulos_a_procesar))

cursor.execute(f"""
    SELECT P.*
    FROM V_MA_Precios P
    WHERE P.TipoLista = 'V' AND  LTRIM(RTRIM(P.IdArticulo)) IN {tuple(id_articulos_a_procesar)}
    ORDER BY P.IdLista;
""")

cols = [col[0] for col in cursor.description]
precios_sql = [dict(zip(cols, map(limpiar, row))) for row in cursor.fetchall()]

total_registros_sql = len(precios_sql)
print(f"🔍 Se encontraron {total_registros_sql} registros de precios para procesar.")

errores_precios = []
contador_registros_procesados = 0

precios_por_lista = groupby(sorted(precios_sql, key=itemgetter('IdLista')), key=itemgetter('IdLista'))

print("DEBUG columnas:", cols)
print("DEBUG total registros:", len(precios_sql))
if precios_sql:
    print("DEBUG uno:", precios_sql[0])
    print("DEBUG tiene 'IdLista'?:", 'IdLista' in precios_sql[0], "valor:", precios_sql[0].get('IdLista'))
    # qué IdLista únicos hay
    ids_unicos = sorted({d.get('IdLista') for d in precios_sql})
    print("DEBUG IdLista únicos:", ids_unicos)



for id_lista_sql_grupo, precios_grupo in precios_por_lista:
    id_lista_sql = str(id_lista_sql_grupo)
    precios_lista = list(precios_grupo)

    print(f"\n⚙️ Procesando lista de precios: {id_lista_sql}")

    lista_odoo = models.execute_kw(db, uid, password, "product.pricelist", "search_read", [[['x_idlista', '=', id_lista_sql]]], {"fields": ["id"], "limit": 1})
    if not lista_odoo:
        lista_id = models.execute_kw(db, uid, password, "product.pricelist", "create", [{"name": f"Lista {id_lista_sql}", "currency_id": 19, "x_idlista": id_lista_sql}])
        print(f" -> ✨ Creada lista de precios en Odoo: Lista {id_lista_sql} (ID: {lista_id})")
    else:
        lista_id = lista_odoo[0]["id"]

    try:
        regla_cero_existente = models.execute_kw(db, uid, password, "product.pricelist.item", "search_read", [[
            ["pricelist_id", "=", lista_id],
            ["product_id", "=", False],
            ["min_quantity", "=", 0],
            ["fixed_price", "=", 0]
        ]], {"fields": ["id"], "limit": 1})

        if regla_cero_existente:
            regla_cero_id = regla_cero_existente[0]["id"]
            models.execute_kw(db, uid, password, "product.pricelist.item", "unlink", [[regla_cero_id]])
            print(f" -> 🗑️ Eliminada regla de precio cero existente en lista {id_lista_sql}")
    except Exception as e:
        print(f" -> 💔 Error al intentar eliminar regla cero en lista {id_lista_sql}: {e}")

    total_productos_lista = len(precios_lista)
    contador_productos_lista = 0
    productos_actualizados = 0
    productos_agregados = 0

    for data in precios_lista:
        contador_productos_lista += 1
        contador_registros_procesados += 1
        idart_precio = data.get("IdArticulo")
        print(f" -> 🔄️ Procesando registro {contador_productos_lista}/{total_productos_lista} (Total: {contador_registros_procesados}/{total_registros_sql}) - Producto: {idart_precio}", end="")

        producto_ids = models.execute_kw(db, uid, password, "product.product", "search_read", [[['default_code', '=', idart_precio.strip()]]], {"fields": ["id"], "limit": 1})
        if not producto_ids:
            print(f" - ⚠️ Producto no encontrado en Odoo")
            continue

        producto = producto_ids[0]
        producto_variant_id = producto["id"]

        cursor.execute("SELECT Moneda FROM v_ma_articulos WHERE LTRIM(RTRIM(IDARTICULO)) = ?", idart_precio.strip())
        articulo_data = cursor.fetchone()
        moneda_articulo = limpiar(articulo_data[0]) if articulo_data else "1"

        try:
            precio4 = float(data.get("Precio4") or 0)
            if moneda_articulo != "1" and moneda_articulo in tasas_conversion:
                precio4 *= tasas_conversion[moneda_articulo]
            print(precio4)

            if precio4 > 0:
                regla_existente = models.execute_kw(db, uid, password, "product.pricelist.item", "search_read", [[
                    ["pricelist_id", "=", lista_id],
                    ["product_id", "=", producto_variant_id],
                    ["min_quantity", "=", 1]
                ]], {"fields": ["id"], "limit": 1})

                if regla_existente:
                    regla_id = regla_existente[0]["id"]
                    models.execute_kw(db, uid, password, "product.pricelist.item", "write", [[regla_id], {"fixed_price": round(precio4, 2)}])
                    print(f" - ✅ Actualizada")
                    productos_actualizados += 1
                else:
                    models.execute_kw(db, uid, password, "product.pricelist.item", "create", [{
                        'pricelist_id': lista_id,
                        'product_id': producto_variant_id,
                        'min_quantity': 1,
                        'fixed_price': round(precio4, 2)
                    }])
                    print(f" - ✨ Agregada")
                    productos_agregados += 1
            else:
                print(f" - 🛑 Precio 4 es cero o negativo")

        except Exception as e:
            print(f" - 🚨 Error: {e}")
            errores_precios.append({"IdArticulo": idart_precio, "IdLista": id_lista_sql, "Error": str(e)})

    try:
        models.execute_kw(db, uid, password, "product.pricelist.item", "create", [{
            'pricelist_id': lista_id,
            'product_id': False,
            'min_quantity': 0,
            'fixed_price': 0
        }])
        print(f" -> 🏁 Creada regla de precio cero al final de la lista {id_lista_sql}")
        print(f" -> 📊 Resumen lista {id_lista_sql}: {productos_actualizados} actualizados, {productos_agregados} agregados.")
    except Exception as e:
        print(f" -> 💔 Error al crear regla cero al final de lista {id_lista_sql}: {e}")

cursor.close()
sql_conn.close()

if errores_precios:
    pd.DataFrame(errores_precios).to_csv("errores_actualizacion_precios.csv", index=False)
    print("\n⚠️ Archivo de errores guardado como errores_actualizacion_precios.csv")

fin_proceso = datetime.datetime.now()
duracion = fin_proceso - inicio_proceso
print(f"\n🎯 Actualización de reglas de precios finalizada. Tiempo total: {duracion}")