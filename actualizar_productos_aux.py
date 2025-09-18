# -*- coding: utf-8 -*-
import pyodbc
import xmlrpc.client
import base64
import os
import pandas as pd
import datetime

from odoo_config import url, db, username, password
from sqlserver_config import sql_server

inicio_proceso = datetime.datetime.now()
print(f"Inicio de la carga de artículos (sin categorías): {inicio_proceso.strftime('%Y-%m-%d %H:%M:%S')}")

MAP_UNIDADES = {"UN": 1, "KG": 15, "GR": 111, "LT": 12, "M": 8, "CM": 7, "MM": 6, "PA": 117, "CA": 116, "BL": 107, "CJ": 116, "PZ": 1}
carpeta_imagenes = r"C:\Alfa Gestion\Imagenes\ImagenesWeb"

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

cursor.execute("SELECT TOP 1 MONEDA2, MONEDA3, MONEDA4, MONEDA5 FROM TA_COTIZACION ORDER BY ID DESC")
tasas = cursor.fetchone()
tasas_conversion = {"2": float(tasas[0] or 1), "3": float(tasas[1] or 1), "4": float(tasas[2] or 1), "5": float(tasas[3] or 1)}

cursor.execute("""
   SELECT 
        a.IDARTICULO, 
        a.DESCRIPCION, 
        a.IDUNIDAD, 
        a.IDFAMILIA, 
        a.TasaIva, 
        a.Moneda, 
        a.IDTIPO,
        a.Procedencia, 
        a.PRECIO1, 
        a.COSTO, 
        a.SUSPENDIDO, 
        a.RutaImagen, 
        a.IDRUBRO, 
        f.Descripcion AS DescripcionFamilia         
    FROM v_ma_articulos a 
    LEFT JOIN V_TA_Familias f ON a.IDFAMILIA = f.IdFamilia
    WHERE EXISTS (
        SELECT 1 
        FROM V_MA_Precios p 
        WHERE p.IDARTICULO = a.IDARTICULO 
        AND p.TipoLista = 'V'
    )
    and a.IDARTICULO = 'NAV484'
    order by IdArticulo 
""")

productos_raw = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
print(f"Total productos a procesar: {len(productos_raw)}")

productos_actualizados = 0
productos_omitidos = 0
errores_productos = []

for producto in productos_raw:
    default_code = producto.get("IDARTICULO").strip()
    name = producto.get("DESCRIPCION")
    procedencia = producto.get("Procedencia")
    tasaIva = producto.get("TasaIva")
    presentacion = producto.get("Presentacion")
    unidad_id = MAP_UNIDADES.get(producto.get("IDUNIDAD"), MAP_UNIDADES.get("UN"))
    precio = float(producto.get("PRECIO1") or 0)
    costo = float(producto.get("COSTO") or 0)
    activo = producto.get("SUSPENDIDO") != "1"
    familia_id = producto.get("IDFAMILIA")
    ruta_imagen = os.path.join(carpeta_imagenes, f"{default_code}.jpg")

    producto_vals = {
        "name": name,
        "default_code": default_code,
        "uom_id": unidad_id,
        "standard_price": round(costo, 2),
        "list_price": round(precio, 2),
        "x_idfamilia_familia": familia_id,
        "x_tasa_iva": tasaIva,
        "x_presentacion_producto": presentacion,
        "x_procedencia": procedencia,
        "active": activo,
    }

    if os.path.exists(ruta_imagen):
        with open(ruta_imagen, "rb") as img_file:
            producto_vals["image_1920"] = base64.b64encode(img_file.read()).decode("utf-8")

    try:
        existe = models.execute_kw(db, uid, password, "product.template", "search", [[['default_code', '=', default_code]]], {"context": {"active_test": False}})
        if existe:
            models.execute_kw(db, uid, password, "product.template", "write", [existe, producto_vals])
            productos_actualizados += 1
            print(f" Actualizado (ID: {existe[0]}) - {name}")
        else:
            productos_omitidos += 1
            print(f" Omitido (no existe en Odoo) - {default_code} - {name}")

    except Exception as e:
        errores_productos.append({"IDARTICULO": default_code, "Descripcion": name, "Error": str(e)})
        print(f" Error procesando {default_code}: {e}")

cursor.close()
sql_conn.close()

print(f"Resumen de la carga:")
print(f"  - Productos actualizados: {productos_actualizados}")
print(f"  - Productos omitidos (no encontrados): {productos_omitidos}")

if errores_productos:
    pd.DataFrame(errores_productos).to_csv("errores_actualizacion_productos.csv", index=False)
    print("Archivo de errores guardado como errores_actualizacion_productos.csv")

fin_proceso = datetime.datetime.now()
duracion = fin_proceso - inicio_proceso
print(f"Actualización de artículos finalizada. Tiempo total: {duracion}")
