# -*- coding: utf-8 -*-
import pyodbc
import xmlrpc.client
import base64
import os
import datetime
import pandas as pd
from odoo_config import url, db, username, password
from sqlserver_config import sql_server

carpeta_imagenes = r"C:\Alfa Gestion\Imagenes\ImagenesWeb"  # Reemplaza con la ruta de tu carpeta de imágenes

inicio_proceso = datetime.datetime.now()
print(f"\n✨ Inicio de la carga de imágenes de artículos: {inicio_proceso.strftime('%Y-%m-%d %H:%M:%S')}")

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

def cargar_imagen(codigo):
    """
    Carga la imagen de un producto desde el sistema de archivos.

    Args:
        codigo (str): El código del producto (nombre del archivo de la imagen sin la extensión).

    Returns:
        str: La imagen codificada en base64, o None si no se encuentra la imagen.
    """
    ruta = os.path.join(carpeta_imagenes, f"{codigo}.jpg")
    if os.path.isfile(ruta):
        with open(ruta, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    return None

def registrar_error(lista, **kwargs):
    """
    Registra un error en una lista.

    Args:
        lista (list): La lista donde se registrará el error.
        **kwargs: Los detalles del error (por ejemplo, codigo, mensaje).
    """
    lista.append(kwargs)

print("\n📦 Importando imágenes de productos...")
cursor.execute('''
SELECT 
ltrim(rtrim(a.IDARTICULO)) as IDARTICULO, 
a.RutaImagen        
FROM v_ma_articulos a 
LEFT JOIN V_TA_Familias f ON a.IDFAMILIA = f.IdFamilia
WHERE EXISTS (
SELECT 1 
FROM V_MA_Precios p 
WHERE p.IDARTICULO = a.IDARTICULO 
AND p.TipoLista = 'V')
AND a.IDARTICULO like '%PROTEJ%'
order by IdArticulo
        ''') # Modificado para traer solo lo necesario

cols = [col[0] for col in cursor.description]
productos_raw = [dict(zip(cols, row)) for row in cursor.fetchall()]
total_productos = len(productos_raw)
print(f"🔢 Total de productos a procesar: {total_productos}")

errores_imagenes = []
productos_actualizados = 0

for i, data in enumerate(productos_raw, 1):
    default_code = data.get("IDARTICULO")
    print(f"{i}/{total_productos} - {default_code}", end=" ")

    img = cargar_imagen(default_code)
    if img:
        try:
            existe = models.execute_kw(db, uid, password, "product.template", "search_read", 
                [[['default_code', '=', default_code]]],
                {'fields': ['id', 'image_1920'], 'context': {'active_test': False}})

            if existe:
                producto = existe[0]
                imagen_actual = producto.get("image_1920") or ""
                
                if imagen_actual == img:
                    print("⏩ Sin cambios")
                    continue  # La imagen es la misma, no hacemos update

                models.execute_kw(db, uid, password, "product.template", "write", 
                    [[producto['id']], {"image_1920": img}])
                productos_actualizados += 1
                print("✅ Imagen actualizada")
            else:
                print("❌ Producto no encontrado")
                registrar_error(errores_imagenes, codigo=default_code, mensaje="Producto no encontrado en Odoo")

        except Exception as e:
            registrar_error(errores_imagenes, codigo=default_code, mensaje=str(e))
            print(f"❌ Error: {e}")
    else:
        print("❌ Imagen no encontrada")
        registrar_error(errores_imagenes, codigo=default_code, mensaje="Imagen no encontrada en el sistema de archivos")


cursor.close()
sql_conn.close()

print(f"\n📊 Resumen de la carga de imágenes:")
print(f"  - Imágenes actualizadas: {productos_actualizados}")

if errores_imagenes:
    pd.DataFrame(errores_imagenes).to_csv("errores_carga_imagenes.csv", index=False)
    print("\n⚠️ Archivo de errores guardado como errores_carga_imagenes.csv")

fin_proceso = datetime.datetime.now()
duracion = fin_proceso - inicio_proceso
print(f"\n🎯 Carga de imágenes finalizada. Tiempo total: {duracion}")
