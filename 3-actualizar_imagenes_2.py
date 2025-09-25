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

import os
import base64

def cargar_imagen(codigo, ruta_magen=""):
    prefix = r"C:\Alfa Gestion\\Imagenes\\ImagenesWeb\\"
    """
    Carga la imagen de un producto desde el sistema de archivos.
    Primero intenta en carpeta_imagenes usando el código, 
    si no existe intenta en la ruta_magen de SQL.

    Args:
        codigo (str): Código del producto (nombre del archivo sin extensión).
        ruta_magen (str): Ruta alternativa obtenida desde SQL.

    Returns:
        str: Imagen codificada en base64 o None si no se encuentra.
    """
    # 1️⃣ Intentar con carpeta principal
    ruta = os.path.join(carpeta_imagenes, f"{codigo}.jpg")
    if os.path.isfile(ruta):
        with open(ruta, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    # 2️⃣ Intentar con la ruta de SQL (si no está vacía)
    ruta = os.path.join(prefix, ruta_imagen)
    if ruta_magen and os.path.isfile(ruta):
        with open(ruta, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    # 3️⃣ Si no existe en ninguna, devolver None
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
        LTRIM(RTRIM(IDARTICULO)) AS IDARTICULO, 
        RutaImagen
        FROM V_MA_ARTICULOS
        WHERE 
            ModificoImagen = 1
            OR FHALTA >= CAST(GETDATE() - 1 AS DATE)
        ''') # Modificado para traer solo lo necesario

cols = [col[0] for col in cursor.description]
productos_raw = [dict(zip(cols, row)) for row in cursor.fetchall()]
total_productos = len(productos_raw)
print(f"🔢 Total de productos a procesar: {total_productos}")

errores_imagenes = []
productos_actualizados = 0

for i, data in enumerate(productos_raw, 1):
    default_code = data.get("IDARTICULO")
    ruta_imagen = data.get("RutaImagen")
    print(f"{i}/{total_productos} - {default_code}", end=" ")

    img = cargar_imagen(default_code, ruta_imagen)
    if img:
        try:
            existe = models.execute_kw(db, uid, password, "product.template", "search_read", 
                [[['default_code', '=', default_code]]],
                {'fields': ['id', 'image_1920'], 'context': {'active_test': False}})

            if existe:
                producto = existe[0]
                imagen_actual = producto.get("image_1920") or ""
                
                #if imagen_actual == img:
                #    print("⏩ Sin cambios")
                #    continue  # La imagen es la misma, no hacemos update

                models.execute_kw(db, uid, password, "product.template", "write", 
                    [[producto['id']], {"image_1920": img}])
                
                # 🔄 Actualiza el campo en SQL
                cursor.execute(
                    "UPDATE V_MA_ARTICULOS SET ModificoImagen = 0 WHERE IDARTICULO LIKE ?",
                    f"%{default_code}%"
                )
                sql_conn.commit()

                sql_conn.commit()

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
