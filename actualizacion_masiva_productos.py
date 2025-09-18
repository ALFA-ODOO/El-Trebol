import pyodbc
import xmlrpc.client
import logging
from odoo_config import url, db, username, password
from sqlserver_config import sql_server


# ------------------ LOGGING ------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------ CONECTAR A ODOO ------------------
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

# ------------------ CONSULTA A SQL SERVER ------------------
def get_product_data():
    conn_str = (
        f"DRIVER={sql_server['driver']};"
        f"SERVER={sql_server['server']};"
        f"DATABASE={sql_server['database']};"
        f"UID={sql_server['user']};"
        f"PWD={sql_server['password']}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    query = """
    SELECT 
        LTRIM(RTRIM(a.IDARTICULO)) AS IDARTICULO,
        a.DESCRIPCION,
        a.Presentacion,
        a.TasaIVA
    FROM v_ma_articulos a
    WHERE EXISTS (
        SELECT 1 
        FROM V_MA_Precios p 
        WHERE p.IDARTICULO = a.IDARTICULO 
        AND p.TipoLista = 'V' 
        AND SUSPENDIDO = 0
    )
    ORDER BY IDARTICULO
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return rows

# ------------------ ACTUALIZAR PRODUCTOS EN ODOO ------------------
def actualizar_productos():
    productos_sql = get_product_data()
    actualizados = 0
    no_encontrados = 0

    for row in productos_sql:
        idarticulo, descripcion, presentacion, tasa_iva = row
        default_code = str(idarticulo).strip()

        # Buscar producto en Odoo
        product_ids = models.execute_kw(db, uid, password,
            'product.product', 'search',
            [[['default_code', '=', default_code]]],
            {'limit': 1})

        if not product_ids:
            logging.warning(f"❌ Producto no encontrado en Odoo: {default_code}")
            no_encontrados += 1
            continue

        product_id = product_ids[0]

        # Protección contra None
        presentacion = presentacion if presentacion is not None else ""
        tasa_iva = float(tasa_iva) if tasa_iva is not None else 0.0

        # Actualizar campos personalizados
        models.execute_kw(db, uid, password,
            'product.product', 'write',
            [[product_id], {
                'x_tasa_iva': tasa_iva,
                'x_presentacion_producto': presentacion
            }])
        logging.info(f"✅ Actualizado: {default_code} - Tasa IVA: {tasa_iva} - Presentación: {presentacion}")
        actualizados += 1

    logging.info(f"\nResumen:\nProductos actualizados: {actualizados}\nNo encontrados: {no_encontrados}")

# ------------------ EJECUCIÓN ------------------
if __name__ == "__main__":
    actualizar_productos()
