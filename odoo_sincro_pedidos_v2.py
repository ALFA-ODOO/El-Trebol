import xmlrpc.client
import subprocess
import os
import re
import sys
from datetime import datetime
from odoo_config import url, db, username, password
from sqlserver_config import sql_server

# Forzar codificación UTF-8 en consola
try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

def imprimir(msg):
    try:
        print(msg.encode("utf-8", errors="replace").decode())
    except:
        print(msg)

def guardar_log_error(msg):
    try:
        path = "C:/TAREAS_ALFA/el_trebol/odoo/logs/errores_odoo_sqlcmd.txt"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8", errors="replace") as f:
            f.write(msg + "\n")
    except Exception as e:
        imprimir(f"[{datetime.now()}] No se pudo guardar el log: {str(e)}")

imprimir(f"[{datetime.now()}] Iniciando sincronización desde Odoo...")

common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

imprimir(f"{datetime.now()} - Conectado a Odoo con UID: {uid}")
imprimir(f"{datetime.now()} - Buscando pedidos confirmados no sincronizados...")

orders = models.execute_kw(db, uid, password, 'sale.order', 'search_read', [[
    ('state', '=', 'sale'),
    ('x_alfa_sincronizado', '=', False)
]], {
    'fields': ['id', 'name', 'date_order', 'partner_id', 'user_id', 'note']
})
imprimir(f"{datetime.now()} - Se encontraron {len(orders)} pedidos para procesar.")

for order in orders:
    try:
        pedido_id = order['id']
        pedido = order['name']
        fecha_original = order['date_order']
        fecha_convertida = datetime.strptime(fecha_original, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
        
        # Obtener la nota principal del pedido
        obs = order.get('note', '') or ''
        
        sucursal = '9998'
        numero = ''
        letra = 'X'
        tipo_comprobante = 'NP'

        partner_id = order['partner_id'][0]
        partner_data = models.execute_kw(db, uid, password, 'res.partner', 'read', [[partner_id]], {'fields': ['ref', 'email'] })
        codigo_cliente_odoo = partner_data[0].get('ref') or ''
        mail_cliente = partner_data[0].get('email')

        existe_en_clientes = False
        existe_en_proveedores = False

        # --- Nueva lógica para verificar la existencia del cliente ---
        sql_cliente_check = f"SELECT 1 FROM VT_CLIENTES WHERE CODIGO = '{codigo_cliente_odoo}'"
        guardar_log_error(sql_cliente_check)
        temp_cliente_check = f"C:/TAREAS_ALFA/el_trebol/odoo/temp/check_cliente_{pedido}.sql"
        with open(temp_cliente_check, "w", encoding="utf-8") as f:
            f.write(sql_cliente_check)
        comando_cliente_check = f'sqlcmd -S "{sql_server["server"]}" -d "{sql_server["database"]}" -U "{sql_server["user"]}" -P "{sql_server["password"]}" -i "{temp_cliente_check}" -o "{temp_cliente_check}.out"'
        subprocess.run(comando_cliente_check, shell=True, capture_output=True, text=True)
        with open(f"{temp_cliente_check}.out", "r", encoding="utf-8") as f:
            resultado_cliente_check = f.read()
            if re.search(r"^\s*1\s*$", resultado_cliente_check, re.MULTILINE):
                existe_en_clientes = True
        os.remove(temp_cliente_check)
        os.remove(f"{temp_cliente_check}.out")

        if not existe_en_clientes:
            sql_proveedor_check = f"SELECT 1 FROM VT_PROVEEDORES WHERE CODIGO = '{codigo_cliente_odoo}'"
            temp_proveedor_check = f"C:/TAREAS_ALFA/el_trebol/odoo/temp/check_proveedor_{pedido}.sql"
            with open(temp_proveedor_check, "w", encoding="utf-8") as f:
                f.write(sql_proveedor_check)
            comando_proveedor_check = f'sqlcmd -S "{sql_server["server"]}" -d "{sql_server["database"]}" -U "{sql_server["user"]}" -P "{sql_server["password"]}" -i "{temp_proveedor_check}" -o "{temp_proveedor_check}.out"'
            subprocess.run(comando_proveedor_check, shell=True, capture_output=True, text=True)
            with open(f"{temp_proveedor_check}.out", "r", encoding="utf-8") as f:
                resultado_proveedor_check = f.read()
                if re.search(r"^\s*1\s*$", resultado_proveedor_check, re.MULTILINE):
                    existe_en_proveedores = True
            os.remove(temp_proveedor_check)
            os.remove(f"{temp_proveedor_check}.out")

        if not existe_en_clientes and not existe_en_proveedores:
            # Nueva lógica: Buscar en VT_CLIENTES por MAIL
            sql_mail_check = f"SELECT CODIGO FROM VT_CLIENTES WHERE MAIL = '{mail_cliente}'"
            temp_mail_check = f"C:/TAREAS_ALFA/el_trebol/odoo/temp/check_mail_{pedido}.sql"
            
            with open(temp_mail_check, "w", encoding="utf-8") as f:
                f.write(sql_mail_check)
            
            comando_mail_check = f'sqlcmd -S "{sql_server["server"]}" -d "{sql_server["database"]}" -U "{sql_server["user"]}" -P "{sql_server["password"]}" -i "{temp_mail_check}" -o "{temp_mail_check}.out"'
            subprocess.run(comando_mail_check, shell=True, capture_output=True, text=True)
            
            with open(f"{temp_mail_check}.out", "r", encoding="utf-8") as f:
                resultado_mail_check = f.read()
                lines = resultado_mail_check.strip().splitlines()

                imprimir(f"Resultado completo: {lines}")

                # Verificar si hay al menos 3 líneas (encabezado, separador, valor)
                if len(lines) >= 3 and lines[2].strip():
                    codigo_cliente_para_sp = lines[2].strip()
                    imprimir(f"{datetime.now()} - Código encontrado por MAIL en VT_CLIENTES: {codigo_cliente_para_sp}")
                else:
                    codigo_cliente_para_sp = "112010002"
                    imprimir(f"{datetime.now()} - No se encontró cliente ni proveedor ni coincidencia por MAIL. Usando código por defecto '112010002'.")

            os.remove(temp_mail_check)
            os.remove(f"{temp_mail_check}.out")

        else:
            codigo_cliente_para_sp = codigo_cliente_odoo


        # Obtener la nota principal del pedido
        obs = order.get('note', '') or ''

        # 🌟 Eliminar la cadena de "Términos y condiciones" de la nota principal.
        #    Esto cubre el caso de que venga como texto plano en el campo 'note'.
        #    La expresión regular busca 'Términos y condiciones:' seguido de cualquier caracter
        #    que no sea un salto de línea, hasta el final de la línea.
        obs = re.sub(r'Términos y condiciones:.*', '', obs, flags=re.MULTILINE).strip()

        # Obtener todas las líneas del pedido, incluyendo las notas
        lineas = models.execute_kw(db, uid, password, 'sale.order.line', 'search_read', [[('order_id', '=', pedido_id)]], {
            'fields': ['product_id', 'product_uom_qty', 'price_unit', 'discount', 'display_type', 'name']
        })

        # Concatenar las notas de línea a la observación principal, excluyendo la nota de términos y condiciones
        line_notes = [linea['name'] for linea in lineas if linea.get('display_type') == 'line_note' and 'Términos y condiciones:' not in linea['name']]

        if line_notes:
            obs += '\n\n' + '\n'.join(line_notes)
            
        obs_limpia = re.sub('<[^<]+?>', '', obs).replace("'", "''")
        imprimir(f"{datetime.now()} - Ejecutando procedimiento para pedido {pedido}...")

        sql = (
            "SET NOCOUNT ON;\n"
            "DECLARE @pRes INT, @pMensaje NVARCHAR(250), @pIdCpte INT;\n"
            f"EXEC sp_odoo_Alta_Comprobante '{codigo_cliente_para_sp}', '', '{fecha_convertida}', '{obs_limpia}', '{pedido}', '0', "
            f"'{tipo_comprobante}', '{sucursal}', '{numero}', '{letra}', @pRes OUTPUT, @pMensaje OUTPUT, @pIdCpte OUTPUT;\n"
            "SELECT @pRes AS pRes, @pMensaje AS pMensaje, @pIdCpte AS pIdCpte;"
        )

        print(sql)

        imprimir(f"{datetime.now()} - SQL de cliente de Odoo para sp: {sql}")

        sql_file = f"C:/TAREAS_ALFA/el_trebol/odoo/temp/temp_exec_{pedido}.sql"
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(sql)

        comando = f'sqlcmd -S "{sql_server["server"]}" -d "{sql_server["database"]}" -U "{sql_server["user"]}" -P "{sql_server["password"]}" -i "{sql_file}"'
        resultado = subprocess.run(comando, shell=True, capture_output=True, text=True)
        os.remove(sql_file)

        stdout_lines = resultado.stdout.splitlines()
        imprimir("STDOUT del sqlcmd:")
        for i, line in enumerate(stdout_lines):
            imprimir(f"[{i}] '{line}'")

        pIdCpte = None
        pMensaje = ""
        for line in stdout_lines:
            match = re.search(r"^\s*(\d+)\s+(.*?)\s+(\d+)\s*$", line)
            if match:
                _, pMensaje, pIdCpte = match.groups()
                break

        if not pIdCpte or not pIdCpte.isdigit():
            raise Exception(f"Error al dar de alta el comprobante: {pMensaje.strip()}")

        imprimir(f"{datetime.now()} - ID del comprobante generado: {pIdCpte}")

        # Filtrar las líneas para procesar solo los productos
        lineas_productos = [linea for linea in lineas if not linea.get('display_type')]

        imprimir(f"{datetime.now()} - El pedido tiene {len(lineas_productos)} ítems para insertar.")

        for linea in lineas_productos:
            default_code = models.execute_kw(db, uid, password, 'product.product', 'read', [[linea['product_id'][0]]], {'fields': ['default_code']})[0]['default_code']
            producto_codigo = default_code.strip()
            cantidad = linea['product_uom_qty']
            precio_unitario = linea['price_unit']
            descuento = linea['discount'] or 0

            imprimir(f"{datetime.now()} - Insertando ítem Código: '{producto_codigo}' x {cantidad} a {precio_unitario} (desc: {descuento}%)")

            sql_item = (
                "SET NOCOUNT ON;\n"
                f"EXEC sp_odoo_CpteInsumosV2 {pIdCpte}, '{producto_codigo}', {cantidad}, '0',{precio_unitario}, {descuento};\n"
            )

            # Reemplazar caracteres inválidos por guion bajo
            producto_codigo_safe = re.sub(r'[<>:"/\\|?*]', '_', producto_codigo)
            archivo_item = f"C:/TAREAS_ALFA/el_trebol/odoo/temp/temp_items_{pedido}_{producto_codigo_safe}.sql"

            with open(archivo_item, "w", encoding="utf-8") as f:
                f.write(sql_item)

            comando_item = f'sqlcmd -S "{sql_server["server"]}" -d "{sql_server["database"]}" -U "{sql_server["user"]}" -P "{sql_server["password"]}" -i "{archivo_item}"'
            resultado_item = subprocess.run(comando_item, shell=True, capture_output=True, text=True)
            os.remove(archivo_item)

            if resultado_item.returncode != 0:
                raise Exception(f"Error al insertar ítem {producto_codigo}:\nSTDOUT: {resultado_item.stdout}\nSTDERR: {resultado_item.stderr}")
            else:
                imprimir(f"✔️ Ítem insertado correctamente. Respuesta:\n{resultado_item.stdout.strip()}")

        imprimir(f"{datetime.now()} - Pedido {pedido} procesado correctamente.")

        # ✅ Marcar como sincronizado
        try:
            models.execute_kw(db, uid, password, 'sale.order', 'write', [[pedido_id], {
                'x_alfa_sincronizado': True
            }])
            imprimir(f"{datetime.now()} - Pedido {pedido} marcado como sincronizado en Odoo.")
        except Exception as e:
            imprimir(f"{datetime.now()} - ⚠️ No se pudo marcar como sincronizado en Odoo: {str(e)}")
            guardar_log_error(f"{datetime.now()} - Error al marcar como sincronizado el pedido {pedido}: {str(e)}")

    except Exception as e:
        error_msg = f"{datetime.now()} - ERROR en pedido {pedido}: {str(e)}"
        imprimir(error_msg)
        guardar_log_error(error_msg)
        continue

imprimir(f"{datetime.now()} - Proceso finalizado.")