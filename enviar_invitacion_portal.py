import xmlrpc.client
import tkinter as tk
from tkinter import ttk, messagebox
from odoo_config import url, db, username, password

# Conexión XML-RPC con allow_none
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# Lista global de contactos válidos para invitar
contactos_a_invitar = []

def validar_clientes(refs):
    contactos_a_invitar.clear()
    tree.delete(*tree.get_children())

    for ref in refs:
        ref = ref.strip()
        if not ref:
            continue

        try:
            partner_ids = models.execute_kw(db, uid, password,
                'res.partner', 'search', [[['ref', '=', ref]]])

            if not partner_ids:
                tree.insert('', 'end', values=(ref, '-', '-', '-', '❌ No encontrado'))
                continue

            partner_id = partner_ids[0]
            partner = models.execute_kw(db, uid, password,
                'res.partner', 'read', [partner_id], {'fields': ['email', 'name', 'user_ids']})[0]

            nombre = partner.get('name') or ''
            email = partner.get('email') or ''
            tiene_usuario = bool(partner.get('user_ids'))

            if not email:
                estado = '❌ No tiene email'
            elif tiene_usuario:
                estado = '✅ Ya tiene acceso'
            else:
                estado = '✅ Listo para invitar'
                contactos_a_invitar.append({
                    'ref': ref,
                    'partner_id': partner_id,
                    'email': email,
                    'name': nombre
                })

            tree.insert('', 'end', values=(
                ref,
                nombre,
                email,
                '✅' if tiene_usuario else '❌',
                estado
            ))

        except Exception as e:
            tree.insert('', 'end', values=(ref, '-', '-', '-', f'❌ Error: {str(e)}'))

def enviar_invitaciones():
    errores = []
    enviados = []

    for c in contactos_a_invitar:
        try:
            # Buscar si ya hay un usuario con ese login (email)
            existing_user_ids = models.execute_kw(db, uid, password,
                'res.users', 'search', [[['login', '=', c['email']]]])

            if existing_user_ids:
                user_id = existing_user_ids[0]
            else:
                # Crear usuario sin retorno para evitar errores de serialización
                models.execute_kw(db, uid, password,
                    'res.users', 'create', [{
                        'login': c['email'],
                        'email': c['email'],
                        'name': c['name'],
                        'partner_id': c['partner_id'],
                    }], {'context': {'no_return': True}})

                # Buscar nuevamente el user_id
                user_id = models.execute_kw(db, uid, password,
                    'res.users', 'search', [[['login', '=', c['email']]]])[0]

            # Enviar invitación (sin retorno para evitar None)
            models.execute_kw(db, uid, password,
                'res.users', 'action_reset_password', [[user_id]], {'context': {'no_return': True}})

            enviados.append(c['ref'])

        except Exception as e:
            errores.append(f"{c['ref']} - {c['name']}: {str(e)}")

    if errores:
        messagebox.showerror("Errores al invitar", "\n".join(errores))
    else:
        messagebox.showinfo("Éxito", f"✅ Invitaciones enviadas correctamente a: {', '.join(enviados)}")

# UI de Tkinter
def procesar_refs():
    texto = entry.get("1.0", tk.END)
    refs = texto.replace('\n', ',').split(',')
    validar_clientes(refs)

# Interfaz gráfica
root = tk.Tk()
root.title("Invitar al Portal de Odoo")

tk.Label(root, text="Ingresar REF de clientes separados por coma o línea:").pack(pady=5)
entry = tk.Text(root, height=5, width=60)
entry.pack(pady=5)

tk.Button(root, text="🔍 Validar referencias", command=procesar_refs).pack(pady=5)

# Tabla de resultados
columns = ('ref', 'nombre', 'email', 'usuario', 'estado')
tree = ttk.Treeview(root, columns=columns, show='headings', height=10)
for col in columns:
    tree.heading(col, text=col.upper())
    tree.column(col, width=200)
tree.pack(pady=10, fill='x')

tk.Button(root, text="📤 Enviar invitaciones", command=enviar_invitaciones).pack(pady=10)

root.mainloop()
