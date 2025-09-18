# -*- coding: utf-8 -*-
import xmlrpc.client
import pandas as pd
from collections import defaultdict
from datetime import datetime
from odoo_config import url, db, username, password

# ===================== PARÁMETROS =====================
# Dejá en [] para analizar TODAS las listas.
PRICELIST_NAMES = []  #["Rolando (ARS)", "Lista 4 (ARS)"]  # [] = todas

# Dejá en None para analizar todos los productos; o poné un código puntual, p.ej. "ELE0002"
CODIGO_PRODUCTO = None  # "ELE0002"

# Sólo detectar (False) o también limpiar (True).
AUTO_FIX = False

# Criterio de conservación al limpiar:
#   "latest" -> conserva la regla con mayor write_date (o mayor id si no hay write_date)
#   "highest_price" -> conserva la de mayor fixed_price
KEEP_STRATEGY = "latest"
# ======================================================

def connect():
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, username, password, {})
    if not uid:
        raise RuntimeError("No se pudo autenticar en Odoo.")
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return uid, models

def get_pricelists(models, uid):
    domain = []
    if PRICELIST_NAMES:
        domain = [('name', 'in', PRICELIST_NAMES)]
    return models.execute_kw(
        db, uid, password, "product.pricelist", "search_read",
        [domain], {"fields": ["id", "name", "currency_id"], "limit": 0}
    )

def get_product_filters(models, uid):
    prod_ids, tmpl_ids = [], []
    if CODIGO_PRODUCTO:
        prods = models.execute_kw(
            db, uid, password, "product.product", "search_read",
            [[('default_code', '=', CODIGO_PRODUCTO)]],
            {"fields": ["id", "product_tmpl_id"], "limit": 0}
        )
        if not prods:
            print(f"⚠️ No se encontró el producto con default_code: {CODIGO_PRODUCTO}")
            return prod_ids, tmpl_ids
        prod_ids = [p["id"] for p in prods]
        tmpl_ids = list({p["product_tmpl_id"][0] for p in prods})
    return prod_ids, tmpl_ids

def read_rules(models, uid, pricelist_ids, prod_ids, tmpl_ids):
    # Traemos reglas con target de producto (variante o plantilla)
    base = ['|', ('product_id', '!=', False), ('product_tmpl_id', '!=', False)]
    if pricelist_ids:
        base = ['&', ('pricelist_id', 'in', pricelist_ids)] + base

    if prod_ids or tmpl_ids:
        sub = []
        if prod_ids:
            sub.append(('product_id', 'in', prod_ids))
        if tmpl_ids:
            sub.append(('product_tmpl_id', 'in', tmpl_ids))
        if len(sub) == 2:
            domain = ['&'] + base + ['|'] + sub
        else:
            domain = ['&'] + base + sub
    else:
        domain = base

    return models.execute_kw(
        db, uid, password, "product.pricelist.item", "search_read",
        [domain],
        {"fields": ["id","pricelist_id","product_id","product_tmpl_id",
                    "min_quantity","fixed_price","date_start","date_end","write_date"],
         "limit": 0}
    )

def best_rule(rules, strategy="latest"):
    if strategy == "highest_price":
        return max(rules, key=lambda r: (float(r.get("fixed_price") or 0.0), r["id"]))
    # latest: por write_date (fallback id)
    def wd(r):
        s = r.get("write_date")
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S") if s else datetime.min
        except Exception:
            return datetime.min
    return max(rules, key=lambda r: (wd(r), r["id"]))

def main():
    uid, models = connect()
    print("✅ Conectado a Odoo")

    pls = get_pricelists(models, uid)
    if not pls:
        print("⚠️ No se encontraron listas según el filtro.")
        return
    pl_ids = [p["id"] for p in pls]
    plx = {p["id"]: p for p in pls}

    prod_ids, tmpl_ids = get_product_filters(models, uid)
    rules = read_rules(models, uid, pl_ids, prod_ids, tmpl_ids)
    print(f"📊 Total de reglas analizadas: {len(rules)}")

    # Agrupar por (lista, objetivo, min_qty). Objetivo: variante o plantilla.
    grupos = defaultdict(list)
    for r in rules:
        pl = r["pricelist_id"][0]
        minq = r.get("min_quantity") or 0.0
        if r.get("product_id"):
            key = ("variant", pl, r["product_id"][0], minq)
        else:
            key = ("template", pl, r["product_tmpl_id"][0], minq)
        grupos[key].append(r)

    # Detectar duplicados (más de 1 regla por grupo)
    duplicados = {k: v for k, v in grupos.items() if len(v) > 1}
    print(f"🔍 Grupos con duplicados: {len(duplicados)}")

    if not duplicados:
        print("✅ No hay duplicados según el criterio (lista, producto/plantilla, min_qty).")
        return

    # Leer info auxiliar de productos/listas para reporte
    prod_ids_all = list({r["product_id"][0] for rs in duplicados.values() for r in rs if r.get("product_id")})
    tmpl_ids_all = list({r["product_tmpl_id"][0] for rs in duplicados.values() for r in rs if r.get("product_tmpl_id")})

    prods = models.execute_kw(db, uid, password, "product.product", "read", [prod_ids_all],
                              {"fields": ["id","default_code","name"]}) if prod_ids_all else []
    tmpls = models.execute_kw(db, uid, password, "product.template", "read", [tmpl_ids_all],
                              {"fields": ["id","default_code","name"]}) if tmpl_ids_all else []
    pidx = {p["id"]: p for p in prods}
    tidx = {t["id"]: t for t in tmpls}

    filas = []
    to_delete = []
    for key, rs in sorted(duplicados.items(), key=lambda kv: (kv[0][1], kv[0][2], kv[0][3])):
        scope, pl_id, obj_id, minq = key
        keep = best_rule(rs, KEEP_STRATEGY)
        keep_id = keep["id"]

        for r in rs:
            pl = plx.get(pl_id, {})
            if scope == "variant":
                p = pidx.get(obj_id, {})
                codigo = p.get("default_code","")
                nombre = p.get("name","")
                tipo = "Variante"
            else:
                t = tidx.get(obj_id, {})
                codigo = t.get("default_code","")
                nombre = t.get("name","")
                tipo = "Plantilla"

            filas.append({
                "ListaID": pl_id,
                "ListaNombre": pl.get("name",""),
                "TipoObjetivo": tipo,
                "ObjetoID": obj_id,
                "Codigo": codigo,
                "Producto": nombre,
                "MinCantidad": minq,
                "ReglaID": r["id"],
                "Precio": r.get("fixed_price"),
                "FechaInicio": r.get("date_start") or "",
                "FechaFin": r.get("date_end") or "",
                "EsConservada": "SI" if r["id"] == keep_id else "NO",
                "Estrategia": KEEP_STRATEGY
            })

            if AUTO_FIX and r["id"] != keep_id:
                to_delete.append(r["id"])

    df = pd.DataFrame(filas).sort_values(["ListaNombre","Codigo","TipoObjetivo","MinCantidad","EsConservada","ReglaID"])
    df.to_csv("pricelist_items_duplicados.csv", index=False, encoding="utf-8-sig")
    print("💾 Generado: pricelist_items_duplicados.csv")

    if AUTO_FIX and to_delete:
        # Borrado en lotes para ser más eficiente
        print(f"🧹 Eliminando {len(to_delete)} reglas duplicadas (manteniendo una por grupo, estrategia: {KEEP_STRATEGY})...")
        # dividir en chunks para evitar límites
        CHUNK = 80
        for i in range(0, len(to_delete), CHUNK):
            batch = to_delete[i:i+CHUNK]
            models.execute_kw(db, uid, password, "product.pricelist.item", "unlink", [batch])
        print("✅ Limpieza finalizada. Revisá el CSV para auditoría.")

if __name__ == "__main__":
    main()
