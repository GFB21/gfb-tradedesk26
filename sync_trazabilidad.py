"""
sync_trazabilidad.py — v29 (2026-09-24)
Holded → Notion: INVENTARIO (CATÁLOGO + TRAZABILIDAD) y VOLUMEN (TN) neto en PEDIDOS.

Se ejecuta en el mismo workflow de GitHub Actions que sync_holded_notion.py, como
paso independiente (si uno falla, el otro sigue).

QUÉ HACE EN CADA EJECUCIÓN
  1. CATÁLOGO: lee /products de Holded y crea/actualiza una fila por SKU
     (nombre, código de fábrica = SKU proveedor, proveedor, familia = TIPOS, tags).
     Las filas con MANUAL marcado no se tocan.
  2. TRAZABILIDAD: una fila por línea de producto de:
        facturas y abonos de venta · facturas y abonos de compra ·
        pedidos de venta (SO) todavía sin facturar (estado PREVISTO)
     SIN PRECIOS. El precio solo se usa internamente para:
        - saber si la cantidad está en TN o en KG (precio unitario ≥ 300 → TN)
        - distinguir un abono que anula cantidad (rectificación / devolución)
          de un abono que solo ajusta precio (este último no entra).
  3. CUADRE por pedido: TN vendidas netas vs TN compradas netas (tolerancia
     0,001 TN). Plazo antes de alarmar por compra que no llega: 1 día
     (3 días LÓPEZ MIRA).
  4. PEDIDOS: para cada pedido con líneas de venta facturadas escribe VOLUMEN (TN)
     = suma neta facturada (solo si cambia). Es el ÚNICO escritor de ese campo
     una vez hay factura (en sync_holded_notion.py v29 el ajuste v21 está apagado).

CLAVES Y SEGURIDAD
  - Clave de línea: <documento>-<nº de línea>. Relanzar nunca duplica.
  - HUELLA: hash de los datos de la línea; solo se escribe lo que cambia.
  - Las líneas que desaparecen de Holded se archivan en Notion (con freno: si la
    carga devuelve menos del 50 % de las líneas existentes, no archiva nada).
  - Errores → salida con código 1 (ejecución en rojo + email de GitHub) y resumen
    en la página de la ejecución.
"""

import hashlib
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import requests

# ── CREDENCIALES ─────────────────────────────────────────────────────────────
# Preferente: secretos de GitHub Actions (NOTION_TOKEN, HOLDED_TOKEN).
# Transitorio: si no existen, se reutilizan los del sync principal.
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
HOLDED_TOKEN = os.environ.get("HOLDED_TOKEN")
if not NOTION_TOKEN or not HOLDED_TOKEN:
    try:
        import sync_holded_notion as _principal
        NOTION_TOKEN = NOTION_TOKEN or _principal.NOTION_TOKEN
        HOLDED_TOKEN = HOLDED_TOKEN or _principal.HOLDED_TOKEN
    except Exception:
        pass

NOTION_HDRS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
HOLDED_HDRS = {"key": HOLDED_TOKEN or ""}

# ── IDS NOTION ───────────────────────────────────────────────────────────────
DB_PEDIDOS = "35eadf8035b080b896c8d5fed8a64a18"
DB_CATALOGO = "c1fb625a80644b12a6f35a99824bdbf3"
DB_TRAZ = "ef5adc5c6c884c6e8a28c4bc1c4c75e3"

# ── PARÁMETROS ───────────────────────────────────────────────────────────────
MADRID = timezone(timedelta(hours=2))
ANIO = datetime.now(MADRID).year
DESDE_TS = int(datetime(ANIO, 1, 1, tzinfo=MADRID).timestamp())
# v29.3: las compras se leen desde el 1 de enero del año anterior, porque muchas
# ventas de este año se compraron (y se facturaron) el año pasado.
DESDE_COMPRAS_TS = int(datetime(ANIO - 1, 1, 1, tzinfo=MADRID).timestamp())
UMBRAL_PRECIO_TN = 300.0          # €/unidad (o $/unidad): ≥ → cantidad en TN
TOLERANCIA_CUADRE_TN = 0.001      # 1 kg
PLAZO_COMPRA_DIAS = {"LOPEZ MIRA": 3}
PLAZO_COMPRA_DEFECTO = 1
RATIO_AJUSTE_PRECIO = 0.30        # abono con precio < 30 % del original → ajuste de precio
FAMILIAS_UD = {"LATAS", "TAPADERAS", "ETIQUETAS", "ENVASES"}
PAUSA_NOTION = 0.34               # ~3 peticiones/s

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("trazabilidad")


class _CapturaErrores(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.errores = []

    def emit(self, record):
        msg = record.getMessage()
        if "(intento" in msg:
            return
        if "❌" in msg or re.search(r"\berror\b", msg, re.IGNORECASE):
            hora = datetime.fromtimestamp(record.created, tz=MADRID).strftime("%H:%M:%S")
            self.errores.append(f"{hora} {msg.strip()}")


captura = _CapturaErrores()
log.addHandler(captura)

# ═════════════════════════════════════════════════════════════════════════════
#  LÓGICA PURA (sin llamadas de red) — testeable
# ═════════════════════════════════════════════════════════════════════════════

RE_NO_PRODUCTO = re.compile(
    r"RETURN OF|CLAIM|FREIGHT|FLETE|TRANSPORTE DE VENTAS|COMMIS|COMISI|COMPENSA|SAMPLE|MUESTRA|"
    r"DISCOUNT|DESCUENTO|OTHER SERVICES|SERVICIOS|LABORATORIO|PENALIZ|CANCELLATION|"
    r"RELABELLING|MISCELLAN|INTEREST|VAT ON|IVA ",
    re.IGNORECASE,
)
RE_SERIE_ABONO = re.compile(r"^(CN|RINV|R-|RECT|ABN|NC)", re.IGNORECASE)
RE_LOTE = re.compile(r"(?:BATCH|LOTE|LOT)\s*(?:NR|NO|N[ºo°])?\s*[:#]?\s*([A-Z0-9][A-Z0-9\-/]{2,})", re.IGNORECASE)


def norm(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def sin_acentos_mayus(s):
    t = norm(s).upper()
    for a, b in (("Á", "A"), ("É", "E"), ("Í", "I"), ("Ó", "O"), ("Ú", "U"), ("Ñ", "N")):
        t = t.replace(a, b)
    return t


def get_cf(doc, nombre):
    for cf in doc.get("customFields") or []:
        if str(cf.get("field", "")).strip().upper() == nombre.upper():
            return norm(cf.get("value"))
    return ""


def tags_de(linea, doc):
    """Tags de la línea si Holded los da por línea; si no, los del documento."""
    t = linea.get("tags") or doc.get("tags") or []
    if isinstance(t, str):
        t = t.split(",")
    return [norm(x).lower().lstrip("#") for x in t if norm(x)]


def pedidos_en_tags(tags):
    """Tags de pedido: 6 dígitos empezando por 25 o 26 (año)."""
    return sorted({t for t in tags if re.fullmatch(r"2[5-9]\d{4}", t)})


def es_linea_producto(nombre, sku, familia, en_catalogo, tiene_pedido):
    """Solo mercancía: fuera fletes, comisiones, servicios, IVA, muestras…
    y fuera cualquier línea que no tenga ni SKU del catálogo ni tag de pedido
    (en compras así se descartan los gastos generales: IT, laboratorio, etc.)."""
    if not (en_catalogo or tiene_pedido):
        return False
    if familia:
        return True
    return not RE_NO_PRODUCTO.search(nombre or "")


def precio_unitario(linea):
    try:
        p = float(linea.get("price") or 0)
        d = float(linea.get("discount") or 0)
        return p * (1 - d / 100.0)
    except (TypeError, ValueError):
        return 0.0


def unidad_y_tn(cantidad, pu, familia):
    if familia in FAMILIAS_UD:
        return "UD", 0.0
    if abs(pu) >= UMBRAL_PRECIO_TN:
        return "TN", cantidad
    return "KG", cantidad / 1000.0


def lote_de(texto):
    m = RE_LOTE.search(texto or "")
    return m.group(1).upper() if m else ""


def huella(d):
    base = {k: v for k, v in d.items() if not k.startswith("_")}
    return hashlib.sha1(json.dumps(base, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()[:16]


def construir_lineas(docs_por_tipo, catalogo, so_docs):
    """
    docs_por_tipo: {"venta": [docs factura/abono venta], "compra": [docs factura/abono compra]}
    catalogo: {SKU: {"familia":..., "sku_proveedor":..., ...}}
    so_docs: {"SO260xxx": doc} pedidos de venta (para PREVISTO y destino final)
    Devuelve lista de dicts (una por línea de TRAZABILIDAD).
    """
    filas = []
    # referencia de precio por (lado, pedido, sku) de las líneas positivas
    ref_precio = defaultdict(list)
    crudas = []
    for lado, docs in docs_por_tipo.items():
        for doc in docs:
            num = norm(doc.get("docNumber")).upper()
            if not num:
                continue
            for idx, ln in enumerate(doc.get("products") or [], start=1):
                nombre = norm(ln.get("name"))
                sku = norm(ln.get("sku")).upper()
                fam = (catalogo.get(sku) or {}).get("familia", "")
                tags = tags_de(ln, doc)
                peds = pedidos_en_tags(tags)
                if not es_linea_producto(nombre, sku, fam, sku in catalogo, bool(peds)):
                    continue
                cantidad = float(ln.get("units") or 0)
                if cantidad == 0:
                    continue
                pu = precio_unitario(ln)
                crudas.append((lado, doc, num, idx, ln, nombre, sku, fam, cantidad, pu, tags, peds))
                if pu > 0 and len(peds) == 1:
                    ref_precio[(lado, peds[0], sku)].append(abs(pu))

    for (lado, doc, num, idx, ln, nombre, sku, fam, cantidad, pu, tags, peds) in crudas:
        # v29.3: la API de Holded NO marca los abonos con precio negativo (el
        # export sí). Se detectan por: endpoint de abonos, unidades o importes
        # negativos, o serie de numeración de rectificativas (CN…, RINV…).
        es_abono = (doc.get("_abono") or cantidad < 0 or pu < 0
                    or float(doc.get("total") or 0) < 0 or float(doc.get("subtotal") or 0) < 0
                    or bool(RE_SERIE_ABONO.match(num)))
        estado = "FACTURADO"
        signo = 1
        if es_abono:
            refs = ref_precio.get((lado, peds[0] if len(peds) == 1 else "", sku)) or []
            ref = max(refs) if refs else None
            if ref is None:
                estado = "REVISAR"          # abono sin factura de referencia
            elif abs(pu) < RATIO_AJUSTE_PRECIO * ref:
                continue                    # ajuste de precio: no entra (sin precios)
            else:
                estado = "DEVOLUCIÓN" if any("devoluc" in t for t in tags) else "RECTIFICACIÓN"
            signo = -1
        unidad, tn = unidad_y_tn(abs(cantidad), pu, fam)
        pedido = peds[0] if len(peds) == 1 else ""
        pendiente = []
        if not sku or sku not in catalogo:
            pendiente.append("SIN SKU")
        if not pedido:
            pendiente.append("SIN PEDIDO")
        if pendiente and estado != "REVISAR":
            estado = "REVISAR"
        desc = norm(ln.get("desc"))
        so = so_docs.get(f"SO{pedido}") if pedido else None
        fila = {
            "linea": f"{num}-{idx}",
            "documento": num,
            "tipo_doc": ("ABONO " if es_abono else "FACTURA ") + ("VENTA" if lado == "venta" else "COMPRA"),
            "lado": "VENTA" if lado == "venta" else "COMPRA",
            "estado": estado,
            "pedido": f"SO{pedido}" if pedido else "",
            "sku": sku,
            "sku_proveedor": (catalogo.get(sku) or {}).get("sku_proveedor", ""),
            "fecha": int(doc.get("date") or 0),
            "contacto": norm(doc.get("contactName")),
            "descripcion": norm(f"{nombre} {desc}")[:1900],
            "cantidad": signo * abs(cantidad),
            "unidad": unidad,
            "tn": round(signo * tn, 3),
            "lote": lote_de(f"{nombre} {desc}") or (get_cf(so, "BATCH") if so else ""),
            "destino": get_cf(so, "FINAL DESTINATION") if so else "",
            "_pendiente_base": pendiente,
            "_multi_pedido": len(peds) > 1,
        }
        filas.append(fila)

    # PREVISTO: SO del año sin ninguna línea de venta facturada
    pedidos_facturados = {f["pedido"] for f in filas if f["lado"] == "VENTA" and f["pedido"]}
    for so_num, so in so_docs.items():
        if not so_num.startswith("SO") or so_num in pedidos_facturados:
            continue
        if int(so.get("date") or 0) < DESDE_TS or int(so.get("status") or 0) == 2:
            continue
        for idx, ln in enumerate(so.get("products") or [], start=1):
            nombre = norm(ln.get("name"))
            sku = norm(ln.get("sku")).upper()
            fam = (catalogo.get(sku) or {}).get("familia", "")
            if not es_linea_producto(nombre, sku, fam, sku in catalogo, True):
                continue
            cantidad = float(ln.get("units") or 0)
            if cantidad <= 0:
                continue
            unidad, tn = unidad_y_tn(cantidad, precio_unitario(ln), fam)
            pend = [] if sku and sku in catalogo else ["SIN SKU"]
            filas.append({
                "linea": f"{so_num}-{idx}", "documento": so_num, "tipo_doc": "PEDIDO VENTA",
                "lado": "VENTA", "estado": "REVISAR" if pend else "PREVISTO",
                "pedido": so_num, "sku": sku,
                "sku_proveedor": (catalogo.get(sku) or {}).get("sku_proveedor", ""),
                "fecha": int(so.get("date") or 0), "contacto": norm(so.get("contactName")),
                "descripcion": norm(f"{nombre} {norm(ln.get('desc'))}")[:1900],
                "cantidad": cantidad, "unidad": unidad, "tn": round(tn, 3),
                "lote": lote_de(norm(ln.get("desc"))) or get_cf(so, "BATCH"),
                "destino": get_cf(so, "FINAL DESTINATION"),
                "_pendiente_base": pend, "_multi_pedido": False,
            })
    return filas


def plazo_para(proveedores):
    for p in proveedores:
        for clave, dias in PLAZO_COMPRA_DIAS.items():
            if clave in sin_acentos_mayus(p):
                return dias
    return PLAZO_COMPRA_DEFECTO


def completar_y_cuadrar(filas, ahora_ts):
    """Añade proveedor/factura de compra/lote proveedor a las líneas de venta,
    calcula CUADRE por pedido, PENDIENTE y CICLO COMPLETO. Devuelve resumen por pedido."""
    por_pedido = defaultdict(lambda: {"venta": [], "compra": []})
    for f in filas:
        if f["pedido"] and f["estado"] not in ("REVISAR", "PREVISTO") and f["unidad"] != "UD":
            por_pedido[f["pedido"]]["venta" if f["lado"] == "VENTA" else "compra"].append(f)

    resumen = {}
    for ped, g in por_pedido.items():
        tv = round(sum(f["tn"] for f in g["venta"]), 3)
        tc = round(sum(f["tn"] for f in g["compra"]), 3)
        provs = sorted({f["contacto"] for f in g["compra"] if f["contacto"]})
        if g["venta"] and g["compra"]:
            cuadre = "OK" if abs(tv - tc) <= TOLERANCIA_CUADRE_TN else "DIFERENCIA"
        elif g["venta"]:
            ult = max(f["fecha"] for f in g["venta"])
            cuadre = "COMPRA PENDIENTE" if (ahora_ts - ult) <= plazo_para(provs) * 86400 else "DIFERENCIA"
        else:
            ult = max(f["fecha"] for f in g["compra"])
            cuadre = "VENTA PENDIENTE" if (ahora_ts - ult) <= PLAZO_COMPRA_DEFECTO * 86400 else "DIFERENCIA"
        if not ped.startswith(f"SO{str(ANIO)[2:]}") and cuadre != "OK":
            cuadre = "AÑO ANTERIOR"   # pedido de otro ejercicio: parte del ciclo cae fuera del periodo leído
        resumen[ped] = {"tn_venta": tv, "tn_compra": tc, "cuadre": cuadre,
                        "proveedores": provs,
                        "facturas_compra": sorted({f["documento"] for f in g["compra"]}),
                        "lotes_proveedor": sorted({f["lote"] for f in g["compra"] if f["lote"]}),
                        "tiene_venta": bool(g["venta"])}

    for f in filas:
        r = resumen.get(f["pedido"])
        pend = list(f["_pendiente_base"])
        if f["lado"] == "VENTA":
            f["cliente"] = f["contacto"]
            f["proveedor"] = ", ".join(r["proveedores"]) if r else ""
            f["factura_compra"] = ", ".join(r["facturas_compra"]) if r else ""
            f["lote_proveedor"] = ", ".join(r["lotes_proveedor"]) if r else ""
            f["tn_venta"], f["tn_compra"] = f["tn"], None
            if f["estado"] != "PREVISTO" and f["unidad"] != "UD":
                if not r or not r["facturas_compra"]:
                    pend.append("SIN COMPRA")
                if not f["lote"]:
                    pend.append("SIN LOTE VENTA")
                if r and r["facturas_compra"] and not r["lotes_proveedor"]:
                    pend.append("SIN LOTE PROVEEDOR")
                if not f["destino"]:
                    pend.append("SIN DESTINO")
        else:
            f["cliente"] = ""
            f["proveedor"] = f["contacto"]
            f["factura_compra"] = f["documento"]
            f["lote_proveedor"] = f["lote"]
            f["lote"] = ""
            f["tn_venta"], f["tn_compra"] = None, f["tn"]
            if f["unidad"] != "UD" and not f["lote_proveedor"]:
                pend.append("SIN LOTE PROVEEDOR")
        f["cuadre"] = r["cuadre"] if (r and f["estado"] not in ("PREVISTO", "REVISAR") and f["unidad"] != "UD") else ""
        f["pendiente"] = sorted(set(pend))
        f["ciclo_completo"] = (f["estado"] in ("FACTURADO", "DEVOLUCIÓN", "RECTIFICACIÓN")
                               and not f["pendiente"] and f["cuadre"] == "OK")
    return resumen


# ═════════════════════════════════════════════════════════════════════════════
#  HOLDED
# ═════════════════════════════════════════════════════════════════════════════

def holded_get(url):
    for intento in range(3):
        try:
            r = requests.get(url, headers=HOLDED_HDRS, timeout=60)
            if r.status_code == 200:
                return r.json()
            log.warning(f"Holded {r.status_code} en {url} (intento {intento+1})")
        except requests.RequestException as e:
            log.warning(f"Holded excepción en {url}: {e} (intento {intento+1})")
        time.sleep(3)
    raise RuntimeError(f"❌ ERROR: Holded no responde: {url}")


def cargar_docs(doc_type, desde_ts=None):
    desde_ts = DESDE_TS if desde_ts is None else desde_ts
    docs, page = [], 0
    vistos = set()
    while True:
        data = holded_get(
            # v29.3: sin starttmp — Holded devuelve 400 si se manda sin endtmp.
            # Se pagina igual que el sync principal y se filtra el año aquí.
            f"https://api.holded.com/api/invoicing/v1/documents/{doc_type}"
            f"?page={page}&limit=500"
        )
        items = data.get("documents", data) if isinstance(data, dict) else data
        if not items or not isinstance(items, list):
            break
        nuevos = [d for d in items if d.get("id") not in vistos]
        if not nuevos:
            break
        for d in nuevos:
            vistos.add(d.get("id"))
            if int(d.get("date") or 0) >= desde_ts:
                docs.append(d)
        if len(items) < 500:
            break
        page += 1
    log.info(f"Holded {doc_type}: {len(docs)} documentos del {ANIO}")
    return docs


def familia_de_producto(p):
    """TIPOS puede venir como atributo, categoría o tipo según la API. Se prueban varios sitios."""
    for a in p.get("attributes") or []:
        nombre = str(a.get("name") or a.get("field") or "").upper()
        if nombre == "TIPOS":
            return sin_acentos_mayus(a.get("value"))
    for k in ("tipos", "TIPOS", "typeName", "categoryName"):
        if p.get(k):
            return sin_acentos_mayus(p[k])
    return ""


def cargar_catalogo_holded():
    data = holded_get("https://api.holded.com/api/invoicing/v1/products")
    prods = data if isinstance(data, list) else data.get("products", [])
    if prods:
        log.info("Muestra de producto Holded (diagnóstico TIPOS): "
                 + json.dumps({k: prods[0].get(k) for k in list(prods[0].keys())[:40]}, ensure_ascii=False, default=str)[:1500])
    cat = {}
    for p in prods:
        sku = norm(p.get("sku")).upper()
        if not sku:
            continue
        tags = p.get("tags") or []
        cat[sku] = {
            "nombre": norm(p.get("name")),
            "sku_proveedor": norm(p.get("factoryCode")),
            "proveedor": norm(p.get("contactName")),
            "familia": familia_de_producto(p),
            "tags": ", ".join(norm(t) for t in tags) if isinstance(tags, list) else norm(tags),
        }
    log.info(f"Holded catálogo: {len(cat)} productos con SKU "
             f"({sum(1 for c in cat.values() if c['familia'])} con familia)")
    return cat


def cargar_paises_contactos():
    paises, page = {}, 1
    try:
        data = holded_get("https://api.holded.com/api/invoicing/v1/contacts")
        for c in data if isinstance(data, list) else []:
            ba = c.get("billAddress") or {}
            paises[norm(c.get("name")).upper()] = norm(ba.get("country") or c.get("country"))
    except RuntimeError as e:
        log.warning(f"No se pudieron leer los países de contactos: {e}")
    return paises


# ═════════════════════════════════════════════════════════════════════════════
#  NOTION
# ═════════════════════════════════════════════════════════════════════════════

def notion(method, url, **kw):
    for intento in range(4):
        r = requests.request(method, url, headers=NOTION_HDRS, timeout=60, **kw)
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", 2)))
            continue
        if r.status_code >= 500:
            time.sleep(2)
            continue
        time.sleep(PAUSA_NOTION)
        return r
    return r


def cargar_db(db_id):
    res, cursor = [], None
    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        r = notion("POST", f"https://api.notion.com/v1/databases/{db_id}/query", json=payload)
        data = r.json()
        if "results" not in data:
            raise RuntimeError(f"❌ ERROR leyendo Notion {db_id}: {data.get('message', '')}")
        res.extend(data["results"])
        if not data.get("has_more"):
            return res
        cursor = data.get("next_cursor")


def txt(props, nombre):
    p = props.get(nombre) or {}
    arr = p.get("title") or p.get("rich_text") or []
    return "".join(x.get("plain_text", "") for x in arr).strip()


def rt(v):
    return {"rich_text": [{"text": {"content": str(v)[:1990]}}]} if v else {"rich_text": []}


def sel(v):
    return {"select": {"name": v}} if v else {"select": None}


def fecha_iso(ts):
    return datetime.fromtimestamp(ts, tz=MADRID).date().isoformat() if ts else None


def props_linea(f, pedido_page, producto_page, paises):
    p = {
        "LÍNEA": {"title": [{"text": {"content": f["linea"]}}]},
        "DOCUMENTO": rt(f["documento"]),
        "TIPO DOC": sel(f["tipo_doc"]),
        "LADO": sel(f["lado"]),
        "ESTADO": sel(f["estado"]),
        "Nº PEDIDO": rt(f["pedido"]),
        "SKU GFB": rt(f["sku"]),
        "SKU PROVEEDOR": rt(f["sku_proveedor"]),
        "FECHA": {"date": {"start": fecha_iso(f["fecha"])} if f["fecha"] else None},
        "CLIENTE": rt(f["cliente"]),
        "PAÍS CLIENTE": rt(paises.get(f["cliente"].upper(), "") if f["cliente"] else ""),
        "DESTINO FINAL": rt(f["destino"]),
        "PROVEEDOR": rt(f["proveedor"]),
        "FACTURA COMPRA": rt(f["factura_compra"]),
        "DESCRIPCIÓN": rt(f["descripcion"]),
        "CANTIDAD": {"number": f["cantidad"]},
        "UNIDAD": sel(f["unidad"]),
        "TN VENTA": {"number": f["tn_venta"]},
        "TN COMPRA": {"number": f["tn_compra"]},
        "LOTE VENTA": rt(f["lote"]),
        "LOTE PROVEEDOR": rt(f["lote_proveedor"]),
        "CUADRE": sel(f["cuadre"]),
        "PENDIENTE": {"multi_select": [{"name": x} for x in f["pendiente"]]},
        "CICLO COMPLETO": {"checkbox": bool(f["ciclo_completo"])},
        "PEDIDO": {"relation": [{"id": pedido_page}] if pedido_page else []},
        "PRODUCTO": {"relation": [{"id": producto_page}] if producto_page else []},
    }
    p["HUELLA"] = rt(huella({k: (v if not isinstance(v, dict) else json.dumps(v, sort_keys=True, default=str))
                              for k, v in p.items()}))
    return p


# ═════════════════════════════════════════════════════════════════════════════
#  PROCESO
# ═════════════════════════════════════════════════════════════════════════════

def sync_catalogo(cat_holded):
    existentes = {}
    for pg in cargar_db(DB_CATALOGO):
        pr = pg["properties"]
        sku = txt(pr, "SKU GFB").upper()
        if sku:
            existentes[sku] = (pg["id"], (pr.get("MANUAL") or {}).get("checkbox", False),
                               txt(pr, "NOMBRE"), txt(pr, "SKU PROVEEDOR"), txt(pr, "PROVEEDOR HABITUAL"),
                               ((pr.get("FAMILIA") or {}).get("select") or {}).get("name", ""), txt(pr, "TAGS"))
    creados = actualizados = 0
    nuevos_skus = []
    for sku, c in cat_holded.items():
        estado = "OK" if c["familia"] else "REVISAR"
        props = {
            "SKU GFB": {"title": [{"text": {"content": sku}}]},
            "NOMBRE": rt(c["nombre"]), "SKU PROVEEDOR": rt(c["sku_proveedor"]),
            "PROVEEDOR HABITUAL": rt(c["proveedor"]), "FAMILIA": sel(c["familia"]),
            "TAGS": rt(c["tags"]), "ESTADO": sel(estado),
        }
        if sku not in existentes:
            r = notion("POST", "https://api.notion.com/v1/pages",
                       json={"parent": {"database_id": DB_CATALOGO}, "properties": props})
            if r.status_code == 200:
                creados += 1
                nuevos_skus.append(sku)
                existentes[sku] = (r.json()["id"], False, c["nombre"], c["sku_proveedor"], c["proveedor"], c["familia"], c["tags"])
            else:
                log.warning(f"❌ ERROR creando producto {sku} en CATÁLOGO: {r.json().get('message', '')}")
            continue
        pid, manual, n, sp, pv, fam, tg = existentes[sku]
        if manual:
            continue
        if (n, sp, pv, fam, tg) != (c["nombre"], c["sku_proveedor"], c["proveedor"], c["familia"], c["tags"]):
            r = notion("PATCH", f"https://api.notion.com/v1/pages/{pid}", json={"properties": props})
            if r.status_code == 200:
                actualizados += 1
            else:
                log.warning(f"❌ ERROR actualizando producto {sku}: {r.json().get('message', '')}")
    sin_familia = [s for s, c in cat_holded.items() if not c["familia"]]
    log.info(f"CATÁLOGO: {creados} creados, {actualizados} actualizados, {len(sin_familia)} sin familia")
    if nuevos_skus and len(existentes) > len(nuevos_skus):
        log.info(f"🆕 Productos nuevos en CATÁLOGO: {', '.join(nuevos_skus[:30])}")
    return {s: v[0] for s, v in existentes.items()}, nuevos_skus


def main():
    if not NOTION_TOKEN or not HOLDED_TOKEN:
        log.error("❌ ERROR: faltan NOTION_TOKEN / HOLDED_TOKEN")
        return {}
    log.info(f"=== INICIO TRAZABILIDAD v29.3 — año {ANIO} ===")
    ahora = int(datetime.now(MADRID).timestamp())

    cat = cargar_catalogo_holded()
    abonos_v = cargar_docs("creditnote")
    abonos_c = cargar_docs("purchaserefund", DESDE_COMPRAS_TS)
    for d in abonos_v + abonos_c:
        d["_abono"] = True
    venta = cargar_docs("invoice") + abonos_v
    compra = cargar_docs("purchase", DESDE_COMPRAS_TS) + abonos_c
    # Diagnóstico: cómo devuelve Holded un abono (signo de unidades/precio/total)
    for d in venta + compra:
        if RE_SERIE_ABONO.match(norm(d.get("docNumber"))) or d.get("_abono"):
            p0 = (d.get("products") or [{}])[0]
            log.info(f"Muestra de abono Holded: {d.get('docNumber')} total={d.get('total')} "
                     f"subtotal={d.get('subtotal')} units={p0.get('units')} price={p0.get('price')} "
                     f"claves={sorted(k for k in d.keys())[:45]}")
            break
    so_list = cargar_docs("salesorder")
    so_docs = {norm(d.get("docNumber")).upper(): d for d in so_list}
    paises = cargar_paises_contactos()

    cat_pages, nuevos = sync_catalogo(cat)

    filas = construir_lineas({"venta": venta, "compra": compra}, cat, so_docs)
    ped_con_venta = {f["pedido"] for f in filas if f["lado"] == "VENTA" and f["pedido"]}
    antes = len(filas)
    filas = [f for f in filas
             if not (f["lado"] == "COMPRA" and f["fecha"] < DESDE_TS and f["pedido"] not in ped_con_venta)]
    log.info(f"Compras del {ANIO-1} sin venta del {ANIO} descartadas: {antes - len(filas)}")
    resumen = completar_y_cuadrar(filas, ahora)

    # PEDIDOS: mapa Nº Pedido → page id (duplicados fuera)
    ped_pages, ped_vol, dup = {}, {}, set()
    for pg in cargar_db(DB_PEDIDOS):
        pr = pg["properties"]
        n = txt(pr, "Nº Pedido").upper()
        if not n:
            continue
        if n in ped_pages:
            dup.add(n)
        ped_pages[n] = pg["id"]
        ped_vol[n] = (pr.get("VOLUMEN (TN)") or {}).get("number")
    for n in dup:
        ped_pages.pop(n, None)

    # TRAZABILIDAD existente
    existentes = {}
    for pg in cargar_db(DB_TRAZ):
        pr = pg["properties"]
        existentes[txt(pr, "LÍNEA")] = (pg["id"], txt(pr, "HUELLA"))

    creadas = actualizadas = iguales = 0
    claves = set()
    for f in filas:
        claves.add(f["linea"])
        props = props_linea(f, ped_pages.get(f["pedido"]), cat_pages.get(f["sku"]), paises)
        h = props["HUELLA"]["rich_text"][0]["text"]["content"]
        if f["linea"] in existentes:
            pid, h_old = existentes[f["linea"]]
            if h_old == h:
                iguales += 1
                continue
            r = notion("PATCH", f"https://api.notion.com/v1/pages/{pid}", json={"properties": props})
            if r.status_code == 200:
                actualizadas += 1
            else:
                log.warning(f"❌ ERROR actualizando línea {f['linea']}: {r.json().get('message', '')}")
        else:
            r = notion("POST", "https://api.notion.com/v1/pages",
                       json={"parent": {"database_id": DB_TRAZ}, "properties": props})
            if r.status_code == 200:
                creadas += 1
            else:
                log.warning(f"❌ ERROR creando línea {f['linea']}: {r.json().get('message', '')}")

    # Archivar líneas que ya no existen en Holded (con freno)
    sobrantes = [k for k in existentes if k and k not in claves]
    archivadas = 0
    if existentes and len(claves) < 0.5 * len(existentes):
        log.warning(f"❌ ERROR: solo {len(claves)} líneas frente a {len(existentes)} existentes — "
                    "no se archiva nada (posible carga incompleta de Holded)")
    else:
        for k in sobrantes:
            r = notion("PATCH", f"https://api.notion.com/v1/pages/{existentes[k][0]}", json={"archived": True})
            if r.status_code == 200:
                archivadas += 1

    # PEDIDOS: VOLUMEN (TN) neto facturado
    # Freno v29.3: si un pedido tiene un documento que por su serie parece abono
    # pero se ha clasificado como factura, NO se escribe su volumen.
    sospechosos = {f["pedido"] for f in filas
                   if f["pedido"] and RE_SERIE_ABONO.match(f["documento"]) and f["tipo_doc"].startswith("FACTURA")}
    for ped in sorted(sospechosos):
        log.warning(f"❌ ERROR abono no reconocido en {ped} — no se escribe su VOLUMEN")
    vol_escritos = 0
    for ped, r in resumen.items():
        if not r["tiene_venta"] or ped not in ped_pages or ped in sospechosos:
            continue
        actual = ped_vol.get(ped)
        if r["tn_venta"] < 0:
            log.warning(f"❌ ERROR volumen neto negativo en {ped} ({r['tn_venta']} TN): revisar tags de abonos — no se escribe")
            continue
        if actual is not None and abs(actual - r["tn_venta"]) <= 0.0005:
            continue
        rr = notion("PATCH", f"https://api.notion.com/v1/pages/{ped_pages[ped]}",
                    json={"properties": {"VOLUMEN (TN)": {"number": r["tn_venta"]}}})
        if rr.status_code == 200:
            vol_escritos += 1
            log.info(f"  📦 {ped}: VOLUMEN (TN) {actual} → {r['tn_venta']} (neto facturado)")
        else:
            log.warning(f"❌ ERROR escribiendo VOLUMEN en {ped}: {rr.json().get('message', '')}")
    if dup:
        log.info(f"Pedidos duplicados en Notion (no se tocan): {', '.join(sorted(dup))}")

    # Resumen
    ventas_fact = [f for f in filas if f["lado"] == "VENTA" and f["estado"] != "PREVISTO"]
    completas = sum(1 for f in ventas_fact if f["ciclo_completo"])
    cuadres = defaultdict(int)
    for r in resumen.values():
        cuadres[r["cuadre"]] += 1
    revisar = sum(1 for f in filas if f["estado"] == "REVISAR")
    res = {
        "lineas": len(filas), "creadas": creadas, "actualizadas": actualizadas, "sin_cambios": iguales,
        "archivadas": archivadas, "revisar": revisar, "volumen_pedidos_escritos": vol_escritos,
        "ciclo_completo_pct": round(100 * completas / len(ventas_fact), 1) if ventas_fact else 0,
        "cuadre_ok": cuadres["OK"], "cuadre_diferencia": cuadres["DIFERENCIA"],
        "compra_pendiente": cuadres["COMPRA PENDIENTE"], "venta_pendiente": cuadres["VENTA PENDIENTE"],
        "productos_nuevos": len(nuevos),
        "tn_vendidas_netas": round(sum(f["tn"] for f in ventas_fact if f["estado"] != "REVISAR"), 3),
        "tn_compradas_netas": round(sum(f["tn"] for f in filas if f["lado"] == "COMPRA" and f["estado"] != "REVISAR"), 3),
    }
    for k, v in res.items():
        log.info(f"  {k}: {v}")
    for ped, r in sorted(resumen.items()):
        if r["cuadre"] == "DIFERENCIA":
            log.info(f"  ⚖️  {ped}: venta {r['tn_venta']} TN vs compra {r['tn_compra']} TN")
    log.info("=== FIN TRAZABILIDAD v29.3 ===")
    return res


def _resumen_github(res):
    ruta = os.environ.get("GITHUB_STEP_SUMMARY")
    if not ruta:
        return
    lin = ["## Trazabilidad Holded → Notion", ""]
    lin += ([f"### ❌ {len(captura.errores)} error(es)", ""] + [f"- {e}" for e in captura.errores]
            if captura.errores else ["### ✅ Sin errores"])
    if res:
        lin += ["", "| Métrica | Valor |", "|---|---|"] + [f"| {k} | {v} |" for k, v in res.items()]
    with open(ruta, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lin) + "\n")


if __name__ == "__main__":
    res = None
    try:
        res = main()
    except Exception as e:
        log.exception(f"❌ ERROR no controlado: {e}")
    _resumen_github(res)
    if captura.errores:
        print(f"\n{len(captura.errores)} ERROR(ES):")
        for e in captura.errores:
            print("  -", e)
        sys.exit(1)
