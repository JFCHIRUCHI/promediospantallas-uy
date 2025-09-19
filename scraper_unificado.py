# -*- coding: utf-8 -*-
import re, json, os, time
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

OUT = "unified.json"
UA = {"User-Agent":"Mozilla/5.0"}

# ---------------- Utilidades ----------------
def fetch_html(url, timeout=60):
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return r.text

def read_table_any(url, table_selector=None):
    try:
        tables = pd.read_html(url, flavor="lxml")
        if tables:
            return tables[0]
    except Exception:
        pass
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")
    t = soup.select_one(table_selector or "table")
    if t is None:
        raise RuntimeError(f"No se encontró tabla en {url}")
    return pd.read_html(str(t))[0]

def to_float(x):
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip().replace("\u00a0"," ")
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s)
    except:
        return None

def pick_col(cols, *cands):
    norm = {unidecode(str(c)).lower().strip(): c for c in cols}
    for cand in cands:
        key = unidecode(cand).lower().strip()
        for k, original in norm.items():
            if key in k:
                return original
    return None

# ---------------- Normalización (canónica) ----------------
CANONICAL = [
    "Terneros hasta 140 kg",
    "Terneros 141 a 180 kg",
    "Terneros más de 180 kg",
    "Terneros generales",
    "Enteros + de 1 año",
    "Terneras",
    "Terneros/as",
    "Mixtos + de 1 año",
    "Novillos 1 a 2 años",
    "Novillos 2 a 3 años",
    "Novillos más de 3 años",
    "Holando y cruza Ho",
    "Vacas de invernada",
    "Vaquillonas de 1 a 2 años",
    "Vaquillonas sin servicio",
    "Vacas de cría",
    "Vientres preñados",
    "Vientres entorados",
    "Piezas de cría",
    "Novillo gordo (ACG)",
    "Vaca gorda (ACG)",
    "Vaquillona gorda (ACG)",
]

ALIASES_FILE = "categories_aliases.json"
ALIASES = {}
if os.path.exists(ALIASES_FILE):
    try:
        with open(ALIASES_FILE, "r", encoding="utf-8") as f:
            ALIASES = json.load(f)
    except Exception:
        ALIASES = {}

def norm_cat(raw):
    if raw is None:
        return ""
    original = str(raw).strip()
    if not original:
        return ""
    # 1) alias exacto insensible a acentos/case
    k = unidecode(original).lower().strip()
    for alias, target in ALIASES.items():
        if unidecode(alias).lower().strip() == k:
            return target
    # 2) reglas amplias por patrones
    s = unidecode(original).lower().strip()
    s = re.sub(r"\s+", " ", s)
    RULES = [
        (r"^terneros?.*<\s*140|^terneros?.*hasta\s*140|^terneros?\s*-\s*140", "Terneros hasta 140 kg"),
        (r"^terneros?.*(141|140\s*a\s*180|141\s*a\s*180|141\s*-\s*180|141\s*–\s*180)", "Terneros 141 a 180 kg"),
        (r"^terneros?.*(\+\s*de\s*180|mas\s*de\s*180|\+?\s*180|>\s*180)", "Terneros más de 180 kg"),
        (r"^terneros?.*(generales|grales|gnrales)$", "Terneros generales"),
        (r"^enteros?.*(\+\s*de\s*1|mas\s*de\s*1).*a[nñ]o", "Enteros + de 1 año"),
        (r"^mixtos?.*(\+\s*de\s*1|mas\s*de\s*1).*a[nñ]o", "Mixtos + de 1 año"),
        (r"^terneras?$", "Terneras"),
        (r"^terneros?\s*/\s*as$|^terneros?\s*y\s*terneras?$", "Terneros/as"),
        (r"^novillos?.*(1\s*(–|-|a)\s*2)\s*a[nñ]os?", "Novillos 1 a 2 años"),
        (r"^novillos?.*(2\s*(–|-|a)\s*3)\s*a[nñ]os?", "Novillos 2 a 3 años"),
        (r"^novillos?.*(\+\s*de\s*3|mas\s*de\s*3|\+3)", "Novillos más de 3 años"),
        (r"^holand.*|^holandos.*|holando.*cruza", "Holando y cruza Ho"),
        (r"^vacas?\s*de\s*invernada", "Vacas de invernada"),
        (r"^vaquillonas?.*(1\s*(–|-|a)\s*2)\s*a[nñ]os?", "Vaquillonas de 1 a 2 años"),
        (r"^vaquillonas?.*sin\s*servicio", "Vaquillonas sin servicio"),
        (r"^vacas?\s*de\s*cri[aá]", "Vacas de cría"),
        (r"^vientres?\s*pre[ñn]ados?", "Vientres preñados"),
        (r"^vientres?\s*entorados?", "Vientres entorados"),
        (r"^piezas?\s*de\s*cri[aá]", "Piezas de cría"),
        (r"^novillo\s*gordo", "Novillo gordo (ACG)"),
        (r"^vaca\s*gorda", "Vaca gorda (ACG)"),
        (r"^vaquillona\s*gorda", "Vaquillona gorda (ACG)"),
    ]
    for pat, target in RULES:
        if re.search(pat, s, flags=re.I):
            return target
    return original.strip().title()

# ---------------- Scrapers (cada uno devuelve url, rows, fecha) ----------------
def plaza_rural():
    url = "https://plazarural.com.uy/promedios"
    df = read_table_any(url)
    # fecha dd/mm/aaaa en el HTML si aparece
    fecha = None
    try:
        html = fetch_html(url)
        m = re.search(r"(\\d{1,2}/\\d{1,2}/\\d{2,4})", html)
        if m:
            d,mn,y = m.group(1).split("/")
            y = y if len(y)==4 else ("20"+y)
            fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    except Exception:
        pass
    df.columns = [str(c).strip() for c in df.columns]
    c_cat = pick_col(df.columns, "categoria", "categoría")
    c_max = pick_col(df.columns, "max", "máx", "maximo", "máximo")
    c_min = pick_col(df.columns, "min", "mín", "minimo", "mínimo")
    c_prom = pick_col(df.columns, "prom", "promedio")
    c_pb = pick_col(df.columns, "prom bulto", "prom. bulto", "prom bulto usd")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get(c_cat, ""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)), "max": to_float(r.get(c_max)), "min": to_float(r.get(c_min)), "prom_bulto": to_float(r.get(c_pb))}
    return url, rows, fecha

def lote21():
    url = "https://www.lote21.uy/promedios.asp"
    html = fetch_html(url)
    m = re.search(r"(\\d{1,2}/\\d{1,2}/\\d{2,4})", html)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    tables = pd.read_html(url, flavor="lxml")
    df = None
    for t in tables:
        t.columns = [str(c).strip() for c in t.columns]
        cols = t.columns
        c_cat  = pick_col(cols, "categoria", "categoría")
        c_max  = pick_col(cols, "maximo", "máximo", "max")
        c_min  = pick_col(cols, "minimo", "mínimo", "min")
        c_prom = pick_col(cols, "promedio", "prom")
        if c_cat and c_prom:
            df = t; break
    if df is None:
        df = read_table_any(url)
        df.columns = [str(c).strip() for c in df.columns]
        c_cat  = pick_col(df.columns, "categoria", "categoría")
        c_max  = pick_col(df.columns, "maximo", "máximo", "max")
        c_min  = pick_col(df.columns, "minimo", "mínimo", "min")
        c_prom = pick_col(df.columns, "promedio", "prom")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get(c_cat, ""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)), "max": to_float(r.get(c_max)), "min": to_float(r.get(c_min))}
    return url, rows, fecha

def pantalla_uruguay():
    url = "https://www.pantallauruguay.com.uy/promedios/"
    html = fetch_html(url)
    m = re.search(r"(\\d{1,2}/\\d{1,2}/\\d{2,4})", html)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    df = read_table_any(url)
    df.columns = [str(c).strip() for c in df.columns]
    c_cat  = pick_col(df.columns, "categoria", "categoría")
    c_max  = pick_col(df.columns, "maximo", "máximo", "max")
    c_min  = pick_col(df.columns, "minimo", "mínimo", "min")
    c_prom = pick_col(df.columns, "prom", "promedio")
    c_pb   = pick_col(df.columns, "prom bulto", "prom. bulto", "pb")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get(c_cat, ""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)), "max": to_float(r.get(c_max)), "min": to_float(r.get(c_min)), "prom_bulto": to_float(r.get(c_pb))}
    return url, rows, fecha

def acg():
    list_url = "https://acg.com.uy/?post_type=precio_semanal"
    html = fetch_html(list_url)
    soup = BeautifulSoup(html, "lxml")
    first = soup.select_one("article a")
    post_url = first.get("href") if first else list_url
    html_post = fetch_html(post_url)
    m = re.search(r"(\\d{1,2}/\\d{1,2}/\\d{2,4})", html_post)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    def rex(label): return re.search(rf"{label}.*?([\\d\\.,]+)", html_post, flags=re.I|re.S)
    rows = {}
    for etiqueta, cat in [("Novillo\\s*gordo","Novillo gordo (ACG)"),("Vaca\\s*gorda","Vaca gorda (ACG)"),("Vaquillona\\s*gorda","Vaquillona gorda (ACG)")]:
        m2 = rex(etiqueta)
        if m2:
            val = to_float(m2.group(1))
            rows[cat] = {"prom": val, "ref": val}
    return post_url, rows, fecha

# ---------------- Main ----------------
def main():
    data = {"last_updated_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "fuentes": {}, "categorias": {}}
    funcs = {"plaza_rural": plaza_rural, "lote21": lote21, "pantalla_uruguay": pantalla_uruguay, "acg": acg}
    all_rows = {}
    for key, fn in funcs.items():
        try:
            url, rows, fecha = fn()
            data["fuentes"][key] = {"url": url, "fecha": fecha}
            for cat, vals in rows.items():
                all_rows.setdefault(cat, {})
                all_rows[cat][key] = vals
            time.sleep(1.5)
        except Exception as e:
            data["fuentes"][key] = {"url": None, "error": str(e)}
    # Orden según CANONICAL primero
    ordered = {}
    for c in CANONICAL:
        if c in all_rows: ordered[c] = all_rows[c]
    for c in sorted(all_rows.keys()):
        if c not in ordered: ordered[c] = all_rows[c]
    data["categorias"] = ordered
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Generado {OUT} con {len(all_rows)} categorías")

if __name__ == "__main__":
    main()
