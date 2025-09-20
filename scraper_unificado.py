# -*- coding: utf-8 -*-
import re, json, os, time
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

OUT = "unified.json"
UA = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def fetch_html(url, timeout=60):
    r = requests.get(url, headers=UA, timeout=timeout)
    # A veces Lote21 sirve ISO-8859-1 sin declarar, forzamos detección
    enc = r.encoding or "utf-8"
    if enc and enc.lower() in ("iso-8859-1","latin-1","windows-1252"):
        r.encoding = enc
    else:
        # fallback a lo que detecte chardet de requests
        if r.apparent_encoding:
            r.encoding = r.apparent_encoding
    r.raise_for_status()
    return r.text

def read_table_any(url, table_selector=None):
    # Primero intentamos con pandas
    try:
        tables = pd.read_html(url, flavor="lxml")
        if tables:
            return tables[0]
    except Exception:
        pass
    # Luego hacemos manual con BeautifulSoup
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")
    # Si hay selector específico, probarlo
    if table_selector:
        t = soup.select_one(table_selector)
        if t is not None:
            return pd.read_html(str(t))[0]
    # Buscar la tabla con cabeceras que contengan Categoria/Promedio/Max/Min
    candidates = []
    for t in soup.find_all("table"):
        try:
            df = pd.read_html(str(t))[0]
        except Exception:
            continue
        cols = [str(c).strip().lower() for c in df.columns]
        score = sum([
            any("cat" in c or "ategor" in c for c in cols),
            any("prom" in c for c in cols),
            any("max" in c or "máx" in c for c in cols),
            any("min" in c or "mín" in c for c in cols),
        ])
        if score >= 2:
            candidates.append((score, df))
    if candidates:
        candidates.sort(key=lambda x: (-x[0], -len(x[1])))
        return candidates[0][1]
    raise RuntimeError(f"No se encontró tabla utilizable en {url}")

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
        # match contiene
        for k, original in norm.items():
            if key in k:
                return original
    return None

ALIASES_FILE = "categories_aliases.json"
ALIASES = {}
if os.path.exists(ALIASES_FILE):
    try:
        with open(ALIASES_FILE, "r", encoding="utf-8") as f:
            ALIASES = json.load(f)
    except Exception:
        ALIASES = {}

def _norm_basic(s: str) -> str:
    s = unidecode(str(s or "")).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_cat_basic(original: str) -> str:
    nb = _norm_basic(original)
    for alias, target in ALIASES.items():
        if _norm_basic(alias) == nb:
            return target
    return original.strip().title()

# ===== Scrapers =====

def plaza_rural():
    url = "https://plazarural.com.uy/promedios"
    df = read_table_any(url)
    fecha = None
    try:
        html = fetch_html(url)
        m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", html)
        if m:
            d,mn,y = m.group(1).split("/")
            y = y if len(y)==4 else ("20"+y)
            fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    except Exception:
        pass
    df.columns = [str(c).strip() for c in df.columns]
    c_cat = pick_col(df.columns, "categoria","categoría")
    c_max = pick_col(df.columns, "max","máx","maximo","máximo")
    c_min = pick_col(df.columns, "min","mín","minimo","mínimo")
    c_prom= pick_col(df.columns, "prom","promedio")
    c_pb  = pick_col(df.columns, "prom bulto","prom. bulto","pb")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat_basic(r.get(c_cat,""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)),
                     "max": to_float(r.get(c_max)),
                     "min": to_float(r.get(c_min)),
                     "prom_bulto": to_float(r.get(c_pb))}
    return url, rows, fecha

def lote21():
    url = "https://www.lote21.uy/promedios.asp"
    html = fetch_html(url)
    # fecha: primer dd/mm/aaaa de la página
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", html)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    # Buscar tabla utilizable manualmente (mejor que pd.read_html directo por bloqueos)
    soup = BeautifulSoup(html, "lxml")
    df = None
    for t in soup.find_all("table"):
        try:
            cand = pd.read_html(str(t))[0]
        except Exception:
            continue
        cand.columns = [str(c).strip() for c in cand.columns]
        cols = [unidecode(str(c)).lower() for c in cand.columns]
        if any("cat" in c for c in cols) and any(("prom" in c) or ("promedio" in c) for c in cols):
            df = cand
            break
    if df is None:
        # último intento: toda la página con pandas
        try:
            tables = pd.read_html(html)
            if tables: df = tables[0]
        except Exception:
            pass
    if df is None:
        raise RuntimeError("No pude leer tabla de Lote21")
    df.columns = [str(c).strip() for c in df.columns]
    c_cat  = pick_col(df.columns, "categoria","categoría")
    c_max  = pick_col(df.columns, "maximo","máximo","max")
    c_min  = pick_col(df.columns, "minimo","mínimo","min")
    c_prom = pick_col(df.columns, "promedio","prom")
    # Limpiar filas vacías o separadores
    df = df.dropna(how="all")
    rows = {}
    for _, r in df.iterrows():
        rawcat = r.get(c_cat, "")
        cat = norm_cat_basic(rawcat)
        if not cat or str(cat).strip("-— ") == "": 
            continue
        prom = to_float(r.get(c_prom))
        maxv = to_float(r.get(c_max))
        minv = to_float(r.get(c_min))
        if prom is None and maxv is None and minv is None:
            continue
        rows[cat] = {"prom": prom, "max": maxv, "min": minv}
    return url, rows, fecha

def pantalla_uruguay():
    url = "https://www.pantallauruguay.com.uy/promedios/"
    df = read_table_any(url)
    html = fetch_html(url)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", html)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    df.columns = [str(c).strip() for c in df.columns]
    c_cat  = pick_col(df.columns, "categoria","categoría")
    c_max  = pick_col(df.columns, "maximo","máximo","max")
    c_min  = pick_col(df.columns, "minimo","mínimo","min")
    c_prom = pick_col(df.columns, "prom","promedio")
    c_pb   = pick_col(df.columns, "prom bulto","prom. bulto","pb")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat_basic(r.get(c_cat,""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)),
                     "max": to_float(r.get(c_max)),
                     "min": to_float(r.get(c_min)),
                     "prom_bulto": to_float(r.get(c_pb))}
    return url, rows, fecha

def acg():
    list_url = "https://acg.com.uy/?post_type=precio_semanal"
    html = fetch_html(list_url)
    soup = BeautifulSoup(html, "lxml")
    first = soup.select_one("article a")
    post_url = first.get("href") if first else list_url
    html_post = fetch_html(post_url)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", html_post)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    def rex(label): 
        return re.search(rf"{label}.*?([\d\.,]+)", html_post, flags=re.I|re.S)
    rows = {}
    for etiqueta, cat in [("Novillo\s*gordo","Novillo gordo (ACG)"),
                          ("Vaca\s*gorda","Vaca gorda (ACG)"),
                          ("Vaquillona\s*gorda","Vaquillona gorda (ACG)")]:
        m2 = rex(etiqueta)
        if m2:
            val = float(str(m2.group(1)).replace(".","").replace(",",".").strip())
            rows[cat] = {"prom": val, "ref": val}
    return post_url, rows, fecha

def main():
    data = {"last_updated_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fuentes": {}, "categorias": {}}
    funcs = {"plaza_rural": plaza_rural, "lote21": lote21, "pantalla_uruguay": pantalla_uruguay, "acg": acg}
    all_rows = {}
    for key, fn in funcs.items():
        try:
            url, rows, fecha = fn()
            data["fuentes"][key] = {"url": url, "fecha": fecha}
            for cat, vals in rows.items():
                all_rows.setdefault(cat, {})
                all_rows[cat][key] = vals
            time.sleep(1.2)
        except Exception as e:
            data["fuentes"][key] = {"url": None, "error": str(e)}
    # ordenar (si existiera CANONICAL en el repo, lo respetará al render; aquí solo empaquetamos)
    data["categorias"] = dict(sorted(all_rows.items(), key=lambda kv: kv[0]))
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Generado {OUT} con {len(all_rows)} categorías; fuentes: {list(data['fuentes'].keys())}")

if __name__ == "__main__":
    main()
