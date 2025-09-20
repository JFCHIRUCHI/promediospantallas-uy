# -*- coding: utf-8 -*-
import re, json, os, time
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

OUT = "unified.json"
UA = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

DEBUG_DIR = "debug"
os.makedirs(DEBUG_DIR, exist_ok=True)

def save_debug(name, content: str):
    try:
        path = os.path.join(DEBUG_DIR, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content if isinstance(content, str) else str(content))
        return path
    except Exception:
        return None

def fetch_variants(urls, timeout=60):
    s = requests.Session()
    s.headers.update(UA)
    last_exc = None
    for u in urls:
        try:
            r = s.get(u, timeout=timeout, allow_redirects=True)
            # Fix encoding issues
            enc = (r.encoding or "").lower()
            if enc in ("iso-8859-1","latin-1","windows-1252"):
                r.encoding = enc
            elif r.apparent_encoding:
                r.encoding = r.apparent_encoding
            r.raise_for_status()
            save_debug("lote21_response_url.txt", u)
            save_debug("lote21_response_len.txt", str(len(r.text)))
            save_debug("lote21_page.html", r.text)
            return r.text
        except Exception as e:
            last_exc = e
            continue
    if last_exc:
        raise last_exc
    raise RuntimeError("No se pudo descargar ninguna variante de Lote21")

def table_candidates_from_html(html):
    # Try pandas on full html first
    cands = []
    try:
        tbls = pd.read_html(html, flavor="lxml")
        for t in tbls:
            cands.append(t)
    except Exception:
        pass
    # Then BeautifulSoup tables one by one
    soup = BeautifulSoup(html, "lxml")
    for t in soup.find_all("table"):
        try:
            df = pd.read_html(str(t))[0]
            cands.append(df)
        except Exception:
            continue
    return cands

def best_lote21_table(cands):
    """Choose the table that most likely contains Categoria + Promedios."""
    scored = []
    for idx, df in enumerate(cands):
        # Drop fully empty columns/rows
        df = df.dropna(how="all")
        df = df.loc[:, ~df.columns.duplicated()]
        df.columns = [str(c).strip() for c in df.columns]
        cols = [unidecode(str(c)).lower() for c in df.columns]
        # score: presence of key words
        score = 0
        if any(("cat" in c or "ategor" in c) for c in cols): score += 2
        if any(("prom" in c) for c in cols): score += 2
        if any(("max" in c or "máx" in c) for c in cols): score += 1
        if any(("min" in c or "mín" in c) for c in cols): score += 1
        # prefer wider tables with numbers
        numerics = (df.applymap(lambda x: bool(re.search(r"[0-9]", str(x))))).sum().sum()
        score += min(numerics, 5) * 0.1
        scored.append((score, idx, df))
    if not scored:
        return None
    scored.sort(key=lambda x: (-x[0], x[1]))
    return scored[0][2]

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

def norm_basic(s):
    s = unidecode(str(s or "")).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_cat_general(original):
    # minimal aliasing inline (front/scraper global aliases can apply too)
    o = norm_basic(original)
    mapping = {
        "terneros 141 a 180 kg": "Terneros entre 140 y 180kg",
        "terneros entre 140 y 180 kg": "Terneros entre 140 y 180kg",
        "terneros mas de 180 kg": "Terneros mas de 180kg",
        "terneros hasta 140 kg": "Terneros hasta 140kg",
    }
    for k,v in mapping.items():
        if norm_basic(k) == o: return v
    return original.strip().title()

def lote21():
    urls = [
        "https://www.lote21.uy/promedios.asp",
        "https://lote21.uy/promedios.asp",
        "http://www.lote21.uy/promedios.asp",
        "http://lote21.uy/promedios.asp",
    ]
    html = fetch_variants(urls, timeout=60)
    # Fecha
    m = re.search(r"(?:Actualizado|Subasta|Remate)?[^\\d]{0,20}(\\d{1,2}/\\d{1,2}/\\d{2,4})", html, flags=re.I)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    # Tablas candidatas
    cands = table_candidates_from_html(html)
    save_debug("lote21_tables_found.txt", f"{len(cands)}")
    if not cands:
        raise RuntimeError("Lote21: no se encontraron tablas (posible render por JS).")
    df = best_lote21_table(cands)
    if df is None:
        raise RuntimeError("Lote21: no se pudo elegir una tabla candidata.")
    # Si la primera fila parece encabezado textual y pandas no la tomo como header:
    if df.shape[0] > 1 and not any(isinstance(x, str) for x in df.columns):
        first = df.iloc[0].astype(str).str.lower().tolist()
        if any("cat" in s or "prom" in s for s in first):
            df.columns = df.iloc[0].tolist()
            df = df.iloc[1:]
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    save_debug("lote21_table_preview.csv", df.head(20).to_csv(index=False))
    c_cat  = pick_col(df.columns, "categoria","categoría","rubro","lote","clase")
    c_prom = pick_col(df.columns, "promedio","prom")
    c_max  = pick_col(df.columns, "maximo","máximo","max")
    c_min  = pick_col(df.columns, "minimo","mínimo","min")
    # Si no hay encabezados claros y hay 4 columnas, asumimos Cat/Prom/Max/Min
    if not c_cat and not c_prom and df.shape[1] >= 4:
        cols = list(df.columns)
        c_cat, c_prom, c_max, c_min = cols[0], cols[1], cols[2], cols[3]
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat_general(r.get(c_cat, ""))
        if not cat or str(cat).strip("-— ") == "":
            continue
        prom = to_float(r.get(c_prom))
        maxv = to_float(r.get(c_max))
        minv = to_float(r.get(c_min))
        if prom is None and maxv is None and minv is None:
            continue
        rows[cat] = {"prom": prom, "max": maxv, "min": minv}
    return urls[0], rows, fecha

# Placeholders for other sources – keep them simple for this patch.
def dummy_source(name, url):
    return url, {}, None

def plaza_rural(): return dummy_source("plaza_rural","https://plazarural.com.uy/promedios")
def pantalla_uruguay(): return dummy_source("pantalla_uruguay","https://www.pantallauruguay.com.uy/promedios/")
def acg(): return dummy_source("acg","https://acg.com.uy/?post_type=precio_semanal")

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
            time.sleep(1.0)
        except Exception as e:
            data["fuentes"][key] = {"url": None, "error": str(e)}
    data["categorias"] = dict(sorted(all_rows.items(), key=lambda kv: kv[0]))
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Hecho. Lote21 rows:", sum(1 for k in data["categorias"] if "lote21" in data["categorias"][k]))

if __name__ == "__main__":
    main()
