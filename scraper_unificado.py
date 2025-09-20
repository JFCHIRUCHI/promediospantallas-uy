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

def fetch_html(url, timeout=60):
    r = requests.get(url, headers=UA, timeout=timeout)
    enc = (r.encoding or "").lower()
    if enc in ("iso-8859-1","latin-1","windows-1252"):
        r.encoding = enc
    elif r.apparent_encoding:
        r.encoding = r.apparent_encoding
    r.raise_for_status()
    return r.text

def read_table_any(url, table_selector=None, debug_prefix=None):
    try:
        tables = pd.read_html(url, flavor="lxml")
        if tables:
            if debug_prefix: save_debug(f"{debug_prefix}_pandas_cols.txt", str(tables[0].columns))
            return tables[0]
    except Exception as e:
        if debug_prefix: save_debug(f"{debug_prefix}_pandas_error.txt", repr(e))
    html = fetch_html(url)
    if debug_prefix: save_debug(f"{debug_prefix}_page.html", html[:200000])
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
    # tolerante a variantes mal codificadas de tildes (CategorÃ­a, MÃ¡ximo, etc.)
    norm_map = {unidecode(str(c)).lower().strip(): c for c in cols}
    raw_map = {str(c).strip(): c for c in cols}
    for cand in cands:
        key = unidecode(cand).lower().strip()
        # 1) por unidecode lowercase contiene
        for k, original in norm_map.items():
            if key in k:
                return original
        # 2) por coincidencia cruda (para casos como "CategorÃ­a", "MÃ¡ximo", "MÃ­nimo", "Promedios")
        for k, original in raw_map.items():
            if cand.lower() in k.lower():
                return original
    return None

# ===== Aliases =====
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

def norm_cat(raw):
    if not raw: return ""
    original = str(raw).strip()
    nb = _norm_basic(original)
    for alias, target in ALIASES.items():
        if _norm_basic(alias) == nb:
            return target
    # Ovinos
    if nb.startswith("corderos y corderas"): return "Corderos y Corderas"
    if nb.startswith("borregos"): return "Borregos"
    if nb.startswith("oveja de cria") or nb.startswith("oveja de cria 2 o") or ("enc" in nb and "oveja" in nb):
        return "Oveja De Cría 2 O + Enc."
    # Holando
    if "holando" in nb or "cruza ho" in nb:
        return "Holando y Cruza Ho"
    # Mixtos
    if re.search(r"\bterneros?\s*[/y]\s*terneras?\b", nb) or nb.startswith("mixtos"):
        return "Terneros / Terneras"
    # Vientres/Vacas preñadas
    if "vientres pren" in nb or "vacas pren" in nb or "vaca pren" in nb:
        return "Vientres Preñados"
    # Piezas de cría
    if re.search(r"\bpiezas?\s+de\s+cria\b", nb):
        return "Piezas de cría"
    # Vacas de Invernada
    if "vacas de invernada" in nb or "vaca de invernada" in nb:
        return "Vacas de Invernada"
    # Terneros (machos) by peso
    if nb.startswith("terneros") and "terneras" not in nb and not re.search(r"\bterneros\s*[/y]\s*terneras\b", nb):
        if re.search(r"(hasta|menos|<|\-\s*140|140\s*kg|^terneros\s*-?\s*140)", nb) and not re.search(r"(180)", nb):
            return "Terneros hasta 140kg"
        if re.search(r"(140.*180|entre 140 y 180|141 a 180|140-180)", nb):
            return "Terneros entre 140 y 180kg"
        if re.search(r"(\+\s*180|mas de 180|> *180|\- 180)", nb):
            return "Terneros mas de 180kg"
        return "Terneros"
    # Terneras by peso
    if nb.startswith("terneras"):
        if re.search(r"(hasta|menos|<|\-\s*140|140\s*kg)", nb) and not re.search(r"(180)", nb):
            return "Terneras hasta 140kg"
        if re.search(r"(140.*180|entre 140 y 180|141 a 180|140-180)", nb):
            return "Terneras entre 140 y 180kg"
        if re.search(r"(\+\s*140|mas de 140|> *140)", nb):
            return "Terneras mas de 140kg"
        return "Terneras"
    # Novillos by edad
    if nb.startswith("novillos") or nb.startswith("novillo "):
        if re.search(r"(1\s*(a|–|-)\s*2|1 a 2)", nb):
            return "Novillos 1 a 2 años"
        if re.search(r"(2\s*(a|–|-)\s*3|de 2 a 3|2 a 3)", nb) or "mas de 2" in nb:
            return "Novillos 2 a 3 años"
        if re.search(r"(\+\s*3|mas de 3|> *3)", nb):
            return "Novillos mas de 3 años"
        return "Novillos 1 a 2 años"
    # Vaquillonas
    if nb.startswith("vaquillonas"):
        if "sin servicio" in nb: return "Vaquillonas sin servicio"
        if "entorad" in nb: return "Vaquillonas entoradas"
        if "pren" in nb: return "Vaquillonas preñadas"
        if re.search(r"(1\s*(a|–|-)\s*2|1 a 2)", nb):
            return "Vaquillonas de 1 a 2 años"
        if re.search(r"(\+\s*2|mas de 2|> *2)", nb):
            return "Vaquillonas mas de 2 años"
        return "Vaquillonas de 1 a 2 años"
    # ACG gordo
    if "novillo gordo" in nb and "(acg)" in nb: return "Novillo gordo (ACG)"
    if "vaca gorda" in nb and "(acg)" in nb: return "Vaca gorda (ACG)"
    if "vaquillona gorda" in nb and "(acg)" in nb: return "Vaquillona gorda (ACG)"
    return original.strip().title()

# ====== SOURCES ======

def plaza_rural():
    url = "https://plazarural.com.uy/promedios"
    df = read_table_any(url, debug_prefix="plazarural")
    fecha = None
    try:
        html = fetch_html(url)
        save_debug("plazarural_page_full.html", html[:200000])
        m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", html)
        if m:
            d,mn,y = m.group(1).split("/")
            y = y if len(y)==4 else ("20"+y)
            fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    except Exception as e:
        save_debug("plazarural_date_error.txt", repr(e))
    df.columns = [str(c).strip() for c in df.columns]
    c_cat = pick_col(df.columns, "categoria","categoría","CategorÃ­a")
    c_max = pick_col(df.columns, "max","máx","maximo","máximo","MÃ¡ximo")
    c_min = pick_col(df.columns, "min","mín","minimo","mínimo","MÃ­nimo")
    c_prom= pick_col(df.columns, "prom","promedio","Promedios")
    c_pb  = pick_col(df.columns, "prom bulto","prom. bulto","pb")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get(c_cat,""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)),
                     "max": to_float(r.get(c_max)),
                     "min": to_float(r.get(c_min)),
                     "prom_bulto": to_float(r.get(c_pb))}
    save_debug("plazarural_rows_count.txt", str(len(rows)))
    return url, rows, fecha

def lote21():
    urls = [
        "https://www.lote21.uy/promedios.asp",
        "https://lote21.uy/promedios.asp",
        "http://www.lote21.uy/promedios.asp",
        "http://lote21.uy/promedios.asp",
    ]
    s = requests.Session(); s.headers.update(UA)
    html, ok_url = None, None
    for u in urls:
        try:
            r = s.get(u, timeout=60, allow_redirects=True)
            enc = (r.encoding or "").lower()
            if enc in ("iso-8859-1","latin-1","windows-1252"):
                r.encoding = enc
            elif r.apparent_encoding:
                r.encoding = r.apparent_encoding
            r.raise_for_status()
            html = r.text; ok_url = u; break
        except Exception:
            continue
    if html is None:
        raise RuntimeError("Lote21: no pude descargar la página")
    save_debug("lote21_response_url.txt", ok_url or "")
    save_debug("lote21_page.html", html[:250000])
    m = re.search(r"(?:Actualizado|Subasta|Remate)?[^\\d]{0,20}(\\d{1,2}/\\d{1,2}/\\d{2,4})", html, flags=re.I)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    cands = []
    try:
        for t in pd.read_html(html, flavor="lxml"):
            cands.append(t)
    except Exception as e:
        save_debug("lote21_pandas_error.txt", repr(e))
    soup = BeautifulSoup(html, "lxml")
    for t in soup.find_all("table"):
        try:
            cands.append(pd.read_html(str(t))[0])
        except Exception:
            pass
    save_debug("lote21_tables_found.txt", str(len(cands)))
    if not cands:
        raise RuntimeError("Lote21: no hay tablas")
    def score_df(df):
        d = df.dropna(how="all")
        d = d.loc[:, ~d.columns.duplicated()]
        d.columns = [str(c).strip() for c in d.columns]
        cols = [unidecode(str(c)).lower() for c in d.columns]
        score = 0
        if any(("cat" in c or "ategor" in c) for c in cols): score += 2
        if any(("prom" in c) or ("promedios" in c) for c in cols): score += 2
        if any(("max" in c or "máx" in c or "mÃ¡ximo" in c) for c in cols): score += 1
        if any(("min" in c or "mín" in c or "mÃ­nimo" in c) for c in cols): score += 1
        return score, d
    scored = [score_df(t) for t in cands]
    scored.sort(key=lambda x: (-x[0], -len(x[1])))
    df = scored[0][1]
    if df.shape[0] > 1 and not any(isinstance(x, str) for x in df.columns):
        first = df.iloc[0].astype(str).str.lower().tolist()
        if any("cat" in s or "prom" in s for s in first):
            df.columns = df.iloc[0].tolist()
            df = df.iloc[1:]
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    save_debug("lote21_table_preview.csv", df.head(60).to_csv(index=False))
    # Column picks con variantes mal codificadas
    c_cat  = pick_col(df.columns, "categoria","categoría","CategorÃ­a","rubro","lote","clase")
    c_prom = pick_col(df.columns, "promedio","prom","Promedios")
    c_max  = pick_col(df.columns, "maximo","máximo","max","MÃ¡ximo")
    c_min  = pick_col(df.columns, "minimo","mínimo","min","MÃ­nimo")
    if not c_cat and not c_prom and df.shape[1] >= 4:
        cols = list(df.columns)
        c_cat, c_prom, c_max, c_min = cols[0], cols[1], cols[2], cols[3]
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get(c_cat, ""))
        if not cat or str(cat).strip("-— ") == "":
            continue
        prom = to_float(r.get(c_prom))
        maxv = to_float(r.get(c_max))
        minv = to_float(r.get(c_min))
        if prom is None and maxv is None and minv is None:
            continue
        rows[cat] = {"prom": prom, "max": maxv, "min": minv}
    save_debug("lote21_rows_count.txt", str(len(rows)))
    return urls[0], rows, fecha

def pantalla_uruguay():
    url = "https://www.pantallauruguay.com.uy/promedios/"
    df = read_table_any(url, debug_prefix="pantalla")
    html = fetch_html(url)
    save_debug("pantalla_page_full.html", html[:200000])
    m = re.search(r"(\\d{1,2}/\\d{1,2}/\\d{2,4})", html)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    df.columns = [str(c).strip() for c in df.columns]
    c_cat  = pick_col(df.columns, "categoria","categoría","CategorÃ­a")
    c_max  = pick_col(df.columns, "maximo","máximo","max","MÃ¡ximo")
    c_min  = pick_col(df.columns, "minimo","mínimo","min","MÃ­nimo")
    c_prom = pick_col(df.columns, "prom","promedio","Promedios")
    c_pb   = pick_col(df.columns, "prom bulto","prom. bulto","pb")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get(c_cat,""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)),
                     "max": to_float(r.get(c_max)),
                     "min": to_float(r.get(c_min)),
                     "prom_bulto": to_float(r.get(c_pb))}
    save_debug("pantalla_rows_count.txt", str(len(rows)))
    return url, rows, fecha

def acg():
    list_url = "https://acg.com.uy/?post_type=precio_semanal"
    html = fetch_html(list_url)
    soup = BeautifulSoup(html, "lxml")
    first = soup.select_one("article a")
    post_url = first.get("href") if first else list_url
    html_post = fetch_html(post_url)
    save_debug("acg_post_url.txt", post_url)
    save_debug("acg_post_page.html", html_post[:200000])
    m = re.search(r"(\\d{1,2}/\\d{1,2}/\\d{2,4})", html_post)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    def rex(label): 
        return re.search(rf"{label}.*?([\\d\\.,]+)", html_post, flags=re.I|re.S)
    rows = {}
    for etiqueta, cat in [("Novillo\\s*gordo","Novillo gordo (ACG)"),
                          ("Vaca\\s*gorda","Vaca gorda (ACG)"),
                          ("Vaquillona\\s*gorda","Vaquillona gorda (ACG)")]:
        m2 = rex(etiqueta)
        if m2:
            val = float(str(m2.group(1)).replace(".","").replace(",",".").strip())
            rows[cat] = {"prom": val, "ref": val}
    save_debug("acg_rows_count.txt", str(len(rows)))
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
            time.sleep(0.8)
        except Exception as e:
            data["fuentes"][key] = {"url": None, "error": str(e)}
    # Orden canónico: mismo que en el front
    CANONICAL = [
        "Corderos y Corderas","Borregos","Oveja De Cría 2 O + Enc.",
        "Terneros hasta 140kg","Terneros entre 140 y 180kg","Terneros mas de 180kg","Terneros",
        "Novillos 1 a 2 años","Novillos 2 a 3 años","Novillos mas de 3 años",
        "Holando y Cruza Ho","Terneros / Terneras",
        "Terneras","Terneras hasta 140kg","Terneras entre 140 y 180kg","Terneras mas de 140kg",
        "Vaquillonas de 1 a 2 años","Vaquillonas mas de 2 años","Vaquillonas sin servicio","Vaquillonas entoradas","Vaquillonas preñadas",
        "Vientres Preñados","Piezas de cría","Vacas de Invernada",
        "Novillo gordo (ACG)","Vaca gorda (ACG)","Vaquillona gorda (ACG)",
    ]
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
