# -*- coding: utf-8 -*-
import re, json, os, time
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

OUT = "unified.json"
UA = {"User-Agent":"Mozilla/5.0"}

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

# CANONICAL stays in block order (front already enforces too)
CANONICAL = [
    # Ovinos
    "Corderos y Corderas","Borregos","Oveja De Cría 2 O + Enc.",
    # Terneros (Machos)
    "Terneros hasta 140kg","Terneros entre 140 y 180kg","Terneros mas de 180kg","Terneros",
    # Novillos
    "Novillos 1 a 2 años","Novillos 2 a 3 años","Novillos mas de 3 años",
    # Holando
    "Holando y Cruza Ho",
    # Mixtos
    "Terneros / Terneras",
    # Terneras
    "Terneras","Terneras hasta 140kg","Terneras entre 140 y 180kg","Terneras mas de 140kg",
    # Vaquillonas
    "Vaquillonas de 1 a 2 años","Vaquillonas mas de 2 años","Vaquillonas sin servicio","Vaquillonas entoradas","Vaquillonas preñadas",
    # Vientres/Vacas preñadas
    "Vientres Preñados",
    # Pieza de cría
    "Piezas de cría",
    # Vacas de Invernada
    "Vacas de Invernada",
    # ACG (fuera del cuadro principal)
    "Novillo gordo (ACG)","Vaca gorda (ACG)","Vaquillona gorda (ACG)",
]

ALIASES_FILE = "categories_aliases.json"
ALIASES = {}
if os.path.exists(ALIASES_FILE):
    try:
        with open(ALIASES_FILE, "r", encoding="utf-8") as f:
            ALIASES = json.load(f)
    except Exception:
        ALIASES = {}

def _norm_basic(s: str) -> str:
    """Lower, unidecode, collapse spaces."""
    s = unidecode(str(s or "")).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_cat(raw):
    """Map raw category text to canonical label using:
    1) explicit aliases,
    2) rule-based normalization for ages/pesos/estados (regex).
    """
    if not raw: return ""
    original = str(raw).strip()
    nb = _norm_basic(original)
    # 1) direct alias match
    for alias, target in ALIASES.items():
        if _norm_basic(alias) == nb:
            return target

    # ----- RULES BY FAMILY -----
    # Ovinos
    if nb.startswith("corderos y corderas"): return "Corderos y Corderas"
    if nb.startswith("borregos"): return "Borregos"
    if nb.startswith("oveja de cria") or nb.startswith("oveja de cria 2 o") or "enc" in nb and "oveja" in nb:
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

    # --- TERNEROS (MACHOS) by peso ---
    if nb.startswith("terneros") and "terneras" not in nb and not re.search(r"\bterneros\s*[/y]\s*terneras\b", nb):
        # hasta / menos 140
        if re.search(r"(hasta|menos|<|\-\s*140|140\s*kg|^terneros\s*-?\s*140)", nb) and not re.search(r"(180)", nb):
            return "Terneros hasta 140kg"
        # entre 140 y 180 (141 a 180 etc.)
        if re.search(r"(140.*180|entre 140 y 180|141 a 180|140-180)", nb):
            return "Terneros entre 140 y 180kg"
        # mas de 180 / +180
        if re.search(r"(\+\s*180|mas de 180|> *180|\- 180)", nb):
            return "Terneros mas de 180kg"
        # generales
        return "Terneros"

    # --- TERNERAS by peso ---
    if nb.startswith("terneras"):
        if re.search(r"(hasta|menos|<|\-\s*140|140\s*kg)", nb) and not re.search(r"(180)", nb):
            return "Terneras hasta 140kg"
        if re.search(r"(140.*180|entre 140 y 180|141 a 180|140-180)", nb):
            return "Terneras entre 140 y 180kg"
        if re.search(r"(\+\s*140|mas de 140|> *140)", nb):
            return "Terneras mas de 140kg"
        return "Terneras"

    # --- NOVILLOS by edad ---
    if nb.startswith("novillos") or nb.startswith("novillo "):
        if re.search(r"(1\s*(a|–|-)\s*2|1 a 2)", nb):
            return "Novillos 1 a 2 años"
        if re.search(r"(2\s*(a|–|-)\s*3|de 2 a 3|2 a 3)", nb) or "mas de 2" in nb:
            return "Novillos 2 a 3 años"
        if re.search(r"(\+\s*3|mas de 3|> *3)", nb):
            return "Novillos mas de 3 años"
        # si dice "mas de 2 años" y no menciona 3, lo mapeamos a 2-3 por defecto
        if "mas de 2" in nb:
            return "Novillos 2 a 3 años"
        return "Novillos 1 a 2 años"

    # --- VAQUILLONAS by edad/estado ---
    if nb.startswith("vaquillonas"):
        if "sin servicio" in nb: return "Vaquillonas sin servicio"
        if "entorad" in nb: return "Vaquillonas entoradas"
        if "pren" in nb: return "Vaquillonas preñadas"
        if re.search(r"(1\s*(a|–|-)\s*2|1 a 2)", nb):
            return "Vaquillonas de 1 a 2 años"
        if re.search(r"(\+\s*2|mas de 2|> *2)", nb):
            return "Vaquillonas mas de 2 años"
        # default
        return "Vaquillonas de 1 a 2 años"

    # --- ACG gordo ---
    if "novillo gordo" in nb and "(acg)" in nb: return "Novillo gordo (ACG)"
    if "vaca gorda" in nb and "(acg)" in nb: return "Vaca gorda (ACG)"
    if "vaquillona gorda" in nb and "(acg)" in nb: return "Vaquillona gorda (ACG)"

    # fallback: title-case original
    return original.strip().title()

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
        cat = norm_cat(r.get(c_cat,""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)),
                     "max": to_float(r.get(c_max)),
                     "min": to_float(r.get(c_min)),
                     "prom_bulto": to_float(r.get(c_pb))}
    return url, rows, fecha

def lote21():
    url = "https://www.lote21.uy/promedios.asp"
    html = fetch_html(url)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", html)
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
        c_cat  = pick_col(cols,"categoria","categoría")
        c_max  = pick_col(cols,"maximo","máximo","max")
        c_min  = pick_col(cols,"minimo","mínimo","min")
        c_prom = pick_col(cols,"promedio","prom")
        if c_cat and c_prom:
            df = t; break
    if df is None:
        df = read_table_any(url)
        df.columns = [str(c).strip() for c in df.columns]
        c_cat  = pick_col(df.columns,"categoria","categoría")
        c_max  = pick_col(df.columns,"maximo","máximo","max")
        c_min  = pick_col(df.columns,"minimo","mínimo","min")
        c_prom = pick_col(df.columns,"promedio","prom")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get(c_cat,""))
        if not cat: continue
        rows[cat] = {"prom": to_float(r.get(c_prom)),
                     "max": to_float(r.get(c_max)),
                     "min": to_float(r.get(c_min))}
    return url, rows, fecha

def pantalla_uruguay():
    url = "https://www.pantallauruguay.com.uy/promedios/"
    html = fetch_html(url)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", html)
    fecha = None
    if m:
        d,mn,y = m.group(1).split("/")
        y = y if len(y)==4 else ("20"+y)
        fecha = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    df = read_table_any(url)
    df.columns = [str(c).strip() for c in df.columns]
    c_cat  = pick_col(df.columns,"categoria","categoría")
    c_max  = pick_col(df.columns,"maximo","máximo","max")
    c_min  = pick_col(df.columns,"minimo","mínimo","min")
    c_prom = pick_col(df.columns,"prom","promedio")
    c_pb   = pick_col(df.columns,"prom bulto","prom. bulto","pb")
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get(c_cat,""))
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
            val = to_float(m2.group(1))
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
    # order according to CANONICAL first, then rest
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
