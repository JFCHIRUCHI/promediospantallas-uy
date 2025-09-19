# -*- coding: utf-8 -*-
"""
Scraper unificado: genera unified.json con categorías comunes y columnas por fuente.
Ejecutar en tu entorno local o vía GitHub Actions (ver .github/workflows/update.yml).
Requisitos: pandas, lxml, beautifulsoup4, requests, unidecode
"""
import pandas as pd, requests, json, re, time
from bs4 import BeautifulSoup
from datetime import datetime
from unidecode import unidecode

OUT = "unified.json"

def fetch_html(url):
    r = requests.get(url, timeout=60, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def read_table_any(url, table_selector=None):
    # 1) Primero intentamos pandas.read_html
    try:
        tables = pd.read_html(url, flavor="lxml")
        if len(tables): return tables[0]
    except Exception:
        pass
    # 2) BeautifulSoup + primer <table> o selector
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one(table_selector or "table")
    if table is None:
        raise RuntimeError(f"No se encontró tabla en {url}")
    return pd.read_html(str(table))[0]

def norm_cat(s):
    if not isinstance(s, str): s = str(s)
    s = unidecode(s.lower().strip())
    # limpiar ruido tipo espacios extras
    s = re.sub(r"\s+", " ", s)
    # reglas de mapeo básicas
    rules = [
        (r"^terneros?(/as)?(.*)?$", "Terneros"),
        (r"^novillos? 1.?-.?2 anos(.*)?$", "Novillos 1-2 años"),
        (r"^novillos? 1 a 2 anos(.*)?$", "Novillos 1-2 años"),
        (r"^vacas? de invernada(.*)?$", "Vacas de invernada"),
        (r"^novillo gordo(.*)?$", "Novillo gordo (ACG)"),
        (r"^vaca gorda(.*)?$", "Vaca gorda (ACG)"),
        (r"^vaquillona gorda(.*)?$", "Vaquillona gorda (ACG)"),
    ]
    for pat, target in rules:
        if re.match(pat, s): return target
    # fallback: capitalizar
    return s.title()

def to_float(x):
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    # variantes con . de miles y , decimal
    s = s.replace(".", "").replace(",", ".")
    # quitar símbolos
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s)
    except:
        return None

def plaza_rural():
    url = "https://plazarural.com.uy/promedios"
    df = read_table_any(url)
    # normalizar encabezados
    df.columns = [unidecode(str(c)).lower().strip() for c in df.columns]
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get("categoria", r.get("categoría","")))
        rows[cat] = {
            "prom": to_float(r.get("prom", "")),
            "max": to_float(r.get("max", r.get("máx",""))),
            "min": to_float(r.get("min", r.get("mín",""))),
            "prom_bulto": to_float(r.get("prom bulto", r.get("prom. bulto",""))),
        }
    return url, rows

def lote21():
    url = "https://www.lote21.uy/promedios.asp"
    df = read_table_any(url)
    df.columns = [unidecode(str(c)).lower().strip() for c in df.columns]
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get("categoria", r.get("categoría","")))
        rows[cat] = {
            "prom": to_float(r.get("promedio", r.get("prom",""))),
            "max": to_float(r.get("maximo", r.get("máximo",""))),
            "min": to_float(r.get("minimo", r.get("mínimo",""))),
        }
    return url, rows

def pantalla_uruguay():
    url = "https://www.pantallauruguay.com.uy/promedios/"
    df = read_table_any(url)
    df.columns = [unidecode(str(c)).lower().strip() for c in df.columns]
    rows = {}
    for _, r in df.iterrows():
        cat = norm_cat(r.get("categoria", r.get("categoría","")))
        rows[cat] = {
            "prom": to_float(r.get("prom", r.get("prom.", ""))),
            "max": to_float(r.get("max", r.get("máximo",""))),
            "min": to_float(r.get("min", r.get("mínimo",""))),
            "prom_bulto": to_float(r.get("prom bulto", r.get("prom. bulto",""))),
        }
    return url, rows

def acg():
    # ACG publica referencias por categoría; aquí tomamos los últimos valores de la página índice.
    # Según el sitio puede requerir navegar a la última "semana" y parsear. Dejamos un parser simple
    # que busca números en tarjeta de "Novillo", "Vaca", "Vaquillona". Si falla, deja None.
    url = "https://acg.com.uy/?post_type=precio_semanal"
    html = fetch_html(url)
    rows = {}
    # heurística básica
    blocks = re.findall(r"(Novillo|Vaca|Vaquillona)[^<]{0,80}</a>.*?([\d\.,]+)", html, flags=re.I|re.S)
    # mapear
    name_map = {
        "novillo":"Novillo gordo (ACG)",
        "vaca":"Vaca gorda (ACG)",
        "vaquillona":"Vaquillona gorda (ACG)",
    }
    for k,v in blocks:
        cat = name_map.get(k.lower())
        if not cat: continue
        val = to_float(v)
        rows[cat] = {"prom": val, "ref": val}
    return url, rows

def main():
    data = {
        "last_updated_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fuentes": {},
        "categorias": {}
    }
    funcs = {
        "plaza_rural": plaza_rural,
        "lote21": lote21,
        "pantalla_uruguay": pantalla_uruguay,
        "acg": acg
    }
    all_rows = {}
    for key, fn in funcs.items():
        try:
            url, rows = fn()
            data["fuentes"][key] = {"url": url}
            for cat, vals in rows.items():
                all_rows.setdefault(cat, {})
                all_rows[cat][key] = vals
            time.sleep(1.5)  # ser amable con los servidores
        except Exception as e:
            data["fuentes"][key] = {"url": None, "error": str(e)}

    data["categorias"] = all_rows
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Generado {OUT} con {len(all_rows)} categorías")

if __name__ == "__main__":
    main()
