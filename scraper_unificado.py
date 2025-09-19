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
    s = str(x).strip()
    s = s.replace("\u00a0"," ")
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s)
    except:
        return None

# --- Nueva normalización robusta ---
CANONICAL = [
    "Terneros","Novillos 1-2 años","Novillos 2-3 años","Novillos +3 años",
    "Vacas de invernada","Novillos y Vaquillonas 1-2 años","Holandos","Borregos",
    "Corderos y Corderas","Oveja de cría 2 o + enc.",
    "Novillo gordo (ACG)","Vaca gorda (ACG)","Vaquillona gorda (ACG)",
]
RULES = [
    (r"^terner", "Terneros"),
    (r"^novill[oa]s?.*1.*2.*ano", "Novillos 1-2 años"),
    (r"^novill[oa]s?.*2.*3.*ano", "Novillos 2-3 años"),
    (r"^novill[oa]s?.*(\+|mas.*3)", "Novillos +3 años"),
    (r"^novill[oa]s?.*vaquillon", "Novillos y Vaquillonas 1-2 años"),
    (r"^vacas?.*invernada", "Vacas de invernada"),
    (r"^holand", "Holandos"),
    (r"^borreg", "Borregos"),
    (r"^corderos?.*corderas?", "Corderos y Corderas"),
    (r"^oveja.*cr[ií]a.*", "Oveja de cría 2 o + enc."),
    (r"^novillo.*gordo", "Novillo gordo (ACG)"),
    (r"^vaca.*gorda", "Vaca gorda (ACG)"),
    (r"^vaquillona.*gorda", "Vaquillona gorda (ACG)"),
]
ALIASES_FILE = "categories_aliases.json"
ALIASES = {}
if os.path.exists(ALIASES_FILE):
    try:
        with open(ALIASES_FILE, "r", encoding="utf-8") as f:
            ALIASES = json.load(f)
    except Exception:
        ALIASES = {}
UNMAPPED = set()
def norm_cat(raw):
    if raw is None: return ""
    original = str(raw).strip()
    if not original: return ""
    k = unidecode(original).lower().strip()
    for alias, target in ALIASES.items():
        if unidecode(alias).lower().strip() == k:
            return target
    s = unidecode(original).lower().strip()
    s = re.sub(r"\s+", " ", s)
    for pat, target in RULES:
        if re.search(pat, s, flags=re.I):
            return target
    UNMAPPED.add(original)
    return original.strip().title()

# --- resto de funciones (plaza_rural, lote21, pantalla_uruguay, acg) ---
# (por brevedad no repito todo aquí, usar la última versión que ya tienes implementada)
