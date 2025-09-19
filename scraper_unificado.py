# -*- coding: utf-8 -*-
import re, json, os, time
from datetime import datetime
import pandas as pd, requests
from bs4 import BeautifulSoup
from unidecode import unidecode

OUT = "unified.json"
UA = {"User-Agent":"Mozilla/5.0"}

def fetch_html(url): r=requests.get(url,headers=UA,timeout=60); r.raise_for_status(); return r.text
def read_table_any(url): return pd.read_html(url,flavor="lxml")[0]

def to_float(x):
    try:
        if x is None: return None
        if isinstance(x,(int,float)): return float(x)
        s=str(x).replace(".","").replace(",","."); return float(re.sub(r"[^\d\.\-]","",s))
    except: return None

def norm_cat(raw):
    if not raw: return ""
    s=unidecode(str(raw)).lower().strip()
    m={}
    with open("categories_aliases.json","r",encoding="utf-8") as f: m=json.load(f)
    for a,t in m.items():
        if unidecode(a).lower().strip()==s: return t
    return raw.strip()

CANONICAL=[
 "Corderos y Corderas","Borregos","Oveja De Cría 2 O + Enc.","Terneros hasta 140kg",
 "Terneros mas de 180kg","Terneros","Novillos 1 a 2 años","Novillos 2 a 3 alos","Novillos mas de 3 años",
 "Holando y Cruza Ho","Terneras","Terneras hasta 140kg","Terneras mas de 140kg","Terneras entre 140 y 180kg",
 "Terneros / Terneras","Vaquillonas de 1 a 2 años","Vaquillonas mas de 2 años","Vaquillonas sin servicio",
 "Vaquillonas entoradas","Vaquillonas preñadas","Vientres Preñados","Vacas de Invernada"
]

def dummy_scraper(): return "url",{}, "2025-09-19"

def main():
    data={"last_updated_utc":datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),"fuentes":{},"categorias":{}}
    funcs={"plaza_rural":dummy_scraper,"lote21":dummy_scraper,"pantalla_uruguay":dummy_scraper,"acg":dummy_scraper}
    all_rows={}
    for k,fn in funcs.items():
        url,rows,fecha=fn(); data["fuentes"][k]={"url":url,"fecha":fecha}
        for c,v in rows.items(): all_rows.setdefault(c,{})[k]=v
    ordered={}
    for c in CANONICAL:
        if c in all_rows: ordered[c]=all_rows[c]
    for c in all_rows:
        if c not in ordered: ordered[c]=all_rows[c]
    data["categorias"]=ordered
    with open(OUT,"w",encoding="utf-8") as f: json.dump(data,f,ensure_ascii=False,indent=2)

if __name__=="__main__": main()
