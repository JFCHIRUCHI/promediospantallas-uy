# Promedios Ganaderos — Unificado (Starter listo)

## Publicación rápida (GitHub Pages)
1. Crea un repositorio vacío en GitHub.
2. Sube **todo** el contenido de esta carpeta (incluida `.github/workflows/`).
3. Ve a *Settings → Pages* y elige **Deploy from branch** (main, root `/`). Guarda.
4. Abre la URL que te da GitHub Pages.

## Actualización automática
- El workflow `.github/workflows/update.yml` corre todos los días 10:00 UTC (~07:00 Uruguay).
- Ejecuta `scraper_unificado.py` y actualiza `unified.json` con los últimos promedios.

## Requisitos locales (opcional)
```
pip install -r requirements.txt
python scraper_unificado.py
```

## Personalización
- Ajusta las reglas de categorías en `scraper_unificado.py` (función `norm_cat`).
- Cambia estilos en `styles.css`.