## 🦇 NYS GBIF Endangered – Marbletown Explorer

Welcome to your tiny biodiversity command center! This project fetches GBIF occurrence records for Marbletown, NY using the official administrative polygon, enriches them with New York Natural Heritage Program conservation status, and lets you explore the results on a friendly Streamlit map. 🗺️🌿

### ✨ What’s Inside
- 🔺 **Precise geometry**: Marbletown boundary pulled from OpenStreetMap/Nominatim (cached locally to play nice with rate limits).
- 🐾 **Targeted species**: GBIF occurrences filtered to IUCN categories CR, EN, VU, and NT.
- 🧩 **Status mash‑up**: NYNHP 2025 status list merged in via Polars, complete with a `has_nynhp_status` flag.
- 📦 **Ready to explore**: Parquet dataset + Streamlit dashboard to slice, dice, and map everything.

### 🚀 Quick Start
Make sure you have [uv](https://github.com/astral-sh/uv) installed, then:

```bash
# create/update the virtual environment and install deps
uv sync
```

### 🧪 Generate the Dataset
```bash
make data
```
This runs `uv run python main.py`, which will:
1. Fetch (or load from cache) the Marbletown polygon.
2. Pull GBIF occurrences for the polygon, paging over IUCN threat statuses.
3. Merge NYNHP conservation ranks.
4. Write `data/marbletown_gbif_occurrences.parquet`.

### 🌈 Launch the Streamlit App
```bash
make streamlit
```
Then open the printed URL (default http://localhost:8501) and:
- Toggle the “Only NYS conservation status” checkbox.
- Filter by specific state conservation ranks.
- Highlight “Species of greatest conservation need.”
- Pan/zoom the map and hover points for quick species facts.

### 📂 Key Files
- `main.py` – Marbletown ETL (Nominatim → GBIF → Polars → Parquet).
- `streamlit_app.py` – Interactive map + table explorer.
- `data/nynhp-status-list_2025-10-29.csv` – NYNHP reference data (already included).
- `data/marbletown_gbif_occurrences.parquet` – Generated dataset (git-ignored by default).

### 🤝 Helpful Tips
- Respect API limits: repeated GBIF calls are automatically retried with exponential backoff, and the Nominatim query is memoized on disk.
- Need fresh data? Delete `.cache/nominatim` and re-run `make data`.
- Want Jupyter too? `make jupyter` opens a lab session inside the uv environment.

Happy exploring! 🧭 Let me know if you want to scale this up to more towns or add extra filters. 🐢
