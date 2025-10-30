.PHONY: jupyter
jupyter:
	uv run -- jupyter lab --no-browser

.PHONY: data
data:
	uv run python main.py

.PHONY: streamlit
streamlit:
	uv run streamlit run streamlit_app.py
