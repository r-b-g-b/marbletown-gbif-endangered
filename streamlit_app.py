from __future__ import annotations

import polars as pl
import pydeck as pdk
import streamlit as st
from marbletown_pipeline import OUTPUT_PATH, ensure_dataset


MAP_INITIAL_VIEW = pdk.ViewState(
    latitude=41.85,
    longitude=-74.13,
    zoom=11,
    pitch=0,
)


@st.cache_data(show_spinner=False)
def load_occurrences() -> pl.DataFrame:
    ensure_dataset(verbose=False)
    return pl.read_parquet(OUTPUT_PATH)


def prepare_filters(df: pl.DataFrame) -> pl.DataFrame:
    st.sidebar.header("Filters")
    only_nys_status = st.sidebar.checkbox(
        "Only NYS conservation status records",
        value=True,
    )

    status_ranks = sorted(
        df.get_column("State conservation status rank")
        .drop_nulls()
        .unique()
        .to_list()
    )
    selected_ranks = st.sidebar.multiselect(
        "State conservation status rank",
        options=status_ranks,
        default=status_ranks,
    )

    species_of_greatest_need = st.sidebar.checkbox(
        "Species of greatest conservation need",
        value=False,
    )

    filtered = df
    if only_nys_status:
        filtered = filtered.filter(pl.col("has_nynhp_status"))
    if selected_ranks:
        rank_condition = pl.col("State conservation status rank").is_in(selected_ranks)
        if not only_nys_status:
            rank_condition = rank_condition | pl.col("State conservation status rank").is_null()
        filtered = filtered.filter(rank_condition)
    if species_of_greatest_need:
        filtered = filtered.filter(
            pl.col("Species of greatest conservation need")
            .fill_null("")
            .str.starts_with("Yes")
        )

    return filtered


def build_map_layer(df: pl.DataFrame) -> pdk.Layer:
    color_enriched = df.select(
        [
            pl.col("decimalLongitude").alias("lon"),
            pl.col("decimalLatitude").alias("lat"),
            "scientificName",
            "Primary common name",
            "iucnRedListCategory",
            "State conservation status rank",
            pl.when(pl.col("iucnRedListCategory") == "CR")
            .then(214)
            .when(pl.col("iucnRedListCategory") == "EN")
            .then(255)
            .when(pl.col("iucnRedListCategory") == "VU")
            .then(255)
            .otherwise(120)
            .alias("color_r"),
            pl.when(pl.col("iucnRedListCategory") == "CR")
            .then(39)
            .when(pl.col("iucnRedListCategory") == "EN")
            .then(127)
            .when(pl.col("iucnRedListCategory") == "VU")
            .then(215)
            .otherwise(120)
            .alias("color_g"),
            pl.when(pl.col("iucnRedListCategory") == "CR")
            .then(40)
            .when(pl.col("iucnRedListCategory") == "EN")
            .then(80)
            .when(pl.col("iucnRedListCategory") == "VU")
            .then(0)
            .otherwise(120)
            .alias("color_b"),
        ]
    )

    return pdk.Layer(
        "ScatterplotLayer",
        data=color_enriched.to_dicts(),
        get_position="[lon, lat]",
        get_radius=100,
        get_fill_color="[color_r, color_g, color_b, 160]",
        pickable=True,
    )


def main() -> None:
    st.set_page_config(page_title="Marbletown GBIF Occurrences", layout="wide")
    st.title("GBIF Occurrences in Marbletown, NY")

    try:
        df = load_occurrences()
    except Exception as exc:  # pylint: disable=broad-except
        st.error(
            "Unable to load the Marbletown dataset. "
            "If this app is running on Streamlit Cloud, make sure outbound network "
            "access is available for Nominatim and GBIF requests, or bundle the "
            "generated Parquet file with the deployment."
        )
        st.exception(exc)
        st.stop()
    filtered = prepare_filters(df)

    st.subheader("Summary")
    total_records = df.height
    filtered_records = filtered.height
    st.metric("Records in view", f"{filtered_records:,}")
    st.caption(f"Dataset total: {total_records:,} records")

    if filtered.is_empty():
        st.info("No occurrences match the current filters.")
        return

    map_layer = build_map_layer(filtered)
    deck = pdk.Deck(
        map_style=None,
        initial_view_state=MAP_INITIAL_VIEW,
        layers=[map_layer],
        tooltip={
            "html": "<b>{scientificName}</b><br/>"
            "Common name: {Primary common name}<br/>"
            "IUCN: {iucnRedListCategory}<br/>"
            "NYS status: {State conservation status rank}",
        },
    )

    st.pydeck_chart(deck)

    st.subheader("Records")
    st.dataframe(filtered.to_arrow(), hide_index=True)


if __name__ == "__main__":
    main()
