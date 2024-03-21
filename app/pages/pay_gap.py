import geopandas as gpd
import json
import plotly.express as px
import plotly.io as pio
import streamlit as st

from app.data import df, unique_years


def pay_gap_page():
    st.title("Pay Gap Rate by Country")

    # Set user defined year to session state
    if "year" not in st.session_state:
        st.session_state.year = df["year"].max()
    year = st.session_state.year

    pio.templates.default = "plotly"
    # Load world GeoJSON data
    geojson = json.load(open("app/data/europe.geojson"))

    # Dictionary to GeoDataFrame
    geo_df = gpd.GeoDataFrame.from_features(geojson["features"])
    geo_df["country"] = geo_df ["ISO2"]

    # Filter data by year
    df_last_year = df[df["year"] == year]

    # Merge data with GeoJSON using ISO_A2 code
    geo_df = geo_df.merge(df_last_year, on="country").set_index("country")

    # Plot
    fig = px.choropleth(
        geo_df,
        geojson=geo_df.geometry,
        locations=geo_df.index,
        color="pay_gap_rate",
        range_color=(geo_df["pay_gap_rate"].min(), geo_df["pay_gap_rate"].max()),
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig, use_container_width=True)

    # Add a selectbox for the user to select a year
    col = st.columns(1)
    with col[0]:
        year = st.selectbox("Year", options=unique_years)
        st.session_state.year = year


if __name__ == "__main__":
    pay_gap_page()
