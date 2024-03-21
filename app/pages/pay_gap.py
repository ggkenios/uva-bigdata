import geopandas as gpd
import json
import plotly.express as px
import plotly.io as pio
import streamlit as st

from app.data import df


WIDTH = 900
HEIGHT = 800


def pay_gap_page():
    st.title("Pay Gap Rate by Country")

    pio.templates.default = "plotly"
    # Load world GeoJSON data
    geojson = json.load(open("app/data/europe.geojson"))

    # Dictionary to GeoDataFrame
    geo_df = gpd.GeoDataFrame.from_features(geojson["features"])
    geo_df["country"] = geo_df ["ISO2"]

    # Merge data with GeoJSON using ISO_A2 code
    geo_df = geo_df.merge(df, on="country").set_index("country")

    # Plot
    fig = px.choropleth(
        geo_df,
        geojson=geo_df.geometry,
        locations=geo_df.index,
        color="pay_gap_rate",
        range_color=(geo_df["pay_gap_rate"].min(), geo_df["pay_gap_rate"].max()),
        height=HEIGHT,
        width=WIDTH,
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(width=WIDTH, height=HEIGHT)
    st.plotly_chart(fig, use_container_width=True) #  width=WIDTH, height=HEIGHT)


if __name__ == "__main__":
    pay_gap_page()
