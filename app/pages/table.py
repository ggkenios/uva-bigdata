import streamlit as st

from app.data import df


def table_page():
    st.title('Table')
    st.write(df)


if __name__ == "__main__":
    table_page()
