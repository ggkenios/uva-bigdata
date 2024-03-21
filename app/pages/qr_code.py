import streamlit as st


def qr_code_page():
    st.title("QR Code")
    st.image("app/data/qr_code.png", use_column_width=True)


if __name__ == "__main__":
    qr_code_page()
