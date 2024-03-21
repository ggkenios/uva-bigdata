import streamlit as st

from app.utils.main import get_image


URL = "https://media.licdn.com/dms/image/C4E0BAQGlGcOme_1tLw/company-logo_200_200/0/1630626894783/university_of_amsterdam_business_school_logo?e=1718841600&v=beta&t=ev4iDCd0KpeGm2e5WgLKlLmh4HKxIRtl4wGfN2FgQ2c"

def home_page():
    st.title('UvA - Big Data Project')
    # Add a nice image
    st.image(get_image(URL))

    st.write("**Author**")
    st.write("Georgios Gkenios")


if __name__ == "__main__":
    home_page()
