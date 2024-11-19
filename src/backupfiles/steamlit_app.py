

import streamlit as st
from src.backupfiles import create_csv, pyspark_codesnippet


def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select a page", ["Home", "Create CSV", "Create CSV and PySpark Code Snippet"])

    if page == "Home":
        st.title("Home")
        st.write("Select a page from the dropdown menu to get started.")
    elif page == "Create CSV":
        create_csv.show_page()
    elif page == "Create CSV and PySpark Code Snippet":
        pyspark_codesnippet.py.show_page()

if __name__ == "__main__":
    main()