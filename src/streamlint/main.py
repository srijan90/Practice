import streamlit as st
from pages import create_csv_page, create_csv_pyspark_page

def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select a page", ["Home", "Create CSV", "Create CSV and PySpark Code Snippet"])

    if page == "Home":
        st.title("Home")
        st.write("Welcome to the app! Use the sidebar to navigate to different pages.")
        st.write("Go to Create CSV")
        st.write("Go to Create CSV and PySpark Code Snippet")
    elif page == "Create CSV":
        create_csv_page.show_page()
    elif page == "Create CSV and PySpark Code Snippet":
        create_csv_pyspark_page.show_page()

if __name__ == "__main__":
    main()