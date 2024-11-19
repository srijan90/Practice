import streamlit as st
from pages import create_csv_page, create_csv_pyspark_page

def main():
    st.sidebar.title("Navigation")

    # Display links to each page directly in the sidebar
    if st.sidebar.button("Home"):
        st.session_state.page = "home"
    if st.sidebar.button("Create CSV"):
        st.session_state.page = "create_csv"
    if st.sidebar.button("Create CSV and PySpark Code Snippet"):
        st.session_state.page = "create_csv_and_pyspark_code_snippet"

    # Set default page if not already set
    if "page" not in st.session_state:
        st.session_state.page = "home"

    # Determine which page to show based on the session state
    if st.session_state.page == "home":
        st.title("Home")
        st.write("Welcome to the app! Use the sidebar to navigate to different pages.")
    elif st.session_state.page == "create_csv":
        create_csv_page.show_page()
    elif st.session_state.page == "create_csv_and_pyspark_code_snippet":
        create_csv_pyspark_page.show_page()

if __name__ == "__main__":
    main()