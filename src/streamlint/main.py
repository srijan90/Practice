import streamlit as st
from pages import create_csv_page,create_csv_pyspark_page

def show_home():
    st.title("Home")
    st.write("Welcome to the app! Use the sidebar to navigate to different pages.")
#
# def show_create_csv():
#     st.title("Create CSV")
#     st.write("This is the Create CSV page.")
#
# def show_create_csv_pyspark():
#     st.title("Create CSV and PySpark Code Snippet")
#     st.write("This is the Create CSV and PySpark Code Snippet page.")

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Home", "Create CSV", "Create CSV and PySpark Code Snippet"])

# Show the selected page
if page == "Home":
    show_home()
elif page == "Create CSV":
    # show_create_csv()
    create_csv_page.show_page()
elif page == "Create CSV and PySpark Code Snippet":
    create_csv_pyspark_page.show_page()
