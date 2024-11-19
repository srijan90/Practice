import streamlit as st
import pandas as pd

def create_csv_page():
    st.title("Create CSV")

    # Default number of columns and rows
    default_num_columns = 2
    default_num_rows = 2

    # Input for number of columns and rows with unique keys
    num_columns = st.sidebar.number_input("Number of Columns", min_value=1, step=1, value=default_num_columns, key="num_columns_csv")
    num_rows = st.sidebar.number_input("Number of Rows", min_value=1, step=1, value=default_num_rows, key="num_rows_csv")

    # Create a table for input
    column_names = [f"Column {i+1}" for i in range(num_columns)]
    data = pd.DataFrame("", index=range(num_rows), columns=column_names)

    st.write("Enter your data:")
    edited_data = st.data_editor(data, key="data_editor_csv")

    # Generate CSV
    if st.button("Generate CSV"):
        csv_data = edited_data.to_csv(index=False, quoting=1)  # quoting=1 for QUOTE_NONNUMERIC
        st.download_button(label="Download CSV", data=csv_data, file_name="data.csv", mime="text/csv")

        st.write("CSV Data:")
        st.code(csv_data, language="csv")

def create_csv_pyspark_page():
    st.title("Create CSV and PySpark Code Snippet")

    # Input for app name
    app_name = st.text_input("App Name")

    # Default number of columns and rows
    default_num_columns = 2
    default_num_rows = 2

    # Input for number of columns and rows with unique keys
    num_columns = st.sidebar.number_input("Number of Columns", min_value=1, step=1, value=default_num_columns, key="num_columns_pyspark")
    num_rows = st.sidebar.number_input("Number of Rows", min_value=1, step=1, value=default_num_rows, key="num_rows_pyspark")

    # Table for column names
    st.write("Enter column names:")
    column_names = [st.text_input(f"Column {i+1} Name", value=f"Column {i+1}", key=f"col_name_pyspark_{i}") for i in range(num_columns)]

    # Table for data input
    st.write("Enter your data:")
    data = pd.DataFrame("", index=range(num_rows), columns=column_names)
    edited_data = st.data_editor(data, key="data_editor_pyspark")

    # Generate CSV and PySpark code snippet
    if st.button("Generate CSV and PySpark Code Snippet"):
        csv_data = edited_data.to_csv(index=False, quoting=1)  # quoting=1 for QUOTE_NONNUMERIC
        st.download_button(label="Download CSV", data=csv_data, file_name="data.csv", mime="text/csv")

        st.write("CSV Data:")
        st.code(csv_data, language="csv")

        # Generate PySpark code snippet
        pyspark_code = f"""
from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("{app_name}").getOrCreate()

# Create DataFrame
data = {edited_data.to_dict(orient='list')}
df = pd.DataFrame(data)
spark_df = spark.createDataFrame(df)

# Show DataFrame
spark_df.show()
"""
        st.write("PySpark Code Snippet:")
        st.code(pyspark_code, language="python")

def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select a page", ["Home", "Create CSV", "Create CSV and PySpark Code Snippet"])

    if page == "Home":
        st.title("Home")
        st.write("Select a page from the dropdown menu to get started.")
    elif page == "Create CSV":
        create_csv()
    elif page == "Create CSV and PySpark Code Snippet":
        pyspark_codesnippet()

if __name__ == "__main__":
    main()