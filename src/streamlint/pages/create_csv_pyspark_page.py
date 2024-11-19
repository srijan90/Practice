import streamlit as st
import pandas as pd

def show_page():
    st.title("Create CSV and PySpark Code Snippet")

    # Input for app name
    app_name = st.text_input("App Name")

    # Default number of columns and rows
    default_num_columns = 2
    default_num_rows = 2

    # Input for number of columns and rows with unique keys
    num_columns = st.sidebar.number_input("Number of Columns", min_value=1, step=1, value=default_num_columns, key="num_columns")
    num_rows = st.sidebar.number_input("Number of Rows", min_value=1, step=1, value=default_num_rows, key="num_rows")

    # Create a table for input
    column_names = [f"Column {i+1}" for i in range(num_columns)]
    data = pd.DataFrame("", index=range(num_rows), columns=column_names)

    st.write("Enter your data:")
    edited_data = st.data_editor(data, key="data_editor")

    # Generate PySpark code snippet
    if st.button("Generate PySpark Code Snippet"):
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

show_page()