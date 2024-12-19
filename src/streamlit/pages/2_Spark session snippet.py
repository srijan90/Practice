import streamlit as st

# Define the headings and input fields
st.title("Spark Session Configuration")
st.sidebar.header("Spark Session Configuration")
master = st.sidebar.text_input("Master(local)", "local")
app_name = st.sidebar.text_input("App Name (Type your app name)", "Word Count")
config_key = st.sidebar.text_input("Config Key(spark.some.config.option)", "spark.some.config.option")
config_value = st.sidebar.text_input("Config Value(type some value)", "some-value")

# Display the input details below the text boxes
st.write("### Input Details")
st.write(f"**Master:** {master}")
st.write(f"**App Name:** {app_name}")
st.write(f"**Config Key:** {config_key}")
st.write(f"**Config Value:** {config_value}")

if master:
    st.write(f"Master: {master}")

# Button to generate the code snippet
if st.button("Go or Create"):
    code_snippet = f"""
from pyspark.sql import SparkSession
    
spark = (
    SparkSession.builder"""
    if master:
        code_snippet += f"""\n          .master("{master}")"""
    if app_name:
        code_snippet+= f"""\n          .appName("{app_name}")"""
    if config_key and config_value:
        code_snippet += f"""\n          .config("{config_key}", "{config_value}")"""
    code_snippet += f"""\n      .getOrCreate() 
        )"""
    st.code(code_snippet, language='python')