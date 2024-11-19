import kagglehub

# Download latest version
path = kagglehub.dataset_download("fireballbyedimyrnmom/us-counties-covid-19-dataset")

print(type(path))
print("Path to dataset files:", path)