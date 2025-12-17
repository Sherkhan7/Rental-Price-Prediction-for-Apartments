import kagglehub

# Download latest version
path = kagglehub.dataset_download("trolukovich/riga-real-estate-dataset")

print("Path to dataset files:", path)
