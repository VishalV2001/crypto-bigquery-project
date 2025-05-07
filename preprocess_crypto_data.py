import os
import pandas as pd

# Set paths
input_folder = "/Users/vishalvadhel/Desktop/Crypto Token Price Data Collection & Processing/all_files"
output_folder = os.path.join(os.path.dirname(input_folder), "cleaned_files")

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Loop through all CSV files in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):
        filepath = os.path.join(input_folder, filename)
        print(f"Processing {filename}...")

        # Read CSV
        df = pd.read_csv(filepath)

        # Step 1: Clean the data
        df.columns = df.columns.str.strip().str.lower()  # Normalize column names
        df['timestamp'] = pd.to_datetime(df['timestamp'])  # Ensure datetime format
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'marketcap']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')  # Convert to floats
        df = df.dropna(subset=numeric_cols + ['timestamp'])  # Drop rows with bad data
        df = df.sort_values('timestamp', ascending=False)  # Sort by time ascending

        # Step 2: Deduplicate the data
        df = df.drop_duplicates(subset=['token', 'timestamp'], keep='first')  # Remove duplicates
        # Remove rows with zero marketcap
        df = df[df['marketcap'] != 0.0]

        # Save cleaned data
        cleaned_filename = f"{os.path.splitext(filename)[0]}_cleaned.csv"
        df.to_csv(os.path.join(output_folder, cleaned_filename), index=False)
        print(f"Saved cleaned file: {cleaned_filename}")

print("All files processed, cleaned, and deduplicated.")
