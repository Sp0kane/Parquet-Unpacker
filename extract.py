import pyarrow.parquet as pq
import pandas as pd
from PIL import Image, UnidentifiedImageError
from io import BytesIO
import glob
import os
import concurrent.futures

# Function to process a single Parquet file
def process_parquet_file(parquet_file):
    try:
        print(f'Processing {parquet_file}...')
        # Load the Parquet file in chunks using PyArrow
        parquet_file_reader = pq.ParquetFile(parquet_file)

        # Extract the base name of the Parquet file for unique image naming
        base_name = os.path.splitext(os.path.basename(parquet_file))[0]

        # Iterate through each row group to reduce memory usage
        for row_group in range(parquet_file_reader.num_row_groups):
            df = parquet_file_reader.read_row_group(row_group).to_pandas()

            # Iterate through the DataFrame, extracting the binary image data
            for i, row in df.iterrows():
                try:
                    image_dict = row['pixel_values']  # Extract the dictionary from the 'pixel_values' column

                    # Get the image bytes from the dictionary
                    image_data = image_dict.get('bytes')  # This is the binary data for the image

                    # Check if image data exists
                    if not image_data:
                        raise ValueError("Missing image bytes data.")

                    # Convert the binary data to an image
                    image = Image.open(BytesIO(image_data))

                    # Generate a unique image name based on the Parquet file and row index
                    image_name = f'{base_name}_row_{row_group}_{i}.png'

                    # Save the image to the current directory
                    image.save(image_name)

                    # Print a message when an image is saved
                    print(f'Image saved: {image_name}')

                except (KeyError, UnidentifiedImageError, ValueError) as e:
                    print(f"Error processing row {i} in file {parquet_file}: {e}")

        # Mark the file as processed
        with open('processed_files.txt', 'a') as f:
            f.write(f'{parquet_file}\n')

    except Exception as e:
        print(f"Failed to process {parquet_file}: {e}")

# Function to orchestrate the whole process
def main():
    # Load the list of already processed files
    if os.path.exists('processed_files.txt'):
        with open('processed_files.txt', 'r') as f:
            processed_files = set(f.read().splitlines())
    else:
        processed_files = set()

    # Get the list of all Parquet files in the directory
    all_parquet_files = glob.glob('*.parquet')
    # Filter out the already processed files
    parquet_files = [f for f in all_parquet_files if f not in processed_files]

    # Process files in parallel using ThreadPoolExecutor with a limit of 5 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_parquet_file, parquet_file): parquet_file for parquet_file in parquet_files}

        # Wait for all futures to complete and gather the results
        for future in concurrent.futures.as_completed(futures):
            parquet_file = futures[future]
            try:
                future.result()  # Just wait for the future to complete
                print(f"Completed processing {parquet_file}.")
            except Exception as e:
                print(f"Error processing {parquet_file}: {e}")

    print('All images from all Parquet files have been extracted and saved.')

if __name__ == "__main__":
    main()
