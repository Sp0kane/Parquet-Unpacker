## Parquet Image Extractor

Extracts embedded image bytes from `.parquet` files and saves them as `.png` images.

Designed for Parquet datasets where a column (default: `pixel_values`) contains a dictionary with a `bytes` key holding raw image data.

Images are extracted efficiently using PyArrow row groups to reduce memory usage and processed in parallel using a thread pool.


## What It Does

* Scans the current directory for `*.parquet` files
* Skips files already listed in `processed_files.txt`
* Reads each Parquet file row group by row group
* Extracts image bytes from the `pixel_values['bytes']` field
* Saves each image as:

<parquet_filename>_row_<rowgroup>_<rowindex>.png

* Appends processed file names to `processed_files.txt`

## Expected Parquet Schema

The script assumes:

pixel_values: dict
    └── bytes: binary image data

If:

* `pixel_values` is missing
* `bytes` key is missing
* image data is invalid

The row is skipped and logged.

## Installation

### 1. Create a virtual environment

bash
python -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows

### 2. Install dependencies

bash
pip install -r requirements.txt

## Usage

Place your `.parquet` files in the same directory as the script.

Then run:

bash
python extract_images.py

The script will:

* Process up to 5 Parquet files in parallel
* Extract images
* Save them in the current directory
* Record processed files in `processed_files.txt`

## Parallel Processing

Uses:
ThreadPoolExecutor(max_workers=5)

You can increase/decrease workers depending on:
* Disk IO speed
* CPU
* Memory constraints

Modify:
max_workers=5

## Recovery / Resume

The script tracks processed files in:
processed_files.txt

If interrupted:

* Restarting will skip completed files
* Delete `processed_files.txt` to reprocess everything

## Performance Notes

* Uses PyArrow row groups to limit memory usage
* Converts each row group to pandas
* Saves images immediately to avoid accumulation in memory
* Designed for large Parquet datasets

If memory becomes an issue:

* Reduce `max_workers`
* Process files sequentially

## Error Handling

Per-row errors are caught:

* Missing `pixel_values`
* Missing `bytes`
* Invalid image format

File-level errors are also caught to prevent stopping the entire batch.
