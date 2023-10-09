# Python Script for Data Cleaning & Analysis

This script is designed to clean & analyze data from multiple files in a directory. It performs various operations such as removing newline & tab characters, checking for null values & more.

## Dependencies

The script requires the following Python libraries:
- `json`
- `os`
- `pandas`
- `tqdm`

## Directory Structure

The script expects a directory named 'parsed_files' on the Desktop of a user named 'narayansajeev' on a machine running macOS. The path to this directory is `/Users/narayansajeev/Desktop/MIT/parsed_files`.

## Functions

The script contains several functions that perform specific tasks:

- `loop_fnames(prov)`: Loops through the files in the directory & returns a list of files that have been parsed.
- `get_df(prov, fname)`: Reads in the first file from the list using pandas.
- `get_known_cols()`: Checks column classifier.
- `clean(col_headers)`: Cleans up the headers by removing newline, carriage return, non-breaking space & space characters.
- `substr_check(substr_sets, k)`: Checks for substrings in the column headers.
- `substring(df, known_cols)`: Lists column headers.
- `drop_columns(df, col_headers)`: Drops specified columns from the dataframe.
- `newline(df, prov)`, `tab(df, prov)`, `adltrnt_msrmnt(df, prov)`, `adltrnt_none(df, prov)`, `headers(df, prov)`, `none(df, prov)`, `test_legal_none(df, prov)` & `test_concat_legal_none(df, prov)`: These functions check for specific conditions in the dataframe & update global dictionaries accordingly.

## Execution

The script loops through all files in the specified directory. For each file, it performs a series of operations to clean & analyze the data. The results are stored in global dictionaries.

## Output

At the end of execution, the script prints out the results stored in the global dictionaries. This includes information about rows affected by certain conditions, files containing these rows & sources of these files.