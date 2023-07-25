# Mitonalysis

## Usage
Use a venv: `python -m venv venv`

The following scripts exist:
 * `read_mitometer_file.py` - Process the data from a single assay, with the translated txt files all in one directory
 * `process_all_assays.py` - Process the data from multiple assays, all assays contained in one directory, each assay with its own subdirectory in the format required for `read_mitometer_file.py`
 * `compare_experiments.py` - Process the data from multiple experiments, each experiment with its own subdirectory in a parent directory. Each subdirectory follows the structure required for `process_all_assays.py`. An Experiment is considered a group of assays.

## Dependencies
If using a venv, make sure to install dependencies and run everything inside the venv

 * `python -m pip install --upgrade pip`
 * `python -m pip install pandas`
 * `python -m pip install lip-pps-run-manager`
 * `python -m pip install plotly`
