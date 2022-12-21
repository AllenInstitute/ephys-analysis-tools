# ephys-analysis-tools
Code base generating formatted data tables that streamlines data analysis process for ephys production metadata.
- Required: Anaconda Command Prompt

## Authors:
- [@ramr24](https://github.com/ramr24)

## Installation:
1) Clone the repository in a directory of your choice.
2) Navigate to the directory of the cloned repository.
```
cd ephys-analysis-tools\
```
3) Create a new virtual environment (ephys-analysis-tools-env) from the .yml file. 
```
conda env create -f environment.yml
```
4) Look below at Usage for detailed steps to run python scripts.

## Usage: Batch Files
* Required: Location of Anaconda bat file --> C:\Users\%USERNAME%\Anaconda3\Scripts\activate.bat

1) In File Explorer, navigate to the directory of the cloned repository.
```
cd ephys-analysis-tools\
```
2) Click on the desired .bat file to run the intended program.

## Usage: Run Transcriptomics Reports through Anconda Command Prompt
*Run in Anaconda Command Prompt
1) Navigate to the directory of the cloned repository.
```
cd ephys-analysis-tools\src\run_scripts
```
2) Activate the specified virtual environment.
```
activate ephys-analysis-tools-env
```
3) Run the python scripts for daily/weekly transcriptomics reports.
```
python ivscc_daily_transcriptomics_report.py
```
```
python ivscc_weekly_transcriptomics_report.py
```
```
python hct_daily_transcriptomics_report.py
```
```
python hct_weekly_transcriptomics_report.py
```
4) Deactivate the specified virtual environment.
```
conda deactivate
```

## Project Organization
```
├── README.md                                            <- The top-level README
├── .gitignore                                           <- Ignore file types
├── environment.yml                                      <- Requirements file
├── batch_files                                          <- Batch files
    ├── hct_transcriptomic_reports                       <- Batch files related to HCT transcriptomic reports
    ├── ivscc_data_generation                            <- Batch files related to IVSCC data generation
    └── ivscc_transcriptomic_reports                     <- Batch files related to IVSCC transcriptomic reports
├── src                                                  <- Source code
    ├── constants                                        <- JSON
    └── run_scripts                                      <- Python run scripts
        └── functions                                    <- Python function scripts
```
