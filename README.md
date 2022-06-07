# ephys_analysis_tools
Code base generating formatted data tables that streamlines data analysis process for ephys production metadata.
- Required: Anaconda Command Prompt

## Authors:
- [@ramr24](https://github.com/ramr24)

## Installation:
1) Fork the repository.
2) Clone the forked repository in a directory of your choice.
3) Navigate to the directory of the forked repository.
```
cd ephys_analysis_tools\
```
4) Create a new virtual environment (ephys_analysis_tools_env) from the .yml file. 
```
conda env create -f environment.yml
```
5) Look below at Usage for detailed steps to run python scripts.

## Usage: Transcriptomics Reports
*Run in Anaconda Command Prompt
1) Navigate to the directory of the forked repository.
```
cd ephys_analysis_tools\src\run_scripts
```
2) Activate the specified virtual environment.
```
activate ephys_analysis_tools_env
```
3) Run the python script for transcriptomics reports.
```
python ivscc_daily_transcriptomics_report.py
OR
python ivscc_weekly_transcriptomics_report.py
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
├── src                                                  <- Source code
    ├── constants                                        <- JSON
    └── run_scripts                                      <- Python run scripts
        └── functions                                    <- Python function scripts
├── run_master_jem.bat                                   <- Bat file
├── run_generate_user_daily.bat                          <- Bat file
├── run_ivscc_daily_transcriptomics_report.bat           <- Bat file
└── run_ivscc_weekly_transcriptomics_report.bat          <- Bat file
```
