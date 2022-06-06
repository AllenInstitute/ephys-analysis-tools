# ephys_analysis_tools
Code base generating formatted data tables that streamlines data analysis process for ephys production metadata.
- Required: Anaconda Command Prompt

## Authors:
- [@ramr24](https://github.com/ramr24)

## Installation:
```
1) Installing environment...
```

## Usage: Transcriptomics Reports
*Run in Anaconda Command Prompt
1) Navigate to the directory with the forked repository.
```
cd ...ephys_analysis_tools\src\run_scripts
```
2) Run the python script for transcriptomics reports.
```
python file_name.py
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
