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
├── LICENSE
├── README.md               <- The top-level README.
├── environment.yml         <- Requirements file.
├── constants               <- Spreadsheets, dictionaries, etc.
├── ephys_metadata_tools    <- Source code.
│   ├── __init__.py         <- Makes ephys_metadata_tools a Python module
│   │
│   └── bin                 <- Scripts
│      └── run_collab_daily_report.py
│
└── setup.py                <- makes project installable (python setup.py or pip install -e .) 
                                so ephys_metadata_tools can be imported
