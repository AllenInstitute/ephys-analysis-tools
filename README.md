# ephys_analysis_tools
Code base generating formatted data tables that streamlines data analysis process for ephys production metadata.

Directions to run daily and weekly transcriptomics reports in Anaconda Command Prompt:
1) Navigate to the directory with the forked repository ---> cd ...ephys_analysis_tools\src\run_scripts
2) Run the python script for transcriptomics reports  ---> python file_name.py

Python Scripts
1) generate_master_jem.py: Creates master_jem.csv and master_jem.xlsx, daily at 7:00 am.
2) generate_user_daily.py: Creates user_daily.xlsx, daily at 4:30 pm.
3) ivscc_daily_transcriptomics_report.py: Creates daily transcriptomics reports for the IVSCC group.
4) ivscc_daily_transcriptomics_report.py: Creates weekly transcriptomics reports for the IVSCC group.
5) hct_daily_transcriptomics_report.py: Creates daily transcriptomics reports for the HCT group.
6) hct_weekly_transcriptomics_report.py: Creates weekly transcriptomics reports for the HCT group.
