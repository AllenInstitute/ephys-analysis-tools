@rem %USERPROFILE% = C:\Users\%USERNAME%
call C:\ProgramData\Anaconda3\Scripts\activate.bat
call cd..\..
call activate ephys-analysis-tools-env
call python src\run_scripts\ivscc_daily_transcriptomics_report.py
call conda deactivate
@pause