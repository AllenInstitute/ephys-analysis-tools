@rem %USERPROFILE% = C:\Users\%USERNAME%
call %USERPROFILE%\Anaconda3\Scripts\activate.bat
call cd..\..
call activate ephys_analysis_tools_env
call python src\run-scripts\hct_weekly_transcriptomics_report.py
call conda deactivate
@pause