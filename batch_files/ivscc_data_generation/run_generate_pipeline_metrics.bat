@rem %USERPROFILE% = C:\Users\%USERNAME%
call %USERPROFILE%\Anaconda3\Scripts\activate.bat
call cd..\..
call activate ephys_analysis_tools_env
call python src\run_scripts\generate_ephys_pipeline_metrics.py
call conda deactivate