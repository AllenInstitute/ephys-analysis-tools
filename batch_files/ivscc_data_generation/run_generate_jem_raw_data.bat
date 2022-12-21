@rem %USERPROFILE% = C:\Users\%USERNAME%
call %USERPROFILE%\Anaconda3\Scripts\activate.bat
call cd..\..
call activate ephys_analysis_tools_env
call python src\run_scripts\generate_jem_raw_data.py
call conda deactivate