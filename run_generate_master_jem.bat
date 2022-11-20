@rem %USERPROFILE% = C:\Users\%USERNAME%
call %USERPROFILE%\Anaconda3\Scripts\activate.bat
call activate ephys_analysis_tools_env
call python src\run_scripts\generate_master_jem.py
call conda deactivate