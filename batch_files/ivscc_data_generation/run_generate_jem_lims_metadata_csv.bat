@rem %USERPROFILE% = C:\Users\%USERNAME%
call %USERPROFILE%\AppData\Local\anaconda3\Scripts\activate.bat
call cd..\..
call activate ephys-analysis-tools-env
call python src\run_scripts\generate_jem_lims_metadata_csv.py
call conda deactivate