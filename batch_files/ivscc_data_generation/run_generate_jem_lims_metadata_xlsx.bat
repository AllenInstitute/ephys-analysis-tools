@rem %USERPROFILE% = C:\Users\%USERNAME%
call C:\ProgramData\Anaconda3\Scripts\activate.bat
call cd..\..
call activate ephys-analysis-tools-env
call python src\run_scripts\generate_jem_lims_metadata_xlsx.py
call conda deactivate