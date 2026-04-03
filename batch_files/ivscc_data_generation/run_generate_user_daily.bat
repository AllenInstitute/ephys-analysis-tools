@rem %USERPROFILE% = C:\Users\%USERNAME%
call %USERPROFILE%\AppData\Local\anaconda3\Scripts\activate.bat
call cd..\..
call activate ephys-analysis-tools-env
call python src\run_scripts\generate_user_daily.py
call conda deactivate