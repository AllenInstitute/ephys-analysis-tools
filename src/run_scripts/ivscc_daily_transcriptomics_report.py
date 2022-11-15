"""
-------------------------------------------------------------------------
File name: ivscc_daily_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
-------------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/27/2021
Description: Generate IVSCC daily transcriptomics report (excel document)
-------------------------------------------------------------------------
"""


#-----Imports-----#
# File imports
from functions.transcriptomics_functions import generate_daily_report


if __name__ == "__main__":
    generate_daily_report("ivscc")
