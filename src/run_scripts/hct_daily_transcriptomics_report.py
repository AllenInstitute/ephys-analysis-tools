"""
-----------------------------------------------------------------------
File name: hct_daily_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/19/2022
Description: Generate HCT daily transcriptomics report (excel document)
-----------------------------------------------------------------------
"""


#-----Imports-----#
# File imports
from functions.transcriptomics_functions import generate_daily_report


if __name__ == "__main__":
	generate_daily_report("hct")
