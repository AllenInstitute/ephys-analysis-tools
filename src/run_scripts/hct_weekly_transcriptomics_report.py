"""
------------------------------------------------------------------------
File name: hct_weekly_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
------------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/20/2022
Description: Generate HCT weekly transcriptomics report (excel document)
------------------------------------------------------------------------
"""


#-----Imports-----#
# File imports
from functions.transcriptomics_functions import generate_weekly_report


if __name__ == "__main__":
	generate_weekly_report("hct")
