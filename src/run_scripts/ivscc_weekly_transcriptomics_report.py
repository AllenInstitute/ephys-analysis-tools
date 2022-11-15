"""
--------------------------------------------------------------------------
File name: ivscc_weekly_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
--------------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/28/2021
Description: Generate IVSCC weekly transcriptomics report (excel document)
--------------------------------------------------------------------------
"""


#-----Imports-----#
# File imports
from functions.transcriptomics_functions import generate_weekly_report


if __name__ == "__main__":
	generate_weekly_report("ivscc")
