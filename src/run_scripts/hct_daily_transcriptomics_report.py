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
# Test imports
#import time # To measure program execution time


if __name__ == "__main__":
	#start = time.time()
	generate_daily_report("hct")
	#print("\nThe program was executed in", round(time.time()-start, 2), "seconds.")
