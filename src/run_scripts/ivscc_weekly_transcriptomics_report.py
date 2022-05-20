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
# Test imports
#import time # To measure program execution time


if __name__ == "__main__":
	#start = time.time()
	generate_weekly_report("ivscc")
	#print("\nThe program was executed in", round(time.time()-start, 2), "seconds.")
