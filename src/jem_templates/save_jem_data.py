"""
-----------------------------------
File name: save_jem_data.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------
Author: Agata
Date/time created: 3/11/2022
Description: Save jem metdata as a csv
-----------------------------------
"""


# Imports
from funcs.functions_jem import make_metadata_csv
from datetime import datetime


# Variables
filename = "test_jem_metadata"
# day_start_default = "171001"
#day_start = "220101"
#day_end = "220131"

today = datetime.today()
datem = datetime(today.year, 3, 1).date()
day_start = datem.strftime("%y%m%d")


# Main
if __name__ == '__main__':
	print("Running save_jem_data.py")
	print()
	_, _, _ = make_metadata_csv(start_day_str=day_start) #start_day_str="171001"
	print("Finished running make_metdata_csv from get_met_data")
