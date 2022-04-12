"""
---------------------------------------------------------------------
File name: lims_funcs.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 04/02/2022
Description: LIMS related functions
---------------------------------------------------------------------
"""


def get_lims():
	"""

	"""

    lims_query="""
    SELECT DISTINCT cell.name, cell.patched_cell_container,
    d.external_donor_name AS id_cell_specimen_id, d.full_genotype AS id_slice_genotype, d.name AS donor_name, 
    org.name AS id_species
    FROM specimens cell 
    JOIN specimens slice ON cell.parent_id = slice.id 
    JOIN donors d ON d.id = cell.donor_id
    JOIN organisms org ON d.organism_id = org.id
    WHERE SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) BETWEEN '170101' AND '301231'"""

    df = pd.DataFrame(limsquery(lims_query))
    if is_this_py3:
        df = rename_byte_cols(df)
    
    return df


def generate_lims_df(date):
	"""Generate a lims dataframe"""

	# Lists
	hct_jt = [str(x) for x in range(101, 151, 1)] # Jonathan(101-150)
	hct_cr = [str(x) for x in range(225, 251, 1)] # Cristina(225-250)
	hct_bk_mk = [str(x) for x in range(301, 351, 1)] # Brian K(301-350), Meanhwan(325-350)
	hct_ln = [str(x) for x in range(351, 401, 1)] # Lindsay(351-400)
	hct_user_tube_num_list = hct_jt + hct_cr + hct_bk_mk + hct_ln

	lims_df = get_lims()
	# Filters dataframe to user specified date
	lims_df = lims_df[lims_df["patched_cell_container"].str.contains(date)]
	# Only run if patched cell containers were collected
	if len(lims_df) > 0:
		# Exclude Collaborator containers
		lims_df = lims_df[(~lims_df["patched_cell_container"].str.startswith("PGS4")) & (~lims_df["patched_cell_container"].str.startswith("PHS4"))]
		# Exclude HCT containers (Ex. column output: 301)
		lims_df["exclude_container"] = lims_df["patched_cell_container"].str.slice(-7, -4)
		lims_df = lims_df[~lims_df["exclude_container"].str.contains("|".join(hct_user_tube_num_list))]
		# Replace values
		lims_df["id_species"].replace({"Homo Sapiens":"Human", "Mus musculus":"Mouse"}, inplace=True)
		lims_df["id_slice_genotype"].replace({None:""}, inplace=True)
		# Apply specimen id
		lims_df["id_cell_specimen_id"] = lims_df.apply(get_specimen_id, axis=1)
		# Sort by patched_cell_container in ascending order
		lims_df.sort_values(by="patched_cell_container", inplace=True)
	return lims_df

