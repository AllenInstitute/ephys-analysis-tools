WITH Collaborators AS (
    SELECT DISTINCT
    C.name AS lims_cell_name,
    C.patched_cell_container AS lims_patch_tube,
    SUBSTRING(C.patched_cell_container, 1, 4) AS lims_patch_tube_id,
    SUBSTRING(C.patched_cell_container, 13, 3) AS lims_patch_tube_number,
    C.cell_depth AS lims_cell_depth,
    D.external_donor_name AS lims_specimen_id,
    D.full_genotype AS lims_slice_genotype,
    D.name AS lims_donor_name, 
    O.name AS lims_species_type,
    P.code AS lims_project_code, 
    S.acronym AS lims_structure
    FROM specimens C
    INNER JOIN specimens Sp
        ON C.parent_id = Sp.id 
    INNER JOIN donors D 
        ON D.id = C.donor_id
    LEFT JOIN organisms O
        ON D.organism_id = O.id
    LEFT JOIN projects P
        ON C.project_id = P.id
    LEFT JOIN structures S
        ON C.structure_id = S.id
    WHERE SUBSTRING(C.patched_cell_container, 1, 4) IN ('PDS4', 'PGS4', 'PHS4', 'PRS4', 'PWS4')
)
SELECT
    *
FROM Collaborators
WHERE SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '001' AND '051'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '101' AND '150'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '201' AND '250'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '501' AND '550'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '601' AND '650'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '701' AND '750'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '801' AND '850'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '901' AND '950'
ORDER BY lims_patch_tube_id, lims_patch_tube_number