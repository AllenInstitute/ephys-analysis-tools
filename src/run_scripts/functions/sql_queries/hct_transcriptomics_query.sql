WITH Hct AS (
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
    WHERE SUBSTRING(C.patched_cell_container, 1, 4) 
        IN (
            'PCS4',
            'PXS4',
            'P3S4',
            'P7S4',
            'PJS4',
            'PKS4',
            'PLS4',
            'PSS4'
        )
)
SELECT
    *
FROM Hct
WHERE SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '101' AND '150'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '301' AND '350'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '751' AND '800'
ORDER BY lims_patch_tube_id, lims_patch_tube_number