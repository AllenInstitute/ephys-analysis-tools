WITH Ivscc AS (
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
            'P1S4',
            'P2S4',
            'P4S4',
            'P5S4',
            'P6S4',
            'P8S4',
            'PAS4',
            'PBS4',
            'PFS4',
            'PNS4',
            'PVS4'
        )
)
SELECT
    *
FROM Ivscc
WHERE SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '001' AND '050'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '051' AND '100'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '151' AND '200'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '201' AND '250'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '251' AND '300'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '351' AND '400'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '401' AND '450'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '501' AND '550'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '701' AND '750'
OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '851' AND '900'
ORDER BY lims_patch_tube_id, lims_patch_tube_number