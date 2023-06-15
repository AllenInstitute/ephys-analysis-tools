WITH Ivscc AS (
    SELECT
    C.name AS lims_cell_name,
    C.patched_cell_container AS lims_patch_tube,
    SUBSTRING(C.patched_cell_container, 1, 4) AS lims_patch_tube_id,
    CAST(SUBSTRING(C.patched_cell_container, 6, 6) AS DATE) AS lims_patch_tube_date,
    SUBSTRING(C.patched_cell_container, 13, 3) AS lims_patch_tube_number,
    Sp.histology_well_name AS lims_histology_well_name,
    P.code AS lims_project_code,
    O.name AS lims_species_type,
    S.acronym AS lims_structure,
    CR.name AS lims_cell_reporter,
    C.cell_depth AS lims_cell_depth,
    D.name AS lims_donor_name,
    D.external_donor_name AS lims_specimen_id,
    D.full_genotype AS lims_slice_genotype
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
    LEFT JOIN cell_reporters CR 
        ON C.cell_reporter_id = CR.id
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
    AND SUBSTRING(C.patched_cell_container FROM 6 FOR 6) >= '171001'
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
ORDER BY lims_patch_tube_date DESC, lims_patch_tube_id ASC, lims_patch_tube_number ASC