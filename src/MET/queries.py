
limsjob_fail_query = """SELECT DISTINCT p.code, 
    err.id, 
    jq.name AS job_queue, 
    js.name, 
    j.dequeued_at, 
    j.completed_at, 
    '=HYPERLINK("http://lims2/jobs?id=' || j.id || '")' AS job_link, 
    j.warning_message, 
    '=HYPERLINK("\' || replace(err.storage_directory, '/', '\') || '")' AS err_dir, 
    (regexp_matches(j.warning_message, '(Exception: )([A-Za-z0-9_ "().]+)'))[2] AS py_err
    FROM ephys_roi_results err
    JOIN jobs j ON j.enqueued_object_id = err.id AND j.archived IS FALSE 
    JOIN job_states js ON js.id = j.job_state_id
    JOIN job_queues jq ON jq.id = j.job_queue_id
    JOIN projects p ON p.id = j.project_id
    WHERE j.job_state_id != 3
    AND p.code IN  %(codes)s
    AND jq.name NOT IN %(queues)s
    ORDER BY p.code, err.id, j.dequeued_at DESC"""



nwbpath_query = """SELECT CONCAT(nwb.storage_directory,  nwb.filename) AS path 
    FROM specimens cell 
    JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
    JOIN well_known_files nwb ON err.id = nwb.attachable_id 
    WHERE nwb.well_known_file_type_id = 475137571 
    AND cell.name = '%(mycell)s'"""


met_query = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%' 
    GROUP BY dg.donor_id),
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name AS cell_name,
cell.patched_cell_container AS container, 
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.seal_gohm AS seal, 
err.published_at, 
err.workflow_state, 
ra.failed AS amp_failed, 
ra.percent_cdna_longer_than_400bp,
ra.amplified_quantity_ng,
org.name AS organism, 
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN donors d ON d.id = cell.donor_id 
LEFT JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id 
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE cell.patched_cell_container IS NOT NULL 
AND proj.code IN ('hIVSCC-MET', 'mIVSCC-MET') 
AND nwb.well_known_file_type_id = 475137571 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""

met_query_s = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%' 
    GROUP BY dg.donor_id),
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name AS cell_name,
cell.patched_cell_container AS container, 
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.seal_gohm AS seal, 
err.published_at, 
err.workflow_state, 
ra.failed AS amp_failed, 
ra.percent_cdna_longer_than_400bp,
ra.amplified_quantity_ng,
org.name AS organism, 
ARRAY_AGG(DISTINCT LEFT(ephys_stimuli.description, STRPOS(ephys_stimuli.description, '[') -1)) AS stimuli_represented,
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
LEFT JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN donors d ON d.id = cell.donor_id 
LEFT JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id 
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE cell.patched_cell_container IS NOT NULL 
AND proj.code IN ('hIVSCC-MET', 'mIVSCC-MET') 
AND nwb.well_known_file_type_id = 475137571 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""

na_query = """WITH
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%' 
    GROUP BY dg.donor_id)
SELECT cell.name AS cell_name,
drv.drivers,
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
org.name AS organism 
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN donors d ON d.id = cell.donor_id 
LEFT JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id 
WHERE cell.patched_cell_container IS NULL 
AND proj.code IN ('hIVSCC-MET', 'mIVSCC-MET') 
AND err.recording_date > '2018-06-01'
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""



met_query_wstim_info = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%' 
    GROUP BY dg.donor_id),
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name AS cell_name,
cell.id AS specimen_id,
cell.patched_cell_container AS container, 
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
str.acronym AS structure, 
layer.acronym AS layer, 
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.id AS ephys_id,
err.seal_gohm AS seal, 
err.published_at, 
err.workflow_state, 
ra.failed AS amp_failed, 
ra.percent_cdna_longer_than_400bp,
ra.amplified_quantity_ng,
org.name AS organism, 
d.external_donor_name AS labtracks_id,  
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPCllATT%%' THEN 1 ELSE NULL END) AS cellatt, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPBREAKN%%' THEN 1 ELSE NULL END) AS breakn, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPEXPEND%%' THEN 1 ELSE NULL END) AS endcore, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPGGAEND%%' THEN 1 ELSE NULL END) AS ggaend,
ARRAY_AGG(DISTINCT LEFT(ephys_stimuli.description, STRPOS(ephys_stimuli.description, '[') -1)) AS stimuli_represented,
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
LEFT JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN donors d ON d.id = cell.donor_id 
LEFT JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id 
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
LEFT JOIN structures str on cell.structure_id = str.id 
LEFT JOIN structures layer on layer.id = cell.cortex_layer_id 
WHERE cell.patched_cell_container IS NOT NULL 
AND proj.code IN ('hIVSCC-MET', 'mIVSCC-MET') 
AND nwb.well_known_file_type_id = 475137571 
GROUP BY cell.name, 
cell.id,
cell.patched_cell_container,
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id,
imgs63.workflow_state, 
swc.swc_filename, 
proj.code,
str.acronym, 
layer.acronym,
TO_CHAR(err.recording_date,'YYYY-MM-DD'),
err.id,
err.seal_gohm, 
err.published_at, 
err.workflow_state, 
ra.failed,
ra.percent_cdna_longer_than_400bp,
ra.amplified_quantity_ng, 
org.name,
d.external_donor_name, 
CONCAT(nwb.storage_directory,  nwb.filename),
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""


mouse_met_query_2 = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%' 
    GROUP BY dg.donor_id),
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name AS cell_name,
cell.patched_cell_container AS container, 
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.seal_gohm AS seal, 
err.published_at, 
err.workflow_state, 
ra.failed AS amp_failed, 
ra.percent_cdna_longer_than_400bp,
ra.amplified_quantity_ng,
org.name AS organism, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPCllATT%%' THEN 1 ELSE NULL END) AS cellatt, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPBREAKN%%' THEN 1 ELSE NULL END) AS breakn, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPEXPEND%%' THEN 1 ELSE NULL END) AS endcore, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPGGAEND%%' THEN 1 ELSE NULL END) AS ggaend,
ARRAY_AGG(DISTINCT LEFT(ephys_stimuli.description, STRPOS(ephys_stimuli.description, '[') -1)) AS stimuli_represented,
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
LEFT JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN donors d ON d.id = cell.donor_id 
LEFT JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id 
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE cell.patched_cell_container IS NOT NULL 
AND proj.code IN ('mIVSCC-MET')
AND nwb.well_known_file_type_id = 475137571
GROUP BY cell.name, 
cell.patched_cell_container,
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id, 
imgs63.workflow_state, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD'), 
err.seal_gohm, 
err.published_at, 
err.workflow_state, 
ra.failed, 
ra.percent_cdna_longer_than_400bp,
ra.amplified_quantity_ng,
org.name,  
CONCAT(nwb.storage_directory,  nwb.filename),
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""


endPipette_met_query = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%' 
    GROUP BY dg.donor_id),
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name AS cell_name,
cell.patched_cell_container AS container, 
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.id as roi_id,
err.seal_gohm AS seal, 
err.published_at, 
err.workflow_state, 
ra.failed AS amp_failed, 
ra.percent_cdna_longer_than_400bp,
ra.amplified_quantity_ng,
org.name AS organism, 
sw.sweep_number,
es.description,
CONCAT(nwb.storage_directory, nwb.filename) AS filepath, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell 
JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli es ON sw.ephys_stimulus_id = es.id 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN donors d ON d.id = cell.donor_id 
LEFT JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id 
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE cell.patched_cell_container IS NOT NULL 
AND nwb.well_known_file_type_id = 475137571 
AND es.description LIKE '%%GGAEND%%' 
AND proj.code IN ('mIVSCC-MET', 'hIVSCC-MET')"""




endR_query = """SELECT cell.name AS cell_name,
cell.patched_cell_container AS container, 
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.id as roi_id,
sw.sweep_number,
es.description,
CONCAT(wkfs.storage_directory,  wkfs.filename) AS miesnwb_filepath,
CONCAT(nwb.storage_directory,  nwb.filename) AS limsnwb_filepath  
FROM specimens cell 
LEFT JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
LEFT JOIN ephys_stimuli es ON sw.ephys_stimulus_id = es.id 
LEFT JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files wkfs ON err.id = wkfs.attachable_id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
WHERE wkfs.well_known_file_type_id = 570280085 
AND nwb.well_known_file_type_id = 475137571  
AND cell.patched_cell_container IS NOT NULL 
AND es.description LIKE '%%GGAEND%%' 
AND proj.code IN ('mIVSCC-MET', 'hIVSCC-MET')"""



met_inbath_query = """SELECT cell.patched_cell_container AS container, 
CONCAT(nwb.storage_directory, nwb.filename) AS filepath,
sw.sweep_number
FROM specimens cell 
JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
WHERE nwb.well_known_file_type_id = 475137571 
AND proj.code IN ('mIVSCC-MET', 'hIVSCC-MET')
AND ephys_stimuli.description LIKE '%%INBATH%%'"""





morpho_tipres_query = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id and tag.id in (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename AS swc_filename, 
        marker.storage_directory || marker.filename as marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id AND 
        marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name AS cell_name, 
cell.patched_cell_container AS container, 
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code, 
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.initial_access_resistance_mohm AS initial_access, 
err.published_at, 
err.workflow_state, 
CONCAT(nwb.storage_directory, nwb.filename) AS filepath, 
ra.failed AS amp_failed, 
org.name AS organism, 
sw.sweep_number, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell 
JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2015-01-01' 
AND ephys_stimuli.description LIKE '%%INBATH%%' 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""


morpho_query = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name AS cell_name,
cell.patched_cell_container AS container, 
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code 
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE cell.patched_cell_container IS NOT NULL 
AND proj.code NOT IN ('mMPATCH','T504', 'hIVSCC-METx', 'mIVSCC-METx', 'T301t')"""



chirp_query = """SELECT cell.name AS cell_name, 
cell.id AS cell_id, 
cell.patched_cell_container AS container, 
proj.code, 
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
CONCAT(nwb.storage_directory, nwb.filename) AS filepath, 
org.name AS organism, 
sw.*,  
st.description,
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell 
JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli st ON sw.ephys_stimulus_id = st.id 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
WHERE nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2015-01-01' 
AND st.description LIKE '%%CHIRP%%' 
AND proj.code in ('mIVSCC-MET', 'hIVSCC-MET')
AND cell.patched_cell_container IS NOT NULL 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""





met_stimulus_count_query = """SELECT DISTINCT cell.name AS cell_name, 
proj.code, 
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
cell.patched_cell_container AS container, 
CONCAT(nwb.storage_directory, nwb.filename) AS filepath, 
ra.failed AS amp_failed, 
go63.name AS go_63x, 
org.name AS organism, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%subthreshold%%' THEN 1 ELSE NULL END) AS subthreshold, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1LSFINEST150112%%' THEN 1 ELSE NULL END) AS longsquare, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1SSFINEST150112%%' THEN 1 ELSE NULL END) AS shortsquare, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1RP25PR1S141203%%' THEN 1 ELSE NULL END) AS ramp, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1SQCAPCHK141203%%' THEN 1 ELSE NULL END) AS cap_check, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C2CHIRP171129%%' THEN 1 ELSE NULL END) AS chirp_v1, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C2CHIRP180503%%' THEN 1 ELSE NULL END) AS chirp_v2, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1NSD1SHORT17110%%' THEN 1 ELSE NULL END) AS noise_1, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1NSD2SHORT17110%%' THEN 1 ELSE NULL END) AS noise_2, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C2SSTRIPLE171103%%' THEN 1 ELSE NULL END) AS triple, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN specimen_tags_specimens sts ON sts.specimen_id = cell.id 
AND sts.specimen_tag_id IN (602122082,602120185) 
LEFT JOIN specimen_tags go63 ON go63.id = sts.specimen_tag_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
WHERE cell.patched_cell_container NOTNULL 
AND nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2017-11-29' 
GROUP BY cell.name, 
TO_CHAR(err.recording_date,'YYYY-MM-DD'), 
proj.code, 
cell.patched_cell_container, 
CONCAT(nwb.storage_directory, nwb.filename), 
ra.failed, 
go63.name, 
org.name, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""

#AND ess.workflow_state != 'auto_failed'



met_stimulus_count_query = """SELECT DISTINCT cell.name AS cell_name, 
proj.code, 
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
cell.patched_cell_container AS container, 
CONCAT(nwb.storage_directory, nwb.filename) AS filepath, 
ra.failed AS amp_failed, 
go63.name AS go_63x, 
org.name AS organism, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%subthreshold%%' THEN 1 ELSE NULL END) AS subthreshold, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1LSFINEST150112%%' THEN 1 ELSE NULL END) AS longsquare, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1SSFINEST150112%%' THEN 1 ELSE NULL END) AS shortsquare, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1RP25PR1S141203%%' THEN 1 ELSE NULL END) AS ramp, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1SQCAPCHK141203%%' THEN 1 ELSE NULL END) AS cap_check, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C2CHIRP171129%%' THEN 1 ELSE NULL END) AS chirp_v1, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C2CHIRP180503%%' THEN 1 ELSE NULL END) AS chirp_v2, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1NSD1SHORT17110%%' THEN 1 ELSE NULL END) AS noise_1, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C1NSD2SHORT17110%%' THEN 1 ELSE NULL END) AS noise_2, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%C2SSTRIPLE171103%%' THEN 1 ELSE NULL END) AS triple, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN specimen_tags_specimens sts ON sts.specimen_id = cell.id 
AND sts.specimen_tag_id IN (602122082,602120185) 
LEFT JOIN specimen_tags go63 ON go63.id = sts.specimen_tag_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
WHERE cell.patched_cell_container NOTNULL 
AND nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2017-11-29' 
GROUP BY cell.name, 
TO_CHAR(err.recording_date,'YYYY-MM-DD'), 
proj.code, 
cell.patched_cell_container, 
CONCAT(nwb.storage_directory, nwb.filename), 
ra.failed, 
go63.name, 
org.name, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""


variable_query = """SELECT cell.name AS cell_name, 
cell.patched_cell_container AS container, 
SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) AS lims_date,
proj.code
FROM specimens cell
JOIN projects proj ON cell.project_id = proj.id 
WHERE proj.code IN ('mIVSCC-MET', 'hIVSCC-MET')
AND cell.patched_cell_container IS NOT NULL
AND SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) > '%(mydate1)s'
AND SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) < '%(mydate2)s'
ORDER BY SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) DESC"""






met_durations_stimulus_count_query = """SELECT DISTINCT cell.name AS cell_name, 
proj.code, 
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
cell.patched_cell_container AS container, 
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
ra.failed AS amp_failed, 
go63.name AS go_63x, 
org.name AS organism, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPCllATT%%' THEN 1 ELSE NULL END) AS cellatt, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPBREAKN%%' THEN 1 ELSE NULL END) AS breakn, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPEXPEND%%' THEN 1 ELSE NULL END) AS endcore, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%EXTPGGAEND%%' THEN 1 ELSE NULL END) AS ggaend,
ARRAY_AGG(DISTINCT LEFT(ephys_stimuli.description, STRPOS(ephys_stimuli.description, '[') -1)) AS stimuli_represented,
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN specimen_tags_specimens sts ON sts.specimen_id = cell.id 
AND sts.specimen_tag_id IN (602122082,602120185) 
LEFT JOIN specimen_tags go63 ON go63.id = sts.specimen_tag_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
WHERE ((cell.patched_cell_container NOTNULL) AND (cell.patched_cell_container != 'NA'))
AND nwb.well_known_file_type_id = 475137571 
AND proj.code IN ('hIVSCC-MET', 'mIVSCC-MET')
GROUP BY cell.name, 
TO_CHAR(err.recording_date,'YYYY-MM-DD'), 
proj.code, 
cell.patched_cell_container, 
CONCAT(nwb.storage_directory,  nwb.filename), 
ra.failed, 
go63.name, 
org.name, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""






"Get a list of the current clamp stimuli run for each cell."

dlgn_all_stimuli_query ="""SELECT cell.name AS cell_name, 
cell.id,
cell.patched_cell_container AS container, 
SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) AS lims_date,
proj.code,
SUBSTRING(ephys_stimuli.description FROM '[a-zA-Z0-9]*') AS stimulus
FROM specimens cell
JOIN projects proj ON cell.project_id = proj.id 
JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
WHERE cell.patched_cell_container IS NOT NULL
AND cell.id IN %(cell_id)s
AND SUBSTRING(ephys_stimuli.description FROM 1 FOR 4) NOT IN ('EXTP', 'VCLA', 'Rheo')
ORDER BY SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) DESC"""


dlgn_stimulus_counts_query = """SELECT DISTINCT cell.name AS cell_name, 
cell.id,
proj.code, 
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
cell.patched_cell_container AS container, 
CONCAT(nwb.storage_directory, nwb.filename) AS filepath, 
ra.failed AS amp_failed, 
go63.name AS go_63x, 
org.name AS organism, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%%%subthreshold%%%%' THEN 1 ELSE NULL END) AS subthreshold, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%%%C1LSFINEST150112%%%%' THEN 1 ELSE NULL END) AS ls_finest, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%%%C1SSFINEST150112%%%%' THEN 1 ELSE NULL END) AS ss_finest,
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%%%C1LSCOARSE150216%%%%' THEN 1 ELSE NULL END) AS ls_coarse, 
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%%%C1LSCOARSEMICRO%%%%' THEN 1 ELSE NULL END) AS ls_coarsemicro,
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%%%C1SSCOARSE150112%%%%' THEN 1 ELSE NULL END) AS ss_coarse,
COUNT(CASE WHEN ephys_stimuli.description LIKE '%%%%C1RP25PR1S141203%%%%' THEN 1 ELSE NULL END) AS ramp, 
ARRAY_AGG(DISTINCT LEFT(ephys_stimuli.description, STRPOS(ephys_stimuli.description, '[') -1)) AS stimuli_represented,
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END AS cell_qc 
FROM specimens cell JOIN ephys_sweeps sw ON cell.id = sw.specimen_id 
JOIN ephys_stimuli ON sw.ephys_stimulus_id = ephys_stimuli.id 
JOIN projects proj ON cell.project_id = proj.id 
LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
LEFT JOIN specimen_tags_specimens sts ON sts.specimen_id = cell.id 
AND sts.specimen_tag_id IN (602122082,602120185) 
LEFT JOIN specimen_tags go63 ON go63.id = sts.specimen_tag_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
WHERE cell.id IN %(cell_id)s 
AND cell.patched_cell_container IS NOT NULL 
AND nwb.well_known_file_type_id = 475137571 
GROUP BY cell.name, 
cell.id,
TO_CHAR(err.recording_date,'YYYY-MM-DD'), 
proj.code, 
cell.patched_cell_container, 
CONCAT(nwb.storage_directory, nwb.filename), 
ra.failed, 
go63.name, 
org.name, 
CASE 
        WHEN err.seal_gohm >= 1 
            AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
                OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
            AND err.initial_access_resistance_mohm < 20 
            AND ABS(err.electrode_0_pa) < 100 
            THEN 'good' 
        ELSE 'bad' END 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""

#query = q % {'cell_id': tuple(dlgn["id"])}
#count_df = pd.DataFrame(limsquery(query))
#count_df.head()



human_me_ls_spiking_query = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name AS cell_name,
cell.id AS cell_id,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.id as roi_id,
err.published_at, 
err.workflow_state, 
sw.sweep_number,
sw.stimulus_amplitude,
sw.num_spikes,
st.description,
CONCAT(nwb.storage_directory,  nwb.filename) AS filepath, 
org.name AS organism 
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
JOIN ephys_features ef on ef.specimen_id = cell.id
JOIN ephys_sweeps sw ON cell.id = sw.specimen_id
JOIN ephys_stimuli st ON sw.ephys_stimulus_id = st.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE proj.code IN ('H301')
AND nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2015-01-01' 
AND err.workflow_state = 'manual_passed'
AND err.published_at IS NOT NULL
AND sw.num_spikes = 1
AND st.description LIKE '%%C1LSFINEST%%' 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""
#AND sw.stimulus_amplitude > 400 
#AND cell.id = 528687520



human_me_rheo_sw_query = """SELECT cell.name AS cell_name,
cell.id AS cell_id,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.id as roi_id, 
sw.sweep_number,
ROUND(sw.stimulus_amplitude) AS stimulus_amplitude,
sw.num_spikes,
st.description,
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_path,
CONCAT(nwb.storage_directory, 'EPHYS_FEATURE_EXTRACTION_V2_QUEUE_' || err.id || '_output.json') as feature_path
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
JOIN ephys_features ef on ef.specimen_id = cell.id
JOIN ephys_sweeps sw ON ef.rheobase_sweep_id = sw.id
JOIN ephys_stimuli st ON sw.ephys_stimulus_id = st.id 
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
WHERE proj.code IN ('H301')
AND nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2015-01-01' 
AND err.workflow_state = 'manual_passed'
AND sw.num_spikes > 0
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""





met_query_all = """WITH imgs63 AS 
#     (SELECT max(id) AS image_series_id, 
#     max(workflow_state) AS workflow_state, 
#     specimen_id 
#     FROM image_series 
#     WHERE is_stack = true 
#     GROUP BY specimen_id), 
# do_63x(cell_id, go) AS 
#     (SELECT DISTINCT cell.id, 
#     array_to_string(array_agg(DISTINCT tag.name), 
#     ' _AND_ ') 
#     FROM specimens cell 
#     JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
#     JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
#     JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
#     (602120185,602122082) 
#     GROUP BY cell.id ORDER BY 1), 
# dendrite_type AS 
#     (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
#     AS dendrite_type 
#     FROM specimen_tags_specimens sts 
#     JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
#     WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
#     'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
#     GROUP BY sts.specimen_id), 
# swc AS 
#     (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
#     AS swc_filename, marker.storage_directory || marker.filename 
#     AS marker_filename, swc.published_at AS published_at 
#     FROM neuron_reconstructions nr 
#     JOIN well_known_files swc ON swc.attachable_id = nr.id 
#     AND swc.well_known_file_type_id = 303941301 
#     LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
#     AND marker.well_known_file_type_id = 486753749 
#     WHERE nr.superseded = false AND nr.manual = true) 
# SELECT cell.name AS cell_name,
# cell.patched_cell_container AS container, 
# dendrite_type.dendrite_type, 
# do_63x.go, 
# imgs63.image_series_id AS image_series_63x_id, 
# imgs63.workflow_state AS image_series_63x_qc, 
# swc.swc_filename, 
# proj.code,
# TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
# err.initial_access_resistance_mohm AS initial_access, 
# err.published_at, 
# err.workflow_state, 
# CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
# CONCAT(nwb.storage_directory,  h5.filename) AS h5_filepath, 
# ra.failed AS amp_failed, 
# org.name AS organism, 
# CASE 
#         WHEN err.seal_gohm >= 1 
#             AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
#                 OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
#             AND err.initial_access_resistance_mohm < 20 
#             AND ABS(err.electrode_0_pa) < 100 
#             THEN 'good' 
#         ELSE 'bad' END AS cell_qc 
# FROM specimens cell 
# JOIN projects proj ON cell.project_id = proj.id 
# LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
# LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
# LEFT JOIN well_known_files h5 ON err.id = h5.attachable_id 
# LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
# LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
# LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
# LEFT JOIN donors d ON d.id = cell.donor_id 
# JOIN organisms org ON d.organism_id = org.id 
# LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
# LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
# LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
# LEFT JOIN swc ON cell.id = swc.cell_id 
# WHERE cell.patched_cell_container IS NOT NULL 
# AND proj.code NOT IN ('mMPATCH','T504', 'hIVSCC-METx', 'mIVSCC-METx', 'T301t')
# AND nwb.well_known_file_type_id = 475137571 
# AND h5.filename LIKE '%%.h5' 
# ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""


# met_query = """WITH imgs63 AS 
#     (SELECT max(id) AS image_series_id, 
#     max(workflow_state) AS workflow_state, 
#     specimen_id 
#     FROM image_series 
#     WHERE is_stack = true 
#     GROUP BY specimen_id), 
# do_63x(cell_id, go) AS 
#     (SELECT DISTINCT cell.id, 
#     array_to_string(array_agg(DISTINCT tag.name), 
#     ' _AND_ ') 
#     FROM specimens cell 
#     JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
#     JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
#     JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
#     (602120185,602122082) 
#     GROUP BY cell.id ORDER BY 1), 
# dendrite_type AS 
#     (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
#     AS dendrite_type 
#     FROM specimen_tags_specimens sts 
#     JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
#     WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
#     'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
#     GROUP BY sts.specimen_id), 
# swc AS 
#     (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
#     AS swc_filename, marker.storage_directory || marker.filename 
#     AS marker_filename, swc.published_at AS published_at 
#     FROM neuron_reconstructions nr 
#     JOIN well_known_files swc ON swc.attachable_id = nr.id 
#     AND swc.well_known_file_type_id = 303941301 
#     LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
#     AND marker.well_known_file_type_id = 486753749 
#     WHERE nr.superseded = false AND nr.manual = true) 
# SELECT cell.name AS cell_name,
# cell.patched_cell_container AS container, 
# dendrite_type.dendrite_type, 
# do_63x.go, 
# imgs63.image_series_id AS image_series_63x_id, 
# imgs63.workflow_state AS image_series_63x_qc, 
# swc.swc_filename, 
# proj.code,
# TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
# err.initial_access_resistance_mohm AS initial_access, 
# err.published_at, 
# err.workflow_state, 
# CONCAT(nwb.storage_directory,  nwb.filename) AS filepath, 
# ra.failed AS amp_failed, 
# org.name AS organism, 
# CASE 
#         WHEN err.seal_gohm >= 1 
#             AND ((err.input_access_resistance_ratio < 0.15 AND org.name = 'Mus musculus') 
#                 OR (err.input_access_resistance_ratio < 0.20 AND org.name = 'Homo Sapiens')) 
#             AND err.initial_access_resistance_mohm < 20 
#             AND ABS(err.electrode_0_pa) < 100 
#             THEN 'good' 
#         ELSE 'bad' END AS cell_qc 
# FROM specimens cell 
# JOIN projects proj ON cell.project_id = proj.id 
# LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
# LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
# LEFT JOIN rna_amplification_inputs rai ON cell.id = rai.sample_id 
# LEFT JOIN rna_amplifications ra ON rai.rna_amplification_id = ra.id 
# LEFT JOIN rna_amplification_sets ras ON ra.rna_amplification_set_id = ras.id 
# LEFT JOIN donors d ON d.id = cell.donor_id 
# JOIN organisms org ON d.organism_id = org.id 
# LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
# LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
# LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
# LEFT JOIN swc ON cell.id = swc.cell_id 
# WHERE cell.patched_cell_container IS NOT NULL 
# AND proj.code NOT IN ('mMPATCH','T504', 'hIVSCC-METx', 'mIVSCC-METx', 'T301t')
# AND nwb.well_known_file_type_id = 475137571 
# AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2017-10-02' 
# ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""

me_feature_query_all = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%%%' 
    GROUP BY dg.donor_id),
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name,
cell.id AS cell_id,
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.id as roi_id,
err.published_at, 
err.workflow_state, 
org.name AS organism, 
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
ef.*
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
JOIN ephys_features ef on ef.specimen_id = cell.id
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE proj.code IN ('T301')
AND nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2015-01-01' 
AND err.workflow_state = 'manual_passed'
AND err.published_at IS NOT NULL
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""

me_feature_query_wdriver = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%%%' 
    GROUP BY dg.donor_id),
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name,
cell.id AS cell_id,
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.id as roi_id,
err.published_at, 
err.workflow_state, 
org.name AS organism, 
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
ef.*
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
JOIN ephys_features ef on ef.specimen_id = cell.id
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE proj.code IN ('T301')
AND nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2015-01-01' 
AND err.workflow_state = 'manual_passed'
AND err.published_at IS NOT NULL
AND drv.drivers = '%(mydriver)s' 
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC"""


me_feature_query_wdriver_spikingsweep = """WITH imgs63 AS 
    (SELECT max(id) AS image_series_id, 
    max(workflow_state) AS workflow_state, 
    specimen_id 
    FROM image_series 
    WHERE is_stack = true 
    GROUP BY specimen_id), 
do_63x(cell_id, go) AS 
    (SELECT DISTINCT cell.id, 
    array_to_string(array_agg(DISTINCT tag.name), 
    ' _AND_ ') 
    FROM specimens cell 
    JOIN ephys_roi_results err ON err.id = cell.ephys_roi_result_id 
    JOIN specimen_tags_specimens sptagsp ON sptagsp.specimen_id = cell.id 
    JOIN specimen_tags tag ON tag.id = sptagsp.specimen_tag_id AND tag.id in 
    (602120185,602122082) 
    GROUP BY cell.id ORDER BY 1), 
reporters AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), ' ') AS reporters 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ilike 'reporter%%%%' 
    GROUP BY dg.donor_id),
drivers AS 
    (SELECT dg.donor_id,
    array_to_string(array_agg(DISTINCT g.name), '; ') AS drivers 
    FROM donors_genotypes dg 
    JOIN genotypes g ON dg.genotype_id = g.id 
    JOIN genotype_types gt ON g.genotype_type_id = gt.id 
    WHERE gt.name ILIKE 'driver%%%%' 
    GROUP BY dg.donor_id),
dendrite_type AS 
    (SELECT sts.specimen_id, array_to_string(array_agg(DISTINCT tag.name), ' ') 
    AS dendrite_type 
    FROM specimen_tags_specimens sts 
    JOIN specimen_tags tag ON sts.specimen_tag_id = tag.id 
    WHERE tag.name IN ('dendrite type - spiny', 'dendrite type - NA', 
    'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    GROUP BY sts.specimen_id), 
swc AS 
    (SELECT nr.specimen_id AS cell_id, swc.storage_directory || swc.filename 
    AS swc_filename, marker.storage_directory || marker.filename 
    AS marker_filename, swc.published_at AS published_at 
    FROM neuron_reconstructions nr 
    JOIN well_known_files swc ON swc.attachable_id = nr.id 
    AND swc.well_known_file_type_id = 303941301 
    LEFT JOIN well_known_files marker ON marker.attachable_id = nr.id 
    AND marker.well_known_file_type_id = 486753749 
    WHERE nr.superseded = false AND nr.manual = true) 
SELECT cell.name,
cell.id AS cell_id,
rpt.reporters, 
drv.drivers,
dendrite_type.dendrite_type, 
do_63x.go, 
imgs63.image_series_id AS image_series_63x_id, 
imgs63.workflow_state AS image_series_63x_qc, 
swc.swc_filename, 
proj.code,
TO_CHAR(err.recording_date,'YYYY-MM-DD') AS lims_date, 
err.id as roi_id,
err.published_at, 
err.workflow_state, 
org.name AS organism,
sw.sweep_number,
sw.stimulus_amplitude,
sw.num_spikes, 
CONCAT(nwb.storage_directory,  nwb.filename) AS nwb_filepath, 
ef.*
FROM specimens cell 
JOIN projects proj ON cell.project_id = proj.id 
JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
JOIN ephys_features ef on ef.specimen_id = cell.id
JOIN ephys_sweeps sw ON cell.id = sw.specimen_id
LEFT JOIN well_known_files nwb ON err.id = nwb.attachable_id 
LEFT JOIN donors d ON d.id = cell.donor_id 
JOIN organisms org ON d.organism_id = org.id 
LEFT JOIN reporters rpt ON cell.donor_id = rpt.donor_id 
LEFT JOIN drivers drv ON cell.donor_id = drv.donor_id
LEFT JOIN dendrite_type ON cell.id = dendrite_type.specimen_id 
LEFT JOIN imgs63 ON cell.id = imgs63.specimen_id 
LEFT JOIN do_63x ON do_63x.cell_id = cell.id 
LEFT JOIN swc ON cell.id = swc.cell_id 
WHERE proj.code IN ('T301')
AND nwb.well_known_file_type_id = 475137571 
AND TO_CHAR(err.recording_date,'YYYY-MM-DD') > '2015-01-01' 
AND err.workflow_state = 'manual_passed'
AND err.published_at IS NOT NULL
AND drv.drivers = '%(mydriver)s' 
AND sw.num_spikes = 2
ORDER BY TO_CHAR(err.recording_date,'YYYY-MM-DD') DESC LIMIT 10"""
