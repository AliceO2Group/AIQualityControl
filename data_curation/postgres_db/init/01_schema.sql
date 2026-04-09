CREATE TABLE IF NOT EXISTS bookkeeping_lhc_fills (
    fill_number BIGINT PRIMARY KEY,
    stable_beams_start BIGINT,
    stable_beams_end BIGINT,
    stable_beams_duration BIGINT,
    beam_type TEXT,
    filling_scheme_name TEXT,
    colliding_bunches_count INTEGER,
    delivered_luminosity NUMERIC,
    statistics_json JSONB,
    metadata_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bookkeeping_runs (
    run_number BIGINT PRIMARY KEY,
    id BIGINT,
    fill_number BIGINT NOT NULL REFERENCES bookkeeping_lhc_fills(fill_number),
    time_o2_start BIGINT,
    time_o2_end BIGINT,
    time_trg_start BIGINT,
    time_trg_end BIGINT,
    start_time BIGINT,
    end_time BIGINT,
    qc_time_start BIGINT,
    qc_time_end BIGINT,
    run_duration BIGINT,
    environment_id TEXT,
    updated_at BIGINT,
    run_type INTEGER,
    definition TEXT,
    calibration_status TEXT,
    run_quality TEXT,
    n_detectors INTEGER,
    n_flps INTEGER,
    n_epns INTEGER,
    lhc_beam_energy NUMERIC,
    lhc_beam_mode TEXT,
    lhc_beta_star NUMERIC,
    pdp_beam_type TEXT,
    pdp_workflow_parameters TEXT,
    trigger_value TEXT,
    start_of_data_transfer BIGINT,
    end_of_data_transfer BIGINT,
    ctf_file_count INTEGER,
    ctf_file_size NUMERIC,
    tf_file_count INTEGER,
    tf_file_size NUMERIC,
    other_file_count INTEGER,
    other_file_size NUMERIC,
    cross_section NUMERIC,
    trigger_efficiency NUMERIC,
    trigger_acceptance NUMERIC,
    eor_reasons_json JSONB,
    detectors_qualities_json JSONB,
    tags_json JSONB,
    qc_flags_json JSONB,
    metadata_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bookkeeping_run_logs (
    log_id BIGINT PRIMARY KEY,
    run_number BIGINT NOT NULL REFERENCES bookkeeping_runs(run_number) ON DELETE CASCADE,
    title TEXT,
    text TEXT,
    author_name TEXT,
    created_at BIGINT,
    origin TEXT,
    subtype TEXT,
    root_log_id BIGINT,
    parent_log_id BIGINT,
    tags_json JSONB,
    payload_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lhc_fills_beam_type
ON bookkeeping_lhc_fills (beam_type);

CREATE INDEX IF NOT EXISTS idx_lhc_fills_metadata_gin
ON bookkeeping_lhc_fills USING GIN (metadata_json);

CREATE INDEX IF NOT EXISTS idx_runs_fill_number
ON bookkeeping_runs (fill_number);

CREATE INDEX IF NOT EXISTS idx_runs_lhc_beam_mode
ON bookkeeping_runs (lhc_beam_mode);

CREATE INDEX IF NOT EXISTS idx_runs_pdp_beam_type
ON bookkeeping_runs (pdp_beam_type);

CREATE INDEX IF NOT EXISTS idx_runs_run_quality
ON bookkeeping_runs (run_quality);

CREATE INDEX IF NOT EXISTS idx_runs_metadata_gin
ON bookkeeping_runs USING GIN (metadata_json);

CREATE INDEX IF NOT EXISTS idx_runs_tags_gin
ON bookkeeping_runs USING GIN (tags_json);

CREATE INDEX IF NOT EXISTS idx_runs_qc_flags_gin
ON bookkeeping_runs USING GIN (qc_flags_json);

CREATE INDEX IF NOT EXISTS idx_run_logs_run_number
ON bookkeeping_run_logs (run_number);

CREATE INDEX IF NOT EXISTS idx_run_logs_payload_gin
ON bookkeeping_run_logs USING GIN (payload_json);
