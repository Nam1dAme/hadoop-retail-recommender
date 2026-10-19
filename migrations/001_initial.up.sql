-- HL-DOC-1.0.0 / docs/06-database.md. No implicit audit-time defaults.

CREATE TABLE data_batch (
    batch_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
    source_url VARCHAR(1024) COLLATE utf8mb4_bin NOT NULL,
    source_bytes BIGINT NOT NULL,
    policy_version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    baseline_version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    schema_version VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    raw_hdfs_path VARCHAR(1024) COLLATE utf8mb4_bin NOT NULL,
    normalized_hdfs_path VARCHAR(1024) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    min_source_date DATE NULL,
    max_source_date DATE NULL,
    raw_rows BIGINT NULL,
    sale_rows BIGINT NULL,
    return_rows BIGINT NULL,
    rejected_rows BIGINT NULL,
    quality_report_path VARCHAR(1024) COLLATE utf8mb4_bin NULL,
    error_code VARCHAR(64) COLLATE utf8mb4_bin NULL,
    error_message VARCHAR(1024) COLLATE utf8mb4_bin NULL,
    PRIMARY KEY (batch_id),
    UNIQUE (source_sha256,policy_version),
    CHECK (status IN ('REGISTERED','PROCESSING','SUCCEEDED','FAILED')),
    CHECK (source_bytes >= 0),
    CHECK (raw_rows >= 0),
    CHECK (sale_rows >= 0),
    CHECK (return_rows >= 0),
    CHECK (rejected_rows >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE task_run (
    run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    batch_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    parent_run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    task_name VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
    attempt INT NOT NULL,
    status VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    config_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    code_commit VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    baseline_version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    started_at DATETIME(6) NULL,
    finished_at DATETIME(6) NULL,
    heartbeat_at DATETIME(6) NULL,
    application_id VARCHAR(64) COLLATE utf8mb4_bin NULL,
    pid BIGINT NULL,
    process_start_utc DATETIME(6) NULL,
    output_manifest_path VARCHAR(1024) COLLATE utf8mb4_bin NULL,
    log_path VARCHAR(1024) COLLATE utf8mb4_bin NULL,
    error_code VARCHAR(64) COLLATE utf8mb4_bin NULL,
    error_message VARCHAR(1024) COLLATE utf8mb4_bin NULL,
    PRIMARY KEY (run_id),
    FOREIGN KEY (batch_id) REFERENCES data_batch (batch_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (parent_run_id) REFERENCES task_run (run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE (batch_id,task_name,attempt),
    INDEX (batch_id,status,created_at),
    CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    CHECK (attempt >= 1),
    CHECK (pid > 0),
    CHECK ((pid IS NULL) = (process_start_utc IS NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE experiment (
    experiment_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    batch_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    split_policy_version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    train_end_exclusive DATE NOT NULL,
    validation_end_exclusive DATE NOT NULL,
    seed INT NOT NULL,
    config_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    baseline_version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    champion_algorithm VARCHAR(8) COLLATE utf8mb4_bin NULL,
    report_path VARCHAR(1024) COLLATE utf8mb4_bin NULL,
    test_sealed_at DATETIME(6) NULL,
    finished_at DATETIME(6) NULL,
    PRIMARY KEY (experiment_id),
    FOREIGN KEY (batch_id) REFERENCES data_batch (batch_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (status IN ('CREATED','RUNNING','SUCCEEDED','FAILED')),
    CHECK (champion_algorithm IN ('POP','ITEMCF','ALS','NCF')),
    CHECK (split_policy_version = 'time-split-v1'),
    CHECK (train_end_exclusive = '2011-09-01'),
    CHECK (validation_end_exclusive = '2011-11-01'),
    CHECK (seed = 42)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE model_run (
    model_run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    batch_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    experiment_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    algorithm VARCHAR(8) COLLATE utf8mb4_bin NOT NULL,
    purpose VARCHAR(8) COLLATE utf8mb4_bin NOT NULL,
    device VARCHAR(8) COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    config_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    mapping_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    artifact_path VARCHAR(1024) COLLATE utf8mb4_bin NOT NULL,
    code_commit VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    environment_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    baseline_version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6) NULL,
    best_epoch INT NULL,
    validation_ndcg DOUBLE NULL,
    hardware_summary VARCHAR(1024) COLLATE utf8mb4_bin NULL,
    PRIMARY KEY (model_run_id),
    FOREIGN KEY (batch_id) REFERENCES data_batch (batch_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (experiment_id) REFERENCES experiment (experiment_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES task_run (run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (algorithm IN ('POP','ITEMCF','ALS','NCF')),
    CHECK (purpose IN ('EVAL','FULLFIT')),
    CHECK (device IN ('CPU','CUDA')),
    CHECK (status IN ('CREATED','RUNNING','SUCCEEDED','FAILED')),
    CHECK (best_epoch >= 1),
    CHECK (validation_ndcg BETWEEN 0 AND 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE experiment_model (
    experiment_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    algorithm VARCHAR(8) COLLATE utf8mb4_bin NOT NULL,
    model_run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    selected_at DATETIME(6) NOT NULL,
    PRIMARY KEY (experiment_id,algorithm),
    FOREIGN KEY (experiment_id) REFERENCES experiment (experiment_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (model_run_id) REFERENCES model_run (model_run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (algorithm IN ('POP','ITEMCF','ALS','NCF'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE data_quality (
    batch_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    metric_name VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
    metric_value BIGINT NOT NULL,
    description VARCHAR(512) COLLATE utf8mb4_bin NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (batch_id,metric_name),
    FOREIGN KEY (batch_id) REFERENCES data_batch (batch_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (metric_value >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE model_epoch (
    model_run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    epoch INT NOT NULL,
    train_loss DOUBLE NOT NULL,
    elapsed_seconds DOUBLE NOT NULL,
    validation_loss DOUBLE NULL,
    validation_ndcg DOUBLE NULL,
    peak_gpu_mib DOUBLE NULL,
    PRIMARY KEY (model_run_id,epoch),
    FOREIGN KEY (model_run_id) REFERENCES model_run (model_run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (epoch >= 1),
    CHECK (train_loss >= 0),
    CHECK (elapsed_seconds >= 0),
    CHECK (validation_loss >= 0),
    CHECK (peak_gpu_mib >= 0),
    CHECK (validation_ndcg BETWEEN 0 AND 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE model_metric (
    model_run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    split_name VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    cohort VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    k INT NOT NULL,
    user_count BIGINT NOT NULL,
    catalog_count BIGINT NOT NULL,
    window_user_count BIGINT NOT NULL,
    precision_at_k DOUBLE NOT NULL,
    recall_at_k DOUBLE NOT NULL,
    ndcg_at_k DOUBLE NOT NULL,
    coverage DOUBLE NOT NULL,
    cold_user_count BIGINT NOT NULL,
    cold_item_count BIGINT NOT NULL,
    cold_interaction_count BIGINT NOT NULL,
    excluded_user_count BIGINT NOT NULL,
    candidate_policy_version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (model_run_id,split_name,cohort,k),
    FOREIGN KEY (model_run_id) REFERENCES model_run (model_run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (split_name IN ('VALIDATION','TEST')),
    CHECK (cohort IN ('WARM','ALL_ELIGIBLE')),
    CHECK (k = 10),
    CHECK (user_count > 0),
    CHECK (catalog_count > 0),
    CHECK (window_user_count >= 0),
    CHECK (cold_user_count >= 0),
    CHECK (cold_item_count >= 0),
    CHECK (cold_interaction_count >= 0),
    CHECK (excluded_user_count >= 0),
    CHECK (precision_at_k BETWEEN 0 AND 1),
    CHECK (recall_at_k BETWEEN 0 AND 1),
    CHECK (ndcg_at_k BETWEEN 0 AND 1),
    CHECK (coverage BETWEEN 0 AND 1),
    CHECK (candidate_policy_version = 'full-catalog-v1')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE gpu_benchmark (
    benchmark_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    model_run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    device VARCHAR(8) COLLATE utf8mb4_bin NOT NULL,
    repeat_index INT NOT NULL,
    seed INT NOT NULL,
    batch_size INT NOT NULL,
    epochs INT NOT NULL,
    warmup_steps INT NOT NULL,
    train_seconds DOUBLE NOT NULL,
    samples_per_second DOUBLE NOT NULL,
    peak_memory_mib DOUBLE NULL,
    hardware VARCHAR(1024) COLLATE utf8mb4_bin NOT NULL,
    torch_version VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
    cuda_runtime VARCHAR(64) COLLATE utf8mb4_bin NULL,
    driver_version VARCHAR(64) COLLATE utf8mb4_bin NULL,
    dataset_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    config_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (benchmark_id),
    FOREIGN KEY (model_run_id) REFERENCES model_run (model_run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE (model_run_id,device,repeat_index),
    CHECK (device IN ('CPU','CUDA')),
    CHECK (repeat_index BETWEEN 1 AND 3),
    CHECK (batch_size > 0),
    CHECK (epochs > 0),
    CHECK (train_seconds > 0),
    CHECK (warmup_steps >= 0),
    CHECK (samples_per_second >= 0),
    CHECK (peak_memory_mib >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE data_release (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    batch_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    experiment_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    champion_algorithm VARCHAR(8) COLLATE utf8mb4_bin NOT NULL,
    manifest_path VARCHAR(1024) COLLATE utf8mb4_bin NOT NULL,
    manifest_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    baseline_version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    published_at DATETIME(6) NULL,
    retired_at DATETIME(6) NULL,
    failure_reason VARCHAR(1024) COLLATE utf8mb4_bin NULL,
    PRIMARY KEY (release_id),
    FOREIGN KEY (batch_id) REFERENCES data_batch (batch_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (experiment_id) REFERENCES experiment (experiment_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (status IN ('BUILDING','READY','FAILED','RETIRED')),
    CHECK (champion_algorithm IN ('POP','ITEMCF','ALS','NCF')),
    INDEX (status,published_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE release_model (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    algorithm VARCHAR(8) COLLATE utf8mb4_bin NOT NULL,
    model_run_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (release_id,algorithm),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (model_run_id) REFERENCES model_run (model_run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE (release_id,model_run_id),
    CHECK (algorithm IN ('POP','ITEMCF','ALS','NCF'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE current_release (
    singleton_id TINYINT NOT NULL,
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (singleton_id),
    CHECK (singleton_id = 1),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO current_release (singleton_id,release_id,updated_at) VALUES (1,NULL,UTC_TIMESTAMP(6));

CREATE TABLE release_event (
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    action VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    from_release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NULL,
    to_release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NULL,
    reason VARCHAR(1024) COLLATE utf8mb4_bin NOT NULL,
    operator VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    FOREIGN KEY (from_release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (to_release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (action IN ('PUBLISH','ROLLBACK','RETIRE'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE product_dim (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    stock_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    description VARCHAR(512) COLLATE utf8mb4_bin NULL,
    is_merchandise BOOLEAN NOT NULL,
    is_candidate BOOLEAN NOT NULL,
    PRIMARY KEY (release_id,stock_code),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (is_merchandise IN (0,1)),
    CHECK (is_candidate IN (0,1)),
    CHECK (is_candidate <= is_merchandise)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE sales_cube (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_date DATE NOT NULL,
    country VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
    stock_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    gross_amount DECIMAL(20,2) NOT NULL,
    return_amount DECIMAL(20,2) NOT NULL,
    sale_quantity BIGINT NOT NULL,
    return_quantity BIGINT NOT NULL,
    PRIMARY KEY (release_id,source_date,country,stock_code),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,stock_code) REFERENCES product_dim (release_id,stock_code) ON DELETE RESTRICT ON UPDATE RESTRICT,
    INDEX (release_id,country,source_date),
    CHECK (gross_amount >= 0),
    CHECK (return_amount >= 0),
    CHECK (sale_quantity >= 0),
    CHECK (return_quantity >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE sale_order (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    invoice_no VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    source_date DATE NOT NULL,
    country VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
    customer_id VARCHAR(32) COLLATE utf8mb4_bin NULL,
    gross_amount DECIMAL(20,2) NOT NULL,
    PRIMARY KEY (release_id,invoice_no),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    INDEX (release_id,source_date,country),
    INDEX (release_id,country,source_date),
    INDEX (release_id,customer_id),
    CHECK (gross_amount >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE sale_order_item (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    invoice_no VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    stock_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    gross_amount DECIMAL(20,2) NOT NULL,
    quantity BIGINT NOT NULL,
    PRIMARY KEY (release_id,invoice_no,stock_code),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,invoice_no) REFERENCES sale_order (release_id,invoice_no) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,stock_code) REFERENCES product_dim (release_id,stock_code) ON DELETE RESTRICT ON UPDATE RESTRICT,
    INDEX (release_id,stock_code,invoice_no),
    CHECK (gross_amount >= 0),
    CHECK (quantity >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE customer_rfm (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    customer_id VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    last_purchase DATE NOT NULL,
    as_of_date DATE NOT NULL,
    recency_days INT NOT NULL,
    frequency INT NOT NULL,
    monetary DECIMAL(20,2) NOT NULL,
    r_score TINYINT NOT NULL,
    f_score TINYINT NOT NULL,
    m_score TINYINT NOT NULL,
    segment VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (release_id,customer_id),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    INDEX (release_id,segment,customer_id),
    CHECK (recency_days >= 0),
    CHECK (frequency >= 0),
    CHECK (monetary >= 0),
    CHECK (r_score BETWEEN 1 AND 5),
    CHECK (f_score BETWEEN 1 AND 5),
    CHECK (m_score BETWEEN 1 AND 5),
    CHECK (segment IN ('CORE','AT_RISK','NEW','POTENTIAL','OTHER'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE customer_seen_item (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    customer_id VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    stock_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (release_id,customer_id,stock_code),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,customer_id) REFERENCES customer_rfm (release_id,customer_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,stock_code) REFERENCES product_dim (release_id,stock_code) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE item_similarity (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    stock_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    related_stock_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    similarity DOUBLE NOT NULL,
    confidence DOUBLE NOT NULL,
    lift DOUBLE NOT NULL,
    co_count BIGINT NOT NULL,
    rank TINYINT NOT NULL,
    PRIMARY KEY (release_id,stock_code,related_stock_code),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,stock_code) REFERENCES product_dim (release_id,stock_code) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,related_stock_code) REFERENCES product_dim (release_id,stock_code) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE (release_id,stock_code,rank),
    CHECK (stock_code <> related_stock_code),
    CHECK (similarity BETWEEN 0 AND 1),
    CHECK (confidence BETWEEN 0 AND 1),
    CHECK (lift >= 0),
    CHECK (co_count >= 0),
    CHECK (rank BETWEEN 1 AND 10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE product_popularity (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    stock_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    order_count BIGINT NOT NULL,
    rank INT NOT NULL,
    PRIMARY KEY (release_id,stock_code),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,stock_code) REFERENCES product_dim (release_id,stock_code) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE (release_id,rank),
    CHECK (order_count >= 0),
    CHECK (rank >= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE customer_recommendation (
    release_id CHAR(23) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    algorithm VARCHAR(8) COLLATE utf8mb4_bin NOT NULL,
    customer_id VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    stock_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    rank TINYINT NOT NULL,
    score DOUBLE NOT NULL,
    reason_code VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
    is_fallback BOOLEAN NOT NULL,
    PRIMARY KEY (release_id,algorithm,customer_id,stock_code),
    FOREIGN KEY (release_id) REFERENCES data_release (release_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,algorithm) REFERENCES release_model (release_id,algorithm) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,customer_id) REFERENCES customer_rfm (release_id,customer_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (release_id,stock_code) REFERENCES product_dim (release_id,stock_code) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE (release_id,algorithm,customer_id,rank),
    CHECK (algorithm IN ('POP','ITEMCF','ALS','NCF')),
    CHECK (reason_code IN ('POPULAR','CO_PURCHASE','LATENT_FACTOR','NEURAL_CF','FALLBACK_POPULAR')),
    CHECK (rank BETWEEN 1 AND 10),
    CHECK (is_fallback IN (0,1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
