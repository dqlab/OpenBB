-- Additive, offline-only dataset. Never changes a_share tables or views.
CREATE SCHEMA baostock18_admission;
CREATE TABLE baostock18_admission.schema_version (
 version INTEGER PRIMARY KEY, sha256 VARCHAR NOT NULL, applied_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE baostock18_admission.offline_store (
 singleton BOOLEAN PRIMARY KEY CHECK(singleton), contract VARCHAR NOT NULL
);
INSERT INTO baostock18_admission.offline_store VALUES (true, 'baostock18-offline-v1');
CREATE TABLE baostock18_admission.snapshot (
 snapshot_id VARCHAR PRIMARY KEY, contract_id VARCHAR NOT NULL,
 manifest_sha256 VARCHAR NOT NULL UNIQUE, manifest_json JSON NOT NULL,
 transform_version VARCHAR NOT NULL, known_at TIMESTAMPTZ NOT NULL,
 source_available_at TIMESTAMPTZ CHECK(source_available_at IS NULL),
 availability_basis VARCHAR NOT NULL,
 state VARCHAR NOT NULL CHECK(state IN ('staging','ready')),
 expected_rows BIGINT NOT NULL CHECK(expected_rows>=0), completed_at TIMESTAMPTZ
);
CREATE TABLE baostock18_admission.literal_version (
 snapshot_id VARCHAR NOT NULL REFERENCES baostock18_admission.snapshot(snapshot_id),
 value_id VARCHAR NOT NULL, symbol VARCHAR NOT NULL, source_date VARCHAR NOT NULL,
 requested_adjustflag VARCHAR NOT NULL, source_adjustflag VARCHAR NOT NULL,
 raw18_json JSON NOT NULL, flags_json JSON NOT NULL,
 disposition VARCHAR NOT NULL CHECK(disposition IN ('clean','conditional','quarantine')),
 PRIMARY KEY(snapshot_id,value_id)
);
CREATE TABLE baostock18_admission.row_lineage (
 snapshot_id VARCHAR NOT NULL, value_id VARCHAR NOT NULL,
 capture_id VARCHAR NOT NULL, capture_sha256 VARCHAR NOT NULL,
 capture_page INTEGER NOT NULL, page_row_index INTEGER NOT NULL,
 capture_row_index INTEGER NOT NULL, source_body_sha256 VARCHAR NOT NULL,
 source_receipt_sha256 VARCHAR NOT NULL, source_path VARCHAR NOT NULL,
 source_ok BOOLEAN NOT NULL, complete BOOLEAN NOT NULL,
 confirmed_partial_data BOOLEAN NOT NULL,
 historical_status VARCHAR, terminal_status VARCHAR,
 authority_received_ns BIGINT, collector_observed_at TIMESTAMPTZ,
 source_available_at TIMESTAMPTZ CHECK(source_available_at IS NULL),
 lineage_json JSON NOT NULL, capture_json JSON NOT NULL,
 PRIMARY KEY(snapshot_id,capture_id,capture_page,page_row_index),
 FOREIGN KEY(snapshot_id,value_id) REFERENCES baostock18_admission.literal_version(snapshot_id,value_id)
);
CREATE TABLE baostock18_admission.quality (
 snapshot_id VARCHAR NOT NULL, value_id VARCHAR NOT NULL,
 ruleset_version VARCHAR NOT NULL, code VARCHAR NOT NULL,
 disposition VARCHAR NOT NULL, detail_json JSON NOT NULL,
 PRIMARY KEY(snapshot_id,value_id,ruleset_version,code),
 FOREIGN KEY(snapshot_id,value_id) REFERENCES baostock18_admission.literal_version(snapshot_id,value_id)
);
CREATE TABLE baostock18_admission.bar_version (
 snapshot_id VARCHAR NOT NULL, value_id VARCHAR NOT NULL,
 symbol VARCHAR NOT NULL, date DATE NOT NULL,
 adjustment_basis VARCHAR NOT NULL CHECK(adjustment_basis IN ('unadjusted','forward_dynamic_snapshot')),
 requested_adjustflag VARCHAR NOT NULL CHECK(requested_adjustflag IN ('2','3')),
 source_adjustflag VARCHAR NOT NULL CHECK(source_adjustflag=requested_adjustflag),
 fixed_qfq_basis_date DATE CHECK(fixed_qfq_basis_date IS NULL),
 open DECIMAL(38,18) NOT NULL CHECK(open>0), high DECIMAL(38,18) NOT NULL,
 low DECIMAL(38,18) NOT NULL CHECK(low>0), close DECIMAL(38,18) NOT NULL,
 preclose DECIMAL(38,18), volume BIGINT CHECK(volume>=0),
 amount DECIMAL(38,18) CHECK(amount>=0), turnover_rate DECIMAL(38,18),
 change_percent DECIMAL(38,18), pe_ttm DECIMAL(38,18), ps_ttm DECIMAL(38,18),
 pcf_ttm DECIMAL(38,18), pb_mrq DECIMAL(38,18),
 trade_status INTEGER NOT NULL CHECK(trade_status IN (0,1)), is_st BOOLEAN,
 currency VARCHAR NOT NULL CHECK(currency='CNY'),
 volume_unit VARCHAR NOT NULL CHECK(volume_unit='shares'),
 session_timezone VARCHAR NOT NULL CHECK(session_timezone='Asia/Shanghai'),
 CHECK((requested_adjustflag='3' AND adjustment_basis='unadjusted') OR
       (requested_adjustflag='2' AND adjustment_basis='forward_dynamic_snapshot')),
 CHECK(low<=least(open,close) AND high>=greatest(open,close)),
 PRIMARY KEY(snapshot_id,value_id), UNIQUE(snapshot_id,symbol,date,adjustment_basis),
 FOREIGN KEY(snapshot_id,value_id) REFERENCES baostock18_admission.literal_version(snapshot_id,value_id)
);
CREATE TABLE baostock18_admission.batch_receipt (
 snapshot_id VARCHAR NOT NULL REFERENCES baostock18_admission.snapshot(snapshot_id),
 batch_number INTEGER NOT NULL, last_value_id VARCHAR NOT NULL,
 rows BIGINT NOT NULL, clean BIGINT NOT NULL, conditional BIGINT NOT NULL,
 quarantine BIGINT NOT NULL, lineage_rows BIGINT NOT NULL, batch_sha256 VARCHAR NOT NULL,
 committed_at TIMESTAMPTZ NOT NULL, elapsed_seconds DOUBLE NOT NULL,
 CHECK(rows=clean+conditional+quarantine), PRIMARY KEY(snapshot_id,batch_number)
);
CREATE TABLE baostock18_admission.basis_admission (
 snapshot_id VARCHAR NOT NULL REFERENCES baostock18_admission.snapshot(snapshot_id),
 adjustment_basis VARCHAR NOT NULL CHECK(adjustment_basis='unadjusted'),
 admitted_at TIMESTAMPTZ NOT NULL, clean_rows BIGINT NOT NULL CHECK(clean_rows>0),
 PRIMARY KEY(snapshot_id,adjustment_basis)
);
CREATE TABLE baostock18_admission.visibility_event (
 event_id BIGINT PRIMARY KEY, adjustment_basis VARCHAR NOT NULL,
 snapshot_id VARCHAR, known_at TIMESTAMPTZ NOT NULL UNIQUE,
 FOREIGN KEY(snapshot_id,adjustment_basis)
 REFERENCES baostock18_admission.basis_admission(snapshot_id,adjustment_basis)
);
CREATE TABLE baostock18_admission.dataset_pointer (
 adjustment_basis VARCHAR PRIMARY KEY, snapshot_id VARCHAR,
 event_id BIGINT NOT NULL REFERENCES baostock18_admission.visibility_event(event_id),
 FOREIGN KEY(snapshot_id,adjustment_basis)
 REFERENCES baostock18_admission.basis_admission(snapshot_id,adjustment_basis)
);
CREATE VIEW baostock18_admission.consumable AS
 SELECT b.*, l.raw18_json, l.flags_json, 'unadjusted' AS base_adjustment,
 e.known_at, s.source_available_at, s.availability_basis
 FROM baostock18_admission.dataset_pointer p
 JOIN baostock18_admission.visibility_event e USING(event_id)
 JOIN baostock18_admission.basis_admission a
   ON a.snapshot_id=p.snapshot_id AND a.adjustment_basis=p.adjustment_basis
 JOIN baostock18_admission.snapshot s ON s.snapshot_id=p.snapshot_id AND s.state='ready'
 JOIN baostock18_admission.bar_version b
   ON b.snapshot_id=p.snapshot_id AND b.adjustment_basis=p.adjustment_basis
 JOIN baostock18_admission.literal_version l ON l.snapshot_id=b.snapshot_id AND l.value_id=b.value_id
 WHERE l.disposition='clean';
