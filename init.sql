-- =============================================================================
-- Esquema de persistencia para el sistema de alertas.
-- Se ejecuta automáticamente la PRIMERA vez que arranca Postgres
-- (cuando el volumen pgdata está vacío).
-- Para recrear la BD desde cero: ./start.sh --reset  (borra el volumen).
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS alertas_emergencia (
    alert_id              TEXT PRIMARY KEY,
    patient_id            TEXT             NOT NULL,
    max_heart_rate        INTEGER          NOT NULL,
    min_oxygen_saturation INTEGER          NOT NULL,
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    window_start          TEXT,
    window_end            TEXT,
    alert_time            BIGINT           NOT NULL,   -- epoch en segundos
    inserted_at           TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_time_desc ON alertas_emergencia (alert_time DESC);
CREATE INDEX IF NOT EXISTS idx_patient_time    ON alertas_emergencia (patient_id, alert_time DESC);

-- Nota: si en el futuro quieres activar TimescaleDB como hypertable,
-- puedes hacerlo manualmente una vez que los datos ya existan:
--   SELECT create_hypertable('alertas_emergencia', 'inserted_at',
--                            if_not_exists => TRUE, migrate_data => TRUE);
-- (se deja fuera de init.sql para no chocar con el UPSERT desde Flink).
