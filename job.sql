-- =============================================================================
-- Job Flink SQL: detección de emergencias médicas (taquicardia + hipoxemia)
--
-- sensor-vital-data --> ventana TUMBLE 15 s por paciente --> fan-out:
--      ├── alertas-emergencia        (Kafka, para el dashboard en vivo)
--      └── alertas_emergencia (PG)   (Postgres/TimescaleDB, persistencia)
--
-- Regla: MAX(heart_rate) > 130  AND  MIN(oxygen_saturation) < 90
-- =============================================================================

SET 'pipeline.name' = 'health-alerts-job';
SET 'execution.runtime-mode' = 'streaming';
-- Si una partición queda sin datos, no bloquea el avance del watermark
SET 'table.exec.source.idle-timeout' = '5 s';

-- ------------------------------ ORIGEN ---------------------------------------
CREATE TABLE sensor_vitals (
    patient_id        STRING,
    heart_rate        INT,
    oxygen_saturation INT,
    temperature       DOUBLE,
    latitude          DOUBLE,
    longitude         DOUBLE,
    event_ts          BIGINT,                                   -- epoch en ms
    event_time AS TO_TIMESTAMP_LTZ(event_ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-vital-data',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-health-alerts',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- ------------------------------ DESTINO KAFKA --------------------------------
CREATE TABLE alertas_emergencia (
    alert_id              STRING,
    patient_id            STRING,
    max_heart_rate        INT,
    min_oxygen_saturation INT,
    latitude              DOUBLE,
    longitude             DOUBLE,
    window_start          STRING,
    window_end            STRING,
    alert_time            BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'alertas-emergencia',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- ------------------------------ DESTINO POSTGRES -----------------------------
-- PRIMARY KEY NOT ENFORCED habilita el modo UPSERT del conector JDBC,
-- lo que hace la escritura idempotente ante reintentos de Flink.
CREATE TABLE alertas_postgres (
    alert_id              STRING,
    patient_id            STRING,
    max_heart_rate        INT,
    min_oxygen_saturation INT,
    latitude              DOUBLE,
    longitude             DOUBLE,
    window_start          STRING,
    window_end            STRING,
    alert_time            BIGINT,
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector'                  = 'jdbc',
    'url'                        = 'jdbc:postgresql://postgres:5432/health',
    'table-name'                 = 'alertas_emergencia',
    'username'                   = 'health',
    'password'                   = 'health',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries'           = '3'
);

-- ------------------------------ PROCESAMIENTO --------------------------------
-- alert_id es determinista (patient_id|window_start|window_end) para que el
-- UPSERT en Postgres sea idempotente y no duplique si Flink reintenta.
CREATE TEMPORARY VIEW alerts_detected AS
SELECT
    CONCAT(
        patient_id, '|',
        CAST(window_start AS STRING), '|',
        CAST(window_end   AS STRING)
    )                                AS alert_id,
    patient_id,
    max_hr                           AS max_heart_rate,
    min_spo2                         AS min_oxygen_saturation,
    avg_lat                          AS latitude,
    avg_lon                          AS longitude,
    CAST(window_start AS STRING)     AS window_start,
    CAST(window_end   AS STRING)     AS window_end,
    UNIX_TIMESTAMP()                 AS alert_time
FROM (
    SELECT
        patient_id,
        window_start,
        window_end,
        MAX(heart_rate)        AS max_hr,
        MIN(oxygen_saturation) AS min_spo2,
        AVG(latitude)          AS avg_lat,
        AVG(longitude)         AS avg_lon
    FROM TABLE(
        TUMBLE(TABLE sensor_vitals, DESCRIPTOR(event_time), INTERVAL '15' SECOND)
    )
    GROUP BY patient_id, window_start, window_end
    HAVING MAX(heart_rate) > 130 AND MIN(oxygen_saturation) < 90
);

-- Fan-out: un solo job, dos sinks (Kafka + Postgres), fuente leída una vez.
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO alertas_emergencia
    SELECT alert_id, patient_id, max_heart_rate, min_oxygen_saturation,
           latitude, longitude, window_start, window_end, alert_time
    FROM alerts_detected;

    INSERT INTO alertas_postgres
    SELECT alert_id, patient_id, max_heart_rate, min_oxygen_saturation,
           latitude, longitude, window_start, window_end, alert_time
    FROM alerts_detected;
END;