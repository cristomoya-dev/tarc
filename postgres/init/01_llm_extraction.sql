-- 01_llm_extraction.sql

ALTER TABLE resoluciones
    ADD COLUMN IF NOT EXISTS organo_contratacion      TEXT,
    ADD COLUMN IF NOT EXISTS importe_contrato         NUMERIC,
    ADD COLUMN IF NOT EXISTS tipo_contrato_llm        TEXT,
    ADD COLUMN IF NOT EXISTS llm_extraido_at          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS objeto_contrato          TEXT,
    ADD COLUMN IF NOT EXISTS recurrente               TEXT,
    ADD COLUMN IF NOT EXISTS importe_resumen          TEXT,
    ADD COLUMN IF NOT EXISTS razones_recurso_json     JSONB,
    ADD COLUMN IF NOT EXISTS razones_oposicion_json   JSONB,
    ADD COLUMN IF NOT EXISTS razones_tribunal_json    JSONB,
    ADD COLUMN IF NOT EXISTS fallo                    TEXT;

CREATE INDEX IF NOT EXISTS idx_res_tipo_contrato_llm 
    ON resoluciones (tipo_contrato_llm);

-- 🔴 SOLUCIÓN: eliminar la vista antes de recrearla
DROP VIEW IF EXISTS v_estado_ingesta;

CREATE VIEW v_estado_ingesta AS
SELECT
    COUNT(*)                                                        AS total,
    COUNT(*) FILTER (WHERE pdf_ingestado_at IS NOT NULL)           AS con_texto_pdf,
    COUNT(*) FILTER (WHERE embedding IS NOT NULL)                   AS con_embedding,
    COUNT(*) FILTER (WHERE llm_extraido_at IS NOT NULL)            AS con_extraccion_llm,
    COUNT(*) FILTER (WHERE pdf_url IS NOT NULL
                       AND pdf_ingestado_at IS NULL)                AS pendientes_pdf,
    COUNT(*) FILTER (WHERE pdf_ingestado_at IS NOT NULL
                       AND embedding IS NULL)                       AS pendientes_embedding,
    COUNT(*) FILTER (WHERE texto_pdf IS NOT NULL
                       AND llm_extraido_at IS NULL)                 AS pendientes_llm,
    MIN(fecha)                                                      AS fecha_mas_antigua,
    MAX(fecha)                                                      AS fecha_mas_reciente
FROM resoluciones;