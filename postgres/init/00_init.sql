-- 00_init.sql
-- Ejecutado automáticamente por PostgreSQL al crear la base de datos por primera vez.

-- ── Extensiones ──────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS vector;          -- pgvector: búsqueda semántica
CREATE EXTENSION IF NOT EXISTS pg_trgm;         -- trigramas: búsqueda de texto libre rápida
CREATE EXTENSION IF NOT EXISTS unaccent;        -- normalización de acentos para búsquedas

-- ── Tabla principal de resoluciones ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS resoluciones (
    id               SERIAL PRIMARY KEY,

    -- Metadatos extraídos por la extensión Chrome
    numero           TEXT        NOT NULL UNIQUE,   -- '0340/2026'
    fecha            DATE,                           -- 2026-03-26
    anio             SMALLINT GENERATED ALWAYS AS (EXTRACT(YEAR FROM fecha)::SMALLINT) STORED,
    tipo_recurso     TEXT,                           -- 'Recurso contra adjudicación en contrato de servicios'
    ley_impugnada    TEXT,                           -- 'LCSP'
    sentido          TEXT,                           -- 'Estimación parcial'
    descripcion      TEXT,                           -- resto del texto estructurado
    pdf_size         TEXT,                           -- '0.32MB'
    pdf_url          TEXT,

    -- Texto completo extraído del PDF
    texto_pdf        TEXT,
    pdf_paginas      SMALLINT,
    pdf_ingestado_at TIMESTAMPTZ,

    -- Embedding del documento completo (metadatos + texto PDF)
    embedding        vector(1536),                  -- dimensión de text-embedding-3-small
    embedding_at     TIMESTAMPTZ,

    -- Control
    creado_at        TIMESTAMPTZ DEFAULT NOW(),
    actualizado_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ── Tabla de chunks de PDF (para RAG con documentos largos) ─────────────────
-- Cada resolución se divide en trozos de ~500 tokens para recuperación precisa
CREATE TABLE IF NOT EXISTS chunks (
    id              SERIAL PRIMARY KEY,
    resolucion_id   INT         NOT NULL REFERENCES resoluciones(id) ON DELETE CASCADE,
    chunk_index     SMALLINT    NOT NULL,            -- posición dentro del documento
    texto           TEXT        NOT NULL,
    embedding       vector(1536),
    creado_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (resolucion_id, chunk_index)
);

-- ── Índices de filtrado rápido (metadatos) ───────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_res_anio        ON resoluciones (anio);
CREATE INDEX IF NOT EXISTS idx_res_fecha       ON resoluciones (fecha DESC);
CREATE INDEX IF NOT EXISTS idx_res_sentido     ON resoluciones (sentido);
CREATE INDEX IF NOT EXISTS idx_res_ley         ON resoluciones (ley_impugnada);

-- Búsqueda de texto libre con trigramas (sin índice vectorial)
CREATE INDEX IF NOT EXISTS idx_res_descripcion_trgm  ON resoluciones USING GIN (descripcion gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_res_tipo_trgm         ON resoluciones USING GIN (tipo_recurso gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_res_texto_pdf_trgm    ON resoluciones USING GIN (texto_pdf gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_chunks_texto_trgm     ON chunks       USING GIN (texto gin_trgm_ops);

-- ── Índices HNSW para búsqueda vectorial (creados tras la ingesta) ───────────
-- Se crean aquí vacíos; pgvector los construye incrementalmente.
-- Usa cosine distance — adecuado para embeddings de OpenAI (normalizados)
CREATE INDEX IF NOT EXISTS idx_res_embedding   ON resoluciones USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS idx_chunks_embedding ON chunks USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- ── Trigger: actualizar updated_at automáticamente ──────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.actualizado_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_resoluciones_updated_at
    BEFORE UPDATE ON resoluciones
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── Vista: estado de ingesta ─────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_estado_ingesta AS
SELECT
    COUNT(*)                                        AS total,
    COUNT(*) FILTER (WHERE pdf_ingestado_at IS NOT NULL) AS con_texto_pdf,
    COUNT(*) FILTER (WHERE embedding IS NOT NULL)        AS con_embedding,
    COUNT(*) FILTER (WHERE pdf_url IS NOT NULL AND pdf_ingestado_at IS NULL) AS pendientes_pdf,
    COUNT(*) FILTER (WHERE pdf_ingestado_at IS NOT NULL AND embedding IS NULL) AS pendientes_embedding,
    MIN(fecha)                                      AS fecha_mas_antigua,
    MAX(fecha)                                      AS fecha_mas_reciente
FROM resoluciones;

-- ── Vista: distribución por año y sentido (útil para dashboards) ─────────────
CREATE OR REPLACE VIEW v_stats_anio_sentido AS
SELECT
    anio,
    sentido,
    COUNT(*) AS total
FROM resoluciones
WHERE anio IS NOT NULL
GROUP BY anio, sentido
ORDER BY anio DESC, total DESC;
