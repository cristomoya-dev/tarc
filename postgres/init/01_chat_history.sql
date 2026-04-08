-- 01_chat_history.sql
-- Historial de conversaciones del chat RAG
-- Ejecutar manualmente si la BD ya existe:
--   docker compose exec postgres psql -U tacrc tacrc -f /docker-entrypoint-initdb.d/01_chat_history.sql

CREATE TABLE IF NOT EXISTS conversaciones (
    id           SERIAL PRIMARY KEY,
    titulo       TEXT,                          -- generado automáticamente del primer mensaje
    creado_at    TIMESTAMPTZ DEFAULT NOW(),
    actualizado_at TIMESTAMPTZ DEFAULT NOW(),
    total_turnos SMALLINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mensajes (
    id               SERIAL PRIMARY KEY,
    conversacion_id  INT NOT NULL REFERENCES conversaciones(id) ON DELETE CASCADE,
    turno            SMALLINT NOT NULL,          -- 0, 1, 2... orden dentro de la conversación
    role             TEXT NOT NULL,              -- 'user' | 'assistant'
    contenido        TEXT NOT NULL,
    -- metadatos de la búsqueda (solo en mensajes assistant)
    anio_filtro      SMALLINT,
    sentido_filtro   TEXT,
    top_k            SMALLINT,
    fuentes_json     JSONB,                      -- lista de ResolucionSummary
    fragmentos_json  JSONB,                      -- lista de Fragmento
    creado_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mensajes_conv  ON mensajes (conversacion_id, turno);
CREATE INDEX IF NOT EXISTS idx_conv_updated   ON conversaciones (actualizado_at DESC);

-- Trigger para actualizar actualizado_at y total_turnos en conversaciones
CREATE OR REPLACE FUNCTION actualizar_conversacion()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    UPDATE conversaciones
    SET actualizado_at = NOW(),
        total_turnos   = (SELECT COUNT(*) FROM mensajes WHERE conversacion_id = NEW.conversacion_id)
    WHERE id = NEW.conversacion_id;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_mensajes_after_insert
    AFTER INSERT ON mensajes
    FOR EACH ROW EXECUTE FUNCTION actualizar_conversacion();
