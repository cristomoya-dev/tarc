# main.py — FastAPI: búsqueda híbrida + chat RAG sobre resoluciones TACRC
import asyncio
import json
import logging
from typing import Optional, List
import io
import pdfplumber
from fastapi import FastAPI, Depends, Query, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from openai import AsyncOpenAI

from config import settings
from db import get_db

logging.basicConfig(level=settings.log_level.upper())
log = logging.getLogger(__name__)

app = FastAPI(
    title="TACRC API",
    description="Búsqueda semántica e híbrida de resoluciones del TACRC",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

openai = AsyncOpenAI(api_key=settings.openai_api_key)

# Reranker (flashrank — modelo ligero, sin GPU, carga en arranque)
try:
    from flashrank import Ranker, RerankRequest
    reranker = Ranker(model_name="ms-marco-MiniLM-L-12-v2", cache_dir="/tmp/flashrank")
    RERANK_ENABLED = True
    log.info("Reranker cargado correctamente")
except Exception as e:
    RERANK_ENABLED = False
    log.warning(f"Reranker no disponible: {e}")


# ── Helpers ──────────────────────────────────────────────────────────────────

# Caché LRU en memoria para embeddings de consultas (máx 500 entradas)
_embedding_cache: dict[str, list[float]] = {}
_EMBED_CACHE_MAX = 500


async def get_embedding(text_input: str) -> list[float]:
    key = text_input[:8000]
    if key in _embedding_cache:
        log.debug("Embedding cache hit")
        return _embedding_cache[key]
    resp = await openai.embeddings.create(
        model=settings.embed_model,
        input=key,
    )
    emb = resp.data[0].embedding
    if len(_embedding_cache) >= _EMBED_CACHE_MAX:
        _embedding_cache.pop(next(iter(_embedding_cache)))
    _embedding_cache[key] = emb
    return emb


def fmt_vector(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


# ── Modelos Pydantic ──────────────────────────────────────────────────────────

class ResolucionSummary(BaseModel):
    id:            int
    numero:        str
    fecha:         Optional[str]
    anio:          Optional[int]
    tipo_recurso:  Optional[str]
    ley_impugnada: Optional[str]
    sentido:       Optional[str]
    descripcion:   Optional[str]
    pdf_url:       Optional[str]
    score:         Optional[float] = None

class SearchResponse(BaseModel):
    total:       int
    resultados:  list[ResolucionSummary]

class ChatRequest(BaseModel):
    pregunta:        str
    anio:            Optional[int]   = None
    sentido:         Optional[str]   = None
    top_k:           int             = 8
    conversacion_id: Optional[int]   = None   # None = nueva conversación

class Fragmento(BaseModel):
    numero:   str
    fecha:    Optional[str]
    sentido:  Optional[str]
    texto:    str                # texto exacto del chunk
    score:    Optional[float]    # score de reranking (0-1)
    pdf_url:  Optional[str]

class ChatResponse(BaseModel):
    respuesta:       str
    fuentes:         list[ResolucionSummary]
    fragmentos:      list[Fragmento] = []
    conversacion_id: Optional[int]   = None
    turno:           int             = 0
    sugerencias:     list[str]       = []    # 3 preguntas de seguimiento

class ConversacionSummary(BaseModel):
    id:            int
    titulo:        Optional[str]
    creado_at:     str
    actualizado_at: str
    total_turnos:  int

class MensajeHistorial(BaseModel):
    turno:      int
    role:       str
    contenido:  str
    fuentes:    list[ResolucionSummary] = []
    fragmentos: list[Fragmento]         = []

class ResumenResolucion(BaseModel):
    numero:              str
    tipo_contrato:       Optional[str] = None
    objeto:              Optional[str] = None
    importe:             Optional[str] = None
    recurrente:          Optional[str] = None
    organo_contratacion: Optional[str] = None
    razones_recurso:     list[str]     = []
    razones_oposicion:   list[str]     = []
    razones_tribunal:    list[str]     = []
    fallo:               Optional[str] = None


# ── Endpoint: health ─────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Endpoint: estadísticas generales ─────────────────────────────────────────

@app.get("/stats")
async def stats(db: AsyncSession = Depends(get_db)):
    try:
        row = (await db.execute(text("SELECT * FROM v_estado_ingesta"))).mappings().one()
        dist = (await db.execute(text(
            "SELECT anio, sentido, total FROM v_stats_anio_sentido"
        ))).mappings().all()
        return {
            "ingesta": dict(row),
            "distribucion": [dict(r) for r in dist],
        }
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {e}")
        return {"ingesta": {}, "distribucion": []}


# ── Endpoint: valores únicos para filtros del frontend ───────────────────────

@app.get("/filtros")
async def filtros(db: AsyncSession = Depends(get_db)):
    sentidos = (await db.execute(text(
        "SELECT DISTINCT sentido FROM resoluciones WHERE sentido IS NOT NULL ORDER BY sentido"
    ))).scalars().all()
    leyes = (await db.execute(text(
        "SELECT DISTINCT ley_impugnada FROM resoluciones WHERE ley_impugnada IS NOT NULL ORDER BY ley_impugnada"
    ))).scalars().all()
    anios = (await db.execute(text(
        "SELECT DISTINCT anio FROM resoluciones WHERE anio IS NOT NULL ORDER BY anio DESC"
    ))).scalars().all()
    return {"sentidos": sentidos, "leyes": leyes, "anios": anios}


# ── Endpoint: búsqueda híbrida (SQL + vectorial, fusión RRF) ─────────────────

@app.get("/buscar", response_model=SearchResponse)
async def buscar(
    q:             Optional[str]  = Query(None),
    modo:          str            = Query("semantico", description="semantico | exacto"),
    anio:          Optional[int]  = Query(None),
    sentido:       Optional[str]  = Query(None),
    ley:           Optional[str]  = Query(None),
    fecha_desde:   Optional[str]  = Query(None),
    fecha_hasta:   Optional[str]  = Query(None),
    page:          int            = Query(1,  ge=1),
    page_size:     int            = Query(20, ge=1, le=100),
    db:            AsyncSession   = Depends(get_db),
):
    # Timeout de query a 28s para que la API responda antes del cierre del frontend (30s)
    await db.execute(text("SET LOCAL statement_timeout = '28s'"))
    # Umbral bajo para text_rank: permite usar el índice GIN trigrama con % operator
    await db.execute(text("SET LOCAL pg_trgm.similarity_threshold = 0.05"))

    offset = (page - 1) * page_size

    # ── Filtros estructurados (siempre aplican) ───────────────────────────────
    filters = []
    params: dict = {}

    if anio:
        filters.append("anio = :anio")
        params["anio"] = anio
    if sentido:
        filters.append("sentido ILIKE :sentido")
        params["sentido"] = f"%{sentido}%"
    if ley:
        filters.append("ley_impugnada ILIKE :ley")
        params["ley"] = f"%{ley}%"
    if fecha_desde:
        filters.append("fecha >= :fecha_desde::date")
        params["fecha_desde"] = fecha_desde
    if fecha_hasta:
        filters.append("fecha <= :fecha_hasta::date")
        params["fecha_hasta"] = fecha_hasta

    where     = ("WHERE " + " AND ".join(filters)) if filters else ""
    and_where = (" AND "  + " AND ".join(filters)) if filters else ""
    # Para text_rank: combina la condición trigrama con los filtros estructurales
    where_trgm = "WHERE (descripcion % :q OR tipo_recurso % :q)" + (" AND " + " AND ".join(filters) if filters else "")

    # ── Sin query: listado filtrado ───────────────────────────────────────────
    if not q:
        result_sql = f"""
            SELECT id, numero, to_char(fecha, 'DD/MM/YYYY') as fecha, anio, tipo_recurso, ley_impugnada,
                   sentido, descripcion, pdf_url, NULL::float AS score,
                   COUNT(*) OVER () AS total
            FROM resoluciones {where}
            ORDER BY fecha DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """
        params["limit"]  = page_size
        params["offset"] = offset
        rows  = (await db.execute(text(result_sql), params)).mappings().all()
        total = rows[0]["total"] if rows else 0
        return SearchResponse(total=total,
                              resultados=[ResolucionSummary(**{k: v for k, v in dict(r).items() if k != "total"}) for r in rows])

    # ── MODO EXACTO: búsqueda literal full-text ───────────────────────────────
    # Busca la cadena exacta en descripcion, tipo_recurso, texto_pdf y numero.
    # Garantiza que solo aparecen documentos donde el término está presente.
    if modo == "exacto":
        # Construir condición de búsqueda literal
        q_terms = q.strip().split()
        text_filters = list(filters)  # copiar filtros existentes
        for i, term in enumerate(q_terms):
            key = f"term_{i}"
            text_filters.append(
                f"(descripcion ILIKE :{key}"
                f" OR tipo_recurso ILIKE :{key}"
                f" OR texto_pdf ILIKE :{key}"
                f" OR numero ILIKE :{key})"
            )
            params[key] = f"%{term}%"

        where_exacto = "WHERE " + " AND ".join(text_filters) if text_filters else ""

        # Una sola query: COUNT(*) OVER() evita el segundo escaneo de texto_pdf
        result_sql = f"""
            SELECT id, numero, to_char(fecha, 'DD/MM/YYYY') as fecha, anio, tipo_recurso, ley_impugnada,
                   sentido, descripcion, pdf_url,
                   (
                       COALESCE(array_length(
                           regexp_split_to_array(
                               lower(COALESCE(descripcion,'') || ' ' || COALESCE(texto_pdf,'')),
                               lower(:q_full)
                           ), 1
                       ), 1) - 1
                   )::float AS score,
                   COUNT(*) OVER () AS total
            FROM resoluciones
            {where_exacto}
            ORDER BY score DESC, fecha DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """
        params["q_full"] = q.lower()
        params["limit"]  = page_size
        params["offset"] = offset

        rows  = (await db.execute(text(result_sql), params)).mappings().all()
        total = rows[0]["total"] if rows else 0
        return SearchResponse(total=total,
                              resultados=[ResolucionSummary(**{k: v for k, v in dict(r).items() if k != "total"}) for r in rows])

    # ── MODO SEMÁNTICO: RRF vectorial (chunks) + trigrama ────────────────────
    # vector_rank usa chunks para mayor precisión semántica:
    # por cada resolución, se toma el chunk más cercano a la consulta.
    emb = await get_embedding(q)

    # Una sola query: count + resultados en el mismo CTE.
    # vector_rank: el subquery más interno usa ORDER BY <=> LIMIT para activar
    # el índice HNSW. Sin ese patrón, PostgreSQL hace escaneo secuencial de chunks.
    # text_rank: el operador % activa el índice GIN trigrama en vez de escanear
    # toda la tabla con similarity().
    hybrid_sql = f"""
        WITH
        vector_rank AS (
            SELECT id, ROW_NUMBER() OVER (ORDER BY best_dist) AS rank
            FROM (
                SELECT r.id, MIN(c.dist) AS best_dist
                FROM (
                    SELECT resolucion_id, embedding <=> (:emb)::vector AS dist
                    FROM chunks
                    WHERE embedding IS NOT NULL
                    ORDER BY embedding <=> (:emb)::vector
                    LIMIT 600
                ) c
                JOIN resoluciones r ON r.id = c.resolucion_id
                WHERE TRUE {and_where}
                GROUP BY r.id
                ORDER BY best_dist
                LIMIT 200
            ) vr
        ),
        text_rank AS (
            SELECT id,
                   ROW_NUMBER() OVER (ORDER BY sim DESC) AS rank
            FROM (
                SELECT id, similarity(descripcion, :q) + similarity(tipo_recurso, :q) AS sim
                FROM resoluciones
                {where_trgm}
                ORDER BY sim DESC
                LIMIT 200
            ) t
        ),
        rrf AS (
            SELECT COALESCE(v.id, t.id) AS id,
                   (COALESCE(1.0/(60+v.rank), 0) + COALESCE(1.0/(60+t.rank), 0)) AS score
            FROM vector_rank v
            FULL OUTER JOIN text_rank t ON v.id = t.id
        ),
        total_count AS (SELECT COUNT(*) AS cnt FROM rrf)
        SELECT r.id, r.numero, to_char(r.fecha, 'DD/MM/YYYY') as fecha, r.anio, r.tipo_recurso,
               r.ley_impugnada, r.sentido, r.descripcion, r.pdf_url, rrf.score,
               tc.cnt AS total
        FROM rrf
        JOIN resoluciones r ON r.id = rrf.id
        CROSS JOIN total_count tc
        ORDER BY rrf.score DESC
        LIMIT :limit OFFSET :offset
    """

    params["emb"]    = fmt_vector(emb)
    params["q"]      = q
    params["limit"]  = page_size
    params["offset"] = offset

    rows = (await db.execute(text(hybrid_sql), params)).mappings().all()
    total = rows[0]["total"] if rows else 0
    return SearchResponse(total=total,
                          resultados=[ResolucionSummary(**{k: v for k, v in dict(r).items() if k != "total"}) for r in rows])


# ── Endpoint: detalle de una resolución ──────────────────────────────────────

@app.get("/resoluciones/{numero_encoded}", response_model=ResolucionSummary)
async def detalle(numero_encoded: str, db: AsyncSession = Depends(get_db)):
    numero = numero_encoded.replace("_", "/")
    row = (await db.execute(
        text("""
            SELECT id, numero, to_char(fecha, 'DD/MM/YYYY') as fecha, anio, tipo_recurso, ley_impugnada,
                   sentido, descripcion, pdf_url, NULL::float AS score
            FROM resoluciones WHERE numero = :numero
        """),
        {"numero": numero},
    )).mappings().one_or_none()

    if not row:
        raise HTTPException(status_code=404, detail="Resolución no encontrada")
    return ResolucionSummary(**dict(row))


# ── Endpoint: resumen estructurado de resolución ─────────────────────────────

@app.get("/resoluciones/{numero_encoded}/resumen", response_model=ResumenResolucion)
async def resumen_resolucion(numero_encoded: str, db: AsyncSession = Depends(get_db)):
    """
    Extrae mediante LLM los campos estructurados de una resolución.
    Si ya existe en BD lo devuelve directamente (sin llamar a OpenAI).
    """
    numero = numero_encoded.replace("_", "/")
    row = (await db.execute(
        text("""
            SELECT numero, texto_pdf,
                   llm_extraido_at, tipo_contrato_llm, objeto_contrato, importe_resumen,
                   recurrente, organo_contratacion,
                   razones_recurso_json, razones_oposicion_json, razones_tribunal_json, fallo
            FROM resoluciones WHERE numero = :numero
        """),
        {"numero": numero},
    )).mappings().one_or_none()

    if not row:
        raise HTTPException(status_code=404, detail="Resolución no encontrada")
    if not row["texto_pdf"]:
        raise HTTPException(status_code=422,
                            detail="Esta resolución no tiene texto PDF extraído todavía.")

    # ── Devolver desde BD si ya fue extraído ──────────────────────────────────
    if row["llm_extraido_at"]:
        return ResumenResolucion(
            numero=row["numero"],
            tipo_contrato=row["tipo_contrato_llm"],
            objeto=row["objeto_contrato"],
            importe=row["importe_resumen"],
            recurrente=row["recurrente"],
            organo_contratacion=row["organo_contratacion"],
            razones_recurso=row["razones_recurso_json"] or [],
            razones_oposicion=row["razones_oposicion_json"] or [],
            razones_tribunal=row["razones_tribunal_json"] or [],
            fallo=row["fallo"],
        )

    # ── Llamar al LLM ─────────────────────────────────────────────────────────
    system_prompt = (
        "Eres un asistente jurídico especializado en contratación pública española. "
        "Analiza la resolución del TACRC y extrae los campos solicitados. "
        "Devuelve ÚNICAMENTE un objeto JSON válido, sin texto adicional. "
        "Si un campo no aparece en el texto, usa null para cadenas o [] para listas. "
        "Cada razón debe ser una frase conceptual breve, máximo 15 palabras, "
        "que capture la esencia del argumento."
    )

    user_prompt = f"""Extrae estos campos y devuelve SOLO el JSON, sin markdown:

{{
  "numero": "número de resolución tal como aparece (ej. 0340/2026)",
  "tipo_contrato": "tipo (servicios / obras / suministros / concesión / administrativo especial / etc.)",
  "objeto": "objeto del contrato en una frase",
  "importe": "importe con unidad (ej. '1.234.567,00 €') o null si no aparece",
  "recurrente": "nombre completo del recurrente (empresa o persona física)",
  "organo_contratacion": "nombre completo del órgano de contratación",
  "razones_recurso": ["frase conceptual de la razón 1", "frase conceptual de la razón 2"],
  "razones_oposicion": ["frase conceptual de la razón de oposición 1"],
  "razones_tribunal": ["frase conceptual del razonamiento del tribunal 1"],
  "fallo": "Estimación / Desestimación / Inadmisión + breve descripción del pronunciamiento"
}}

TEXTO DE LA RESOLUCIÓN:
{row["texto_pdf"][:12000]}"""

    completion = await openai.chat.completions.create(
        model=settings.llm_model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=1000,
        response_format={"type": "json_object"},
    )

    data = json.loads(completion.choices[0].message.content)

    # ── Persistir en BD ───────────────────────────────────────────────────────
    await db.execute(text("""
        UPDATE resoluciones SET
            tipo_contrato_llm       = :tipo_contrato,
            objeto_contrato         = :objeto,
            importe_resumen         = :importe,
            recurrente              = :recurrente,
            organo_contratacion     = :organo_contratacion,
            razones_recurso_json    = :razones_recurso,
            razones_oposicion_json  = :razones_oposicion,
            razones_tribunal_json   = :razones_tribunal,
            fallo                   = :fallo,
            llm_extraido_at         = NOW()
        WHERE numero = :numero
    """), {
        "numero":              numero,
        "tipo_contrato":       data.get("tipo_contrato"),
        "objeto":              data.get("objeto"),
        "importe":             data.get("importe"),
        "recurrente":          data.get("recurrente"),
        "organo_contratacion": data.get("organo_contratacion"),
        "razones_recurso":     json.dumps(data.get("razones_recurso", []), ensure_ascii=False),
        "razones_oposicion":   json.dumps(data.get("razones_oposicion", []), ensure_ascii=False),
        "razones_tribunal":    json.dumps(data.get("razones_tribunal", []), ensure_ascii=False),
        "fallo":               data.get("fallo"),
    })
    await db.commit()
    log.info("Resumen guardado en BD: %s", numero)

    return ResumenResolucion(**data)


# ── Endpoint: chat RAG ────────────────────────────────────────────────────────

@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, db: AsyncSession = Depends(get_db)):
    """
    Responde preguntas en lenguaje natural recuperando los chunks
    más relevantes de las resoluciones y sintetizando con el LLM.
    """
    emb = await get_embedding(req.pregunta)

    # Filtros opcionales sobre los chunks
    chunk_filters = []
    chunk_params: dict = {"emb": fmt_vector(emb), "top_k": req.top_k}

    if req.anio:
        chunk_filters.append("r.anio = :anio")
        chunk_params["anio"] = req.anio
    if req.sentido:
        chunk_filters.append("r.sentido ILIKE :sentido")
        chunk_params["sentido"] = f"%{req.sentido}%"

    chunk_where = ("AND " + " AND ".join(chunk_filters)) if chunk_filters else ""

    # Recuperar el triple de chunks para que el reranker tenga margen
    fetch_k = req.top_k * 3

    chunks_sql = f"""
        SELECT c.texto, c.id AS chunk_id, r.numero, to_char(r.fecha, 'DD/MM/YYYY') as fecha, r.sentido,
               r.pdf_url, r.id, r.tipo_recurso, r.ley_impugnada, r.descripcion, r.anio,
               c.embedding <=> (:emb)::vector AS distance
        FROM chunks c
        JOIN resoluciones r ON r.id = c.resolucion_id
        WHERE c.embedding IS NOT NULL
        {chunk_where}
        ORDER BY c.embedding <=> (:emb)::vector
        LIMIT :top_k
    """
    chunk_params["top_k"] = fetch_k

    rows = (await db.execute(text(chunks_sql), chunk_params)).mappings().all()

    if not rows:
        return ChatResponse(
            respuesta="No encontré resoluciones relevantes para tu pregunta. "
                      "Prueba con otros términos o amplía los filtros.",
            fuentes=[],
        )

    # ── Reranking con cross-encoder ──────────────────────────────────────────
    if RERANK_ENABLED and len(rows) > req.top_k:
        passages = [{"id": i, "text": r["texto"]} for i, r in enumerate(rows)]
        rerank_req = RerankRequest(query=req.pregunta, passages=passages)
        reranked   = reranker.rerank(rerank_req)
        # Tomar los top_k mejor puntuados y reordenar rows
        top_ids = [r["id"] for r in reranked[:req.top_k]]
        rows    = [rows[i] for i in top_ids]
        log.info(f"Reranking aplicado: {fetch_k} → {len(rows)} chunks")
    else:
        rows = list(rows)[:req.top_k]

    # Construir contexto para el LLM
    context_parts = []
    seen_ids = {}
    fuentes = []

    fragmentos = []
    for i, row in enumerate(rows):
        rid = row["id"]
        context_parts.append(
            f"[{row['numero']} · {row['fecha'] or ''} · {row['sentido'] or ''}]\n{row['texto']}"
        )
        # Guardar fragmento exacto con score de posición (1=mejor)
        fragmentos.append(Fragmento(
            numero  = row["numero"],
            fecha   = row["fecha"],
            sentido = row["sentido"],
            texto   = row["texto"],
            score   = round(1 - i / max(len(rows), 1), 3),
            pdf_url = row["pdf_url"],
        ))
        if rid not in seen_ids:
            seen_ids[rid] = True
            fuentes.append(ResolucionSummary(
                id=rid,
                numero=row["numero"],
                fecha=row["fecha"],
                anio=row["anio"],
                tipo_recurso=row["tipo_recurso"],
                ley_impugnada=row["ley_impugnada"],
                sentido=row["sentido"],
                descripcion=row["descripcion"],
                pdf_url=row["pdf_url"],
            ))

    context = "\n\n---\n\n".join(context_parts)

    system_prompt = """Eres un asistente jurídico especializado en contratación pública española.
Tu tarea es responder preguntas basándote EXCLUSIVAMENTE en los fragmentos de resoluciones del
Tribunal Administrativo Central de Recursos Contractuales (TACRC) que se te proporcionan.

Reglas:
- Cita siempre el número de resolución cuando afirmes algo.
- Si los fragmentos no contienen información suficiente, dilo explícitamente.
- No inventes jurisprudencia ni doctrina no presente en los fragmentos.
- Responde en español, con lenguaje claro y preciso.
- Estructura tu respuesta con párrafos breves. Si hay varias resoluciones relevantes, agrúpalas por criterio."""

    user_prompt = f"""PREGUNTA: {req.pregunta}

FRAGMENTOS DE RESOLUCIONES:
{context}

Por favor, responde a la pregunta basándote en los fragmentos anteriores."""

    completion = await openai.chat.completions.create(
        model=settings.llm_model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=1500,
    )

    respuesta_texto = completion.choices[0].message.content

    # ── Persistir en BD y generar sugerencias en paralelo ────────────────────

    async def _persistir() -> tuple[int, int]:
        _conv_id = req.conversacion_id
        _turno   = 0
        try:
            async with db.begin():
                if not _conv_id:
                    titulo = req.pregunta[:60] + ("…" if len(req.pregunta) > 60 else "")
                    result = await db.execute(
                        text("INSERT INTO conversaciones (titulo) VALUES (:t) RETURNING id"),
                        {"t": titulo}
                    )
                    _conv_id = result.scalar()

                turno_result = await db.execute(
                    text("SELECT COALESCE(MAX(turno), -1) + 1 FROM mensajes WHERE conversacion_id = :cid"),
                    {"cid": _conv_id}
                )
                _turno = turno_result.scalar() or 0

                await db.execute(text("""
                    INSERT INTO mensajes (conversacion_id, turno, role, contenido,
                                          anio_filtro, sentido_filtro, top_k)
                    VALUES (:cid, :t, 'user', :c, :anio, :sentido, :top_k)
                """), {
                    "cid": _conv_id, "t": _turno, "c": req.pregunta,
                    "anio": req.anio, "sentido": req.sentido, "top_k": req.top_k
                })

                await db.execute(text("""
                    INSERT INTO mensajes (conversacion_id, turno, role, contenido,
                                          fuentes_json, fragmentos_json)
                    VALUES (:cid, :t, 'assistant', :c, :fuentes, :frags)
                """), {
                    "cid": _conv_id, "t": _turno + 1,
                    "c": respuesta_texto,
                    "fuentes": json.dumps([f.dict() for f in fuentes]),
                    "frags":   json.dumps([f.dict() for f in fragmentos]),
                })
        except Exception as e:
            log.warning(f"No se pudo guardar conversación: {e}")
        return _conv_id, _turno

    async def _sugerencias() -> list[str]:
        try:
            ctx = (
                "Pregunta: " + req.pregunta + "\n\n" +
                "Respuesta: " + respuesta_texto[:500] + "\n\n" +
                "Resoluciones: " + ", ".join(f.numero for f in fuentes[:5])
            )
            sug = await openai.chat.completions.create(
                model=settings.llm_model,
                messages=[
                    {"role": "system", "content": (
                        "Eres experto en contratación pública española. "
                        "Genera 3 preguntas de seguimiento concretas tras esta respuesta sobre TACRC. "
                        "Responde SOLO con las 3 preguntas, una por línea, sin numeración."
                    )},
                    {"role": "user", "content": ctx},
                ],
                temperature=0.4,
                max_tokens=200,
            )
            raw = sug.choices[0].message.content.strip()
            return [
                s.strip().lstrip("*-123456789). ")
                for s in raw.splitlines()
                if s.strip() and len(s.strip()) > 10
            ][:3]
        except Exception as e:
            log.debug("Sugerencias no generadas: %s", e)
            return []

    (conv_id, turno), sugerencias = await asyncio.gather(_persistir(), _sugerencias())

    return ChatResponse(
        respuesta=respuesta_texto,
        fuentes=fuentes,
        fragmentos=fragmentos,
        conversacion_id=conv_id,
        turno=turno,
        sugerencias=sugerencias,
    )


# ── Endpoint: análisis de riesgo de pliego ────────────────────────────────────

class RiesgoAspecto(BaseModel):
    aspecto:          str
    nivel:            str            # ALTO / MEDIO / BAJO / SIN_DATOS
    probabilidad:     int            # 0-100 basado en ratio de estimaciones
    razonamiento:     str
    resoluciones:     List[str]      # números de resolución que sustentan
    estimaciones:     int            # cuántas estimaciones hay sobre este aspecto
    total_docs:       int            # total de resoluciones recuperadas
    contexto_doctrina: str = ""      # chunks relevantes para follow-up sin nueva búsqueda

class SeguimientoRequest(BaseModel):
    aspecto:           str
    nivel:             str
    razonamiento:      str
    contexto_doctrina: str
    historial:         List[dict] = []   # [{role, content}, ...]
    pregunta:          str
    modo:              str = "chat"      # "chat" | "redaccion" | "ejemplos"

class SeguimientoResponse(BaseModel):
    respuesta:   str
    historial:   List[dict]

class RiesgoResponse(BaseModel):
    nombre_pliego: str
    riesgo_global: str            # ALTO / MEDIO / BAJO
    score_global:  int            # 0-100
    aspectos:      list[RiesgoAspecto]
    fuentes:       list[ResolucionSummary]
    resumen:       str

@app.post("/riesgo_pliego", response_model=RiesgoResponse)
async def riesgo_pliego(
    pdf:      UploadFile = File(...),
    memoria:  Optional[UploadFile] = File(None),
    aspectos: str        = Form(...),
    top_k:    int        = Form(6),
    db:       AsyncSession = Depends(get_db),
):
    """
    Analiza el riesgo de recurso de cada cláusula del pliego.
    Para cada aspecto:
    1. Recupera las resoluciones más similares del TACRC
    2. Calcula el ratio de estimaciones (anulaciones) vs total
    3. El LLM razona si la cláusula concreta del pliego tiene riesgo
    """
    import io, json as json_mod

    # ── Extraer texto del pliego ──────────────────────────────────────────────
    contenido = await pdf.read()
    texto_pliego = ""
    try:
        with pdfplumber.open(io.BytesIO(contenido)) as doc:
            texto_pliego = "\n\n".join(p.extract_text() or "" for p in doc.pages)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer el PDF: {e}")

    if len(texto_pliego.strip()) < 100:
        raise HTTPException(status_code=400, detail="El PDF no contiene texto extraíble.")

    # Extraer memoria si se proporcionó
    texto_memoria = ""
    if memoria:
        try:
            mem_bytes = await memoria.read()
            with pdfplumber.open(io.BytesIO(mem_bytes)) as doc:
                texto_memoria = "\n\n".join(p.extract_text() or "" for p in doc.pages)
        except Exception:
            pass

    lista_aspectos = [a.strip("- ").strip() for a in aspectos.strip().splitlines() if a.strip()]
    fuentes_dict: dict[int, ResolucionSummary] = {}
    resultados: list[RiesgoAspecto] = []

    for aspecto in lista_aspectos:
        # Recuperar resoluciones similares para este aspecto
        emb = await get_embedding(f"{aspecto} pliego contratación pública LCSP recurso")

        rows = (await db.execute(text("""
            SELECT r.id, r.numero, to_char(r.fecha, 'DD/MM/YYYY') as fecha, r.sentido, r.pdf_url,
                   r.tipo_recurso, r.ley_impugnada, r.descripcion, r.anio,
                   c.texto,
                   c.embedding <=> (:emb)::vector AS distance
            FROM chunks c
            JOIN resoluciones r ON r.id = c.resolucion_id
            WHERE c.embedding IS NOT NULL
            ORDER BY c.embedding <=> (:emb)::vector
            LIMIT :top_k
        """), {"emb": fmt_vector(emb), "top_k": top_k})).mappings().all()

        if not rows:
            resultados.append(RiesgoAspecto(
                aspecto=aspecto, nivel="SIN_DATOS", probabilidad=0,
                razonamiento="No se encontraron resoluciones relevantes en la base de datos.",
                resoluciones=[], estimaciones=0, total_docs=0,
            ))
            continue

        # Calcular ratio de estimaciones (proxy de riesgo estadístico)
        total     = len(rows)
        estimadas = sum(
            1 for r in rows
            if r["sentido"] and "estimaci" in r["sentido"].lower()
        )
        ratio_estimaciones = estimadas / total if total else 0

        # Contexto para el LLM
        contexto_chunks = "\n---\n".join(
            f"[{r['numero']} · {r['sentido'] or ''}]\n{r['texto']}" for r in rows
        )
        numeros = list({r["numero"] for r in rows})

        # Extraer fragmento relevante del pliego + memoria para este aspecto
        palabras = aspecto.lower().split()
        lineas_pliego = [
            l for l in texto_pliego.splitlines()
            if any(p in l.lower() for p in palabras if len(p) > 4)
        ]
        extracto_pliego = "\n".join(lineas_pliego[:15]) if lineas_pliego else texto_pliego[:600]

        # Añadir fragmento relevante de la memoria si existe
        if texto_memoria:
            lineas_mem = [
                l for l in texto_memoria.splitlines()
                if any(p in l.lower() for p in palabras if len(p) > 4)
            ]
            if lineas_mem:
                extracto_pliego += "\n\n[MEMORIA DEL CONTRATO - justificación]:\n" + "\n".join(lineas_mem[:10])

        # El LLM evalúa el riesgo concreto de este aspecto
        prompt_riesgo = f"""Analiza el riesgo de recurso de la siguiente cláusula del pliego ante el TACRC.

ASPECTO: {aspecto}

EXTRACTO DEL PLIEGO (cláusulas relevantes):
{extracto_pliego}

DOCTRINA TACRC (resoluciones similares):
{contexto_chunks}

ESTADÍSTICA: De {total} resoluciones similares recuperadas, {estimadas} fueron estimadas (el TACRC anuló o corrigió).

Responde EXCLUSIVAMENTE con este JSON (sin markdown, sin explicación adicional):
{{
  "nivel": "ALTO|MEDIO|BAJO|SIN_DATOS",
  "probabilidad": <número 0-100>,
  "razonamiento": "<2-3 frases explicando el riesgo concreto de ESTA cláusula citando resoluciones>",
  "resoluciones_clave": ["<numero1>", "<numero2>"]
}}

Criterios:
- ALTO (70-100): La cláusula infringe doctrina consolidada del TACRC, alta probabilidad de estimación
- MEDIO (30-69): Doctrina no uniforme o la cláusula es borderline según las resoluciones
- BAJO (0-29): La cláusula es conforme con la doctrina mayoritaria del TACRC
- SIN_DATOS: No hay suficientes resoluciones para evaluar"""

        try:
            completion = await openai.chat.completions.create(
                model=settings.llm_model,
                messages=[{"role": "user", "content": prompt_riesgo}],
                temperature=0.0,
                max_tokens=400,
            )
            raw = completion.choices[0].message.content.strip()
            # Limpiar posibles marcas de código
            raw = raw.replace("```json", "").replace("```", "").strip()
            parsed = json_mod.loads(raw)

            nivel         = parsed.get("nivel", "SIN_DATOS")
            probabilidad  = int(parsed.get("probabilidad", 0))
            razonamiento  = parsed.get("razonamiento", "")
            res_clave     = parsed.get("resoluciones_clave", numeros[:3])
        except Exception as e:
            log.warning(f"LLM riesgo falló para '{aspecto}': {e}")
            nivel        = "MEDIO"
            probabilidad = int(ratio_estimaciones * 100)
            razonamiento = f"Ratio estadístico: {estimadas}/{total} resoluciones similares fueron estimadas."
            res_clave    = numeros[:3]

        resultados.append(RiesgoAspecto(
            aspecto=aspecto, nivel=nivel, probabilidad=probabilidad,
            razonamiento=razonamiento, resoluciones=res_clave,
            estimaciones=estimadas, total_docs=total,
            contexto_doctrina=contexto_chunks[:4000],
        ))

        # Acumular fuentes
        for r in rows:
            if r["id"] not in fuentes_dict:
                fuentes_dict[r["id"]] = ResolucionSummary(
                    id=r["id"], numero=r["numero"], fecha=r["fecha"],
                    anio=r["anio"], tipo_recurso=r["tipo_recurso"],
                    ley_impugnada=r["ley_impugnada"], sentido=r["sentido"],
                    descripcion=r["descripcion"], pdf_url=r["pdf_url"],
                )

    # Score global = media ponderada (los ALTO cuentan doble)
    if resultados:
        scores = []
        for r in resultados:
            if r.nivel == "SIN_DATOS":
                continue
            peso = 2 if r.nivel == "ALTO" else 1
            scores.extend([r.probabilidad] * peso)
        score_global = int(sum(scores) / len(scores)) if scores else 0
    else:
        score_global = 0

    if score_global >= 60:
        riesgo_global = "ALTO"
    elif score_global >= 30:
        riesgo_global = "MEDIO"
    else:
        riesgo_global = "BAJO"

    # Resumen ejecutivo
    n_alto  = sum(1 for r in resultados if r.nivel == "ALTO")
    n_medio = sum(1 for r in resultados if r.nivel == "MEDIO")
    n_bajo  = sum(1 for r in resultados if r.nivel == "BAJO")

    resumen_prompt = f"""El análisis del pliego "{pdf.filename}" muestra:
- {n_alto} aspectos de RIESGO ALTO
- {n_medio} aspectos de RIESGO MEDIO
- {n_bajo} aspectos de RIESGO BAJO
- Score global de riesgo: {score_global}/100

Aspectos de mayor riesgo:
{chr(10).join(f"- {r.aspecto}: {r.razonamiento}" for r in resultados if r.nivel == "ALTO")}

Redacta un párrafo ejecutivo de 3-4 frases para un técnico de contratación,
indicando qué aspectos son más urgentes de revisar y por qué."""

    try:
        resumen_completion = await openai.chat.completions.create(
            model=settings.llm_model,
            messages=[{"role": "user", "content": resumen_prompt}],
            temperature=0.1,
            max_tokens=300,
        )
        resumen = resumen_completion.choices[0].message.content
    except Exception:
        resumen = f"El pliego presenta {n_alto} aspectos de riesgo alto que requieren revisión urgente."

    return RiesgoResponse(
        nombre_pliego=pdf.filename or "pliego.pdf",
        riesgo_global=riesgo_global,
        score_global=score_global,
        aspectos=resultados,
        fuentes=list(fuentes_dict.values()),
        resumen=resumen,
    )


# ── Endpoint: seguimiento por aspecto ────────────────────────────────────────

@app.post("/seguimiento_aspecto", response_model=SeguimientoResponse)
async def seguimiento_aspecto(req: SeguimientoRequest):
    """
    Chat de seguimiento por aspecto individual.
    Modos:
    - chat:      pregunta libre sobre el aspecto
    - redaccion: genera redacción alternativa conforme al TACRC
    - ejemplos:  muestra cláusulas similares que fueron estimadas
    """

    system_base = """Eres un abogado experto en contratación pública española especializado en el TACRC.
Tienes delante el análisis de riesgo de una cláusula concreta de un pliego de condiciones.
Usa EXCLUSIVAMENTE la doctrina TACRC proporcionada para responder. Cita siempre el número de resolución."""

    if req.modo == "redaccion":
        task = f"""El aspecto analizado es: {req.aspecto}
Nivel de riesgo detectado: {req.nivel}
Problema identificado: {req.razonamiento}

DOCTRINA TACRC RELEVANTE:
{req.contexto_doctrina}

Tu tarea: redacta una cláusula alternativa que subsane el problema identificado y sea conforme
a la doctrina del TACRC. La redacción debe:
1. Estar en lenguaje jurídico-administrativo formal
2. Citar el artículo de la LCSP aplicable
3. Incorporar los criterios que el TACRC ha considerado válidos en las resoluciones
4. Ser directamente insertable en el PCAP

Tras la redacción, explica en 2-3 frases por qué esta redacción supera el análisis de riesgo."""

    elif req.modo == "ejemplos":
        task = f"""El aspecto analizado es: {req.aspecto}
Nivel de riesgo detectado: {req.nivel}

DOCTRINA TACRC RELEVANTE:
{req.contexto_doctrina}

Tu tarea: extrae de las resoluciones proporcionadas ejemplos concretos de cláusulas o situaciones
similares que el TACRC consideró CONFORMES (recursos desestimados o inadmitidos).
Para cada ejemplo indica: número de resolución, qué permitía la cláusula y por qué el TACRC la avaló."""

    else:
        # Modo chat libre
        task = f"""Contexto del aspecto analizado:
- Aspecto: {req.aspecto}
- Nivel de riesgo: {req.nivel}
- Análisis previo: {req.razonamiento}

DOCTRINA TACRC RELEVANTE:
{req.contexto_doctrina}

Pregunta del usuario: {req.pregunta}"""

    # Construir historial completo
    messages = [{"role": "system", "content": system_base}]

    # Añadir historial previo
    for msg in req.historial:
        messages.append({"role": msg["role"], "content": msg["content"]})

    # Añadir mensaje actual
    if req.modo == "chat":
        messages.append({"role": "user", "content": task})
    else:
        messages.append({"role": "user", "content": task})

    completion = await openai.chat.completions.create(
        model=settings.llm_model,
        messages=messages,
        temperature=0.1,
        max_tokens=1500,
    )

    respuesta = completion.choices[0].message.content

    # Actualizar historial para el frontend
    nuevo_historial = list(req.historial)
    if req.modo == "chat":
        nuevo_historial.append({"role": "user",      "content": req.pregunta})
    else:
        nuevo_historial.append({"role": "user",      "content": f"[{req.modo.upper()}] {req.pregunta or req.modo}"})
    nuevo_historial.append(    {"role": "assistant", "content": respuesta})

    return SeguimientoResponse(respuesta=respuesta, historial=nuevo_historial)


# ── Endpoint: analizar pliego (análisis narrativo) ────────────────────────────

@app.post("/analizar_pliego")
async def analizar_pliego(
    pdf:      UploadFile = File(...),
    memoria:  Optional[UploadFile] = File(None),
    aspectos: str        = Form(...),
    top_k:    int        = Form(6),
    db:       AsyncSession = Depends(get_db),
):
    """
    Análisis narrativo del pliego contra doctrina TACRC.
    Acepta opcionalmente la memoria del contrato para contextualizar.
    """
    # Extraer texto del pliego
    contenido = await pdf.read()
    texto_pliego = ""
    try:
        with pdfplumber.open(io.BytesIO(contenido)) as doc:
            texto_pliego = "\n\n".join(p.extract_text() or "" for p in doc.pages)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer el PDF: {e}")

    # Extraer texto de la memoria si se proporcionó
    texto_memoria = ""
    if memoria:
        try:
            mem_bytes = await memoria.read()
            with pdfplumber.open(io.BytesIO(mem_bytes)) as doc:
                texto_memoria = "\n\n".join(p.extract_text() or "" for p in doc.pages)
        except Exception:
            pass

    lista_aspectos = [a.strip("- ").strip() for a in aspectos.strip().splitlines() if a.strip()]
    fuentes_dict: dict = {}
    contextos = []

    for aspecto in lista_aspectos:
        emb = await get_embedding(f"{aspecto} pliego contratación pública LCSP")
        rows = (await db.execute(text("""
            SELECT r.id, r.numero, r.fecha::text, r.sentido, r.pdf_url,
                   r.tipo_recurso, r.ley_impugnada, r.descripcion, r.anio, c.texto,
                   c.embedding <=> (:emb)::vector AS distance
            FROM chunks c JOIN resoluciones r ON r.id = c.resolucion_id
            WHERE c.embedding IS NOT NULL
            ORDER BY c.embedding <=> (:emb)::vector LIMIT :top_k
        """), {"emb": fmt_vector(emb), "top_k": top_k})).mappings().all()

        if rows:
            frags = "\n---\n".join(f"[{r['numero']} · {r['sentido']}]\n{r['texto']}" for r in rows)
            contextos.append(f"### {aspecto}\n{frags}")
            for r in rows:
                if r["id"] not in fuentes_dict:
                    fuentes_dict[r["id"]] = ResolucionSummary(
                        id=r["id"], numero=r["numero"], fecha=r["fecha"],
                        anio=r["anio"], tipo_recurso=r["tipo_recurso"],
                        ley_impugnada=r["ley_impugnada"], sentido=r["sentido"],
                        descripcion=r["descripcion"], pdf_url=r["pdf_url"],
                    )

    memoria_section = ""
    if texto_memoria:
        memoria_section = f"""\nMEMORIA DEL CONTRATO (justificación de las cláusulas):
{texto_memoria[:3000]}\n"""

    system = """Eres un abogado experto en contratación pública española especializado en el TACRC.
Analiza si el pliego es conforme a la doctrina del Tribunal.
Para cada aspecto: cita el texto del pliego, indica CONFORME / DUDOSO / NO CONFORME y razona citando resoluciones."""

    user = f"""TEXTO DEL PLIEGO:
{texto_pliego[:5000]}
{memoria_section}
ASPECTOS A ANALIZAR:
{chr(10).join(f'- {a}' for a in lista_aspectos)}

DOCTRINA TACRC:
{chr(10).join(contextos)[:7000]}

Analiza cada aspecto."""

    completion = await openai.chat.completions.create(
        model=settings.llm_model,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=0.1, max_tokens=3000,
    )

    return {
        "analisis": completion.choices[0].message.content,
        "fuentes":  list(fuentes_dict.values()),
    }


# ── Endpoints: historial de conversaciones ────────────────────────────────────

@app.get("/conversaciones", response_model=list[ConversacionSummary])
async def listar_conversaciones(
    limit:  int = Query(50, ge=1, le=200),
    db:     AsyncSession = Depends(get_db),
):
    """Lista las conversaciones más recientes."""
    rows = (await db.execute(text("""
        SELECT id, titulo,
               creado_at::text,
               actualizado_at::text,
               total_turnos
        FROM conversaciones
        ORDER BY actualizado_at DESC
        LIMIT :limit
    """), {"limit": limit})).mappings().all()
    return [ConversacionSummary(**dict(r)) for r in rows]


@app.get("/conversaciones/{conv_id}", response_model=list[MensajeHistorial])
async def cargar_conversacion(
    conv_id: int,
    db:      AsyncSession = Depends(get_db),
):
    """Carga todos los mensajes de una conversación."""
    rows = (await db.execute(text("""
        SELECT turno, role, contenido,
               fuentes_json, fragmentos_json
        FROM mensajes
        WHERE conversacion_id = :cid
        ORDER BY turno ASC
    """), {"cid": conv_id})).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="Conversación no encontrada")

    result = []
    for r in rows:
        fuentes    = []
        fragmentos = []
        if r["fuentes_json"]:
            try:
                raw = r["fuentes_json"] if isinstance(r["fuentes_json"], list) else json.loads(r["fuentes_json"])
                fuentes = [ResolucionSummary(**f) for f in raw]
            except Exception:
                pass
        if r["fragmentos_json"]:
            try:
                raw = r["fragmentos_json"] if isinstance(r["fragmentos_json"], list) else json.loads(r["fragmentos_json"])
                fragmentos = [Fragmento(**f) for f in raw]
            except Exception:
                pass
        result.append(MensajeHistorial(
            turno=r["turno"], role=r["role"], contenido=r["contenido"],
            fuentes=fuentes, fragmentos=fragmentos,
        ))
    return result


@app.delete("/conversaciones/{conv_id}")
async def borrar_conversacion(
    conv_id: int,
    db:      AsyncSession = Depends(get_db),
):
    """Elimina una conversación y todos sus mensajes."""
    async with db.begin():
        result = await db.execute(
            text("DELETE FROM conversaciones WHERE id = :cid RETURNING id"),
            {"cid": conv_id}
        )
        if not result.scalar():
            raise HTTPException(status_code=404, detail="Conversación no encontrada")
    return {"deleted": conv_id}


@app.patch("/conversaciones/{conv_id}/titulo")
async def renombrar_conversacion(
    conv_id: int,
    titulo:  str,
    db:      AsyncSession = Depends(get_db),
):
    """Renombra una conversación."""
    async with db.begin():
        await db.execute(
            text("UPDATE conversaciones SET titulo = :t WHERE id = :cid"),
            {"t": titulo[:100], "cid": conv_id}
        )
    return {"id": conv_id, "titulo": titulo}
