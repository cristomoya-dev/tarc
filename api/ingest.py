"""
ingest.py — Pipeline de ingesta de resoluciones TACRC
======================================================
1. Lee el JSON exportado por la extensión Chrome
2. Inserta/actualiza metadatos en PostgreSQL
3. Extrae texto de cada PDF en paralelo (pdfplumber → OCR fallback)
4. Divide el texto en chunks de ~500 tokens con cabecera contextual
5. Genera embeddings con OpenAI y los almacena en pgvector

Uso:
    docker compose run --rm ingest /data/json/resoluciones.json
    docker compose run --rm ingest /data/json/resoluciones.json --only-meta
    docker compose run --rm ingest /data/json/resoluciones.json --only-pdf
    docker compose run --rm ingest /data/json/resoluciones.json --only-embed
"""

import asyncio
import json
import logging
import re
import sys
import argparse
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

import asyncpg
import pdfplumber
import tiktoken
from openai import AsyncOpenAI
from tqdm import tqdm

from config import settings

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

openai = AsyncOpenAI(api_key=settings.openai_api_key)
enc    = tiktoken.get_encoding("cl100k_base")

CHUNK_TOKENS  = 500
CHUNK_OVERLAP = 50
EMBED_BATCH   = 20
EMBED_WORKERS = 4
PDF_WORKERS   = 8    # PDFs en paralelo (I/O bound → más workers que CPUs)


# ── Estadísticas de progreso ──────────────────────────────────────────────────

class Stats:
    def __init__(self):
        self.total = 0
        self.ok    = 0
        self.skip  = 0
        self.error = 0
        self.start = time.monotonic()

    def rate(self):
        elapsed = time.monotonic() - self.start
        return self.ok / elapsed if elapsed > 0 else 0

    def eta(self, remaining):
        r = self.rate()
        return remaining / r if r > 0 else 0

    def summary(self):
        elapsed = time.monotonic() - self.start
        return (f"OK={self.ok} | Saltados={self.skip} | "
                f"Errores={self.error} | {elapsed:.0f}s | {self.rate():.1f} pdf/s")


# ── PDF → texto ───────────────────────────────────────────────────────────────

def pdf_path_for(numero: str, pdf_url: str) -> Path | None:
    year_match = re.search(r"\b(20\d{2})\b", pdf_url)
    year = year_match.group(1) if year_match else "sin_año"
    name = re.sub(r"[/\\:*?\"<>|]", "_", numero.strip()) + ".pdf"
    path = Path(settings.pdf_dir) / year / name
    return path if path.exists() else None


def clean(text: str) -> str:
    """Elimina bytes nulos y caracteres problemáticos para PostgreSQL."""
    return text.replace("\x00", "").replace("\u0000", "")


def extract_text_pdfplumber(path: Path) -> tuple[str, int]:
    try:
        with pdfplumber.open(path) as pdf:
            pages = []
            for page in pdf.pages:
                t = page.extract_text(x_tolerance=2, y_tolerance=2)
                if t:
                    pages.append(t)
            return "\n\n".join(pages), len(pdf.pages)
    except Exception as e:
        log.warning(f"pdfplumber falló en {path}: {e}")
        return "", 0


def extract_text_ocr(path: Path) -> str:
    try:
        import pytesseract
        texts = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                img = page.to_image(resolution=200).original
                t = pytesseract.image_to_string(img, lang="spa")
                if t.strip():
                    texts.append(t)
        return "\n\n".join(texts)
    except Exception as e:
        log.warning(f"OCR falló en {path}: {e}")
        return ""


def extract_text(path: Path) -> tuple[str, int]:
    texto, paginas = extract_text_pdfplumber(path)
    if len(texto.strip()) < 100:
        log.info(f"Texto escaso en {path.name}, intentando OCR…")
        texto = extract_text_ocr(path)
    return clean(texto), paginas


def process_pdf_sync(args: tuple) -> dict:
    """
    Función síncrona ejecutada en ProcessPoolExecutor.
    Devuelve un dict con el resultado para guardarlo en BD.
    """
    row_id, numero, pdf_url = args
    path = pdf_path_for(numero, pdf_url or "")
    if not path:
        return {"id": row_id, "status": "skip", "reason": "no_file"}
    try:
        texto, paginas = extract_text(path)
        if not texto.strip():
            return {"id": row_id, "status": "skip", "reason": "no_text"}
        return {"id": row_id, "status": "ok", "texto": texto, "paginas": paginas}
    except Exception as e:
        return {"id": row_id, "status": "error", "reason": str(e)}


# ── Chunking contextual ───────────────────────────────────────────────────────

def chunk_text(texto: str, numero: str = "", sentido: str = "",
               chunk_tokens: int = CHUNK_TOKENS,
               overlap: int = CHUNK_OVERLAP) -> list[str]:
    """
    Divide el texto en chunks con cabecera contextual.
    La cabecera garantiza que el LLM siempre sabe de qué resolución viene el fragmento.
    """
    header = ""
    if numero:
        parts = [f"Resolución TACRC {numero}"]
        if sentido:
            parts.append(f"Sentido: {sentido}")
        header = " | ".join(parts) + "\n\n"

    tokens_header = enc.encode(header)
    tokens_texto  = enc.encode(texto)

    effective_size = chunk_tokens - len(tokens_header)
    chunks = []
    start  = 0

    while start < len(tokens_texto):
        end   = min(start + effective_size, len(tokens_texto))
        chunk = header + enc.decode(tokens_texto[start:end])
        chunks.append(clean(chunk))
        if end == len(tokens_texto):
            break
        start += effective_size - overlap

    return chunks


# ── Embeddings ────────────────────────────────────────────────────────────────

async def embed_texts(texts: list[str]) -> list[list[float]]:
    resp = await openai.embeddings.create(
        model=settings.embed_model,
        input=[t[:8000] for t in texts],
    )
    return [d.embedding for d in resp.data]


def fmt_vector(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


# ── Base de datos ─────────────────────────────────────────────────────────────

async def get_pool() -> asyncpg.Pool:
    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    return await asyncpg.create_pool(dsn, min_size=2, max_size=PDF_WORKERS + 4)


def parse_fecha(fecha_str: str):
    from datetime import date
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", fecha_str or "")
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


# ── Paso 1: metadatos ─────────────────────────────────────────────────────────

async def upsert_metadata(pool: asyncpg.Pool, items: list[dict]) -> int:
    sql = """
        INSERT INTO resoluciones
            (numero, fecha, tipo_recurso, ley_impugnada, sentido, descripcion, pdf_size, pdf_url)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (numero) DO UPDATE SET
            fecha         = EXCLUDED.fecha,
            tipo_recurso  = EXCLUDED.tipo_recurso,
            ley_impugnada = EXCLUDED.ley_impugnada,
            sentido       = EXCLUDED.sentido,
            descripcion   = EXCLUDED.descripcion,
            pdf_size      = EXCLUDED.pdf_size,
            pdf_url       = EXCLUDED.pdf_url,
            actualizado_at = NOW()
    """
    count = 0
    async with pool.acquire() as conn:
        for item in tqdm(items, desc="Insertando metadatos", unit="res"):
            await conn.execute(sql,
                item.get("numero", ""),
                parse_fecha(item.get("fechaResolucion", "")),
                item.get("tipoRecurso"),
                item.get("leyImpugnada"),
                item.get("sentido"),
                item.get("descripcion"),
                item.get("pdfSize"),
                item.get("pdfUrl"),
            )
            count += 1
    return count


# ── Paso 2: extracción PDF en paralelo ────────────────────────────────────────

async def ingest_pdfs(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        pending = await conn.fetch("""
            SELECT id, numero, pdf_url, sentido
            FROM resoluciones
            WHERE pdf_url IS NOT NULL
              AND (pdf_ingestado_at IS NULL OR texto_pdf IS NULL)
            ORDER BY fecha DESC NULLS LAST
        """)

    total = len(pending)
    log.info(f"PDFs pendientes de extracción: {total}")
    if not total:
        return

    stats   = Stats()
    stats.total = total
    semaphore = asyncio.Semaphore(PDF_WORKERS)

    loop = asyncio.get_event_loop()

    async def save_result(result: dict, sentido: str):
        if result["status"] != "ok":
            return
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE resoluciones
                SET texto_pdf = $1, pdf_paginas = $2, pdf_ingestado_at = NOW()
                WHERE id = $3
            """, result["texto"], result["paginas"], result["id"])

            chunks = chunk_text(result["texto"],
                                numero="",   # numero no disponible aquí, se añade en embed
                                sentido=sentido)
            await conn.execute(
                "DELETE FROM chunks WHERE resolucion_id = $1", result["id"])
            await conn.executemany(
                "INSERT INTO chunks (resolucion_id, chunk_index, texto) VALUES ($1, $2, $3)",
                [(result["id"], i, c) for i, c in enumerate(chunks)],
            )

    async def process_one(row):
        async with semaphore:
            result = await loop.run_in_executor(
                None,
                process_pdf_sync,
                (row["id"], row["numero"], row["pdf_url"]),
            )
            if result["status"] == "ok":
                await save_result(result, row.get("sentido") or "")
                stats.ok += 1
            elif result["status"] == "skip":
                stats.skip += 1
            else:
                stats.error += 1
                log.warning(f"Error en {row['numero']}: {result.get('reason')}")

    with tqdm(total=total, desc="Extrayendo texto PDFs", unit="pdf",
              dynamic_ncols=True) as bar:

        tasks = [asyncio.create_task(process_one(row)) for row in pending]

        for task in asyncio.as_completed(tasks):
            await task
            bar.update(1)
            bar.set_postfix_str(
                f"OK={stats.ok} skip={stats.skip} err={stats.error} "
                f"{stats.rate():.1f}pdf/s"
            )

    log.info(f"Extracción finalizada: {stats.summary()}")


# ── Paso 3: embeddings ────────────────────────────────────────────────────────

async def embed_doc_text(row: dict) -> str:
    parts = []
    if row.get("numero"):        parts.append(f"Resolución TACRC {row['numero']}")
    if row.get("tipo_recurso"):  parts.append(row["tipo_recurso"])
    if row.get("ley_impugnada"): parts.append(row["ley_impugnada"])
    if row.get("sentido"):       parts.append(row["sentido"])
    if row.get("descripcion"):   parts.append(row["descripcion"])
    if row.get("texto_pdf"):     parts.append(row["texto_pdf"][:2000])
    return " | ".join(parts)


async def ingest_embeddings(pool: asyncpg.Pool):
    semaphore = asyncio.Semaphore(EMBED_WORKERS)

    # Embeddings de documentos
    async with pool.acquire() as conn:
        pending_docs = await conn.fetch("""
            SELECT id, numero, tipo_recurso, ley_impugnada, sentido, descripcion, texto_pdf
            FROM resoluciones WHERE embedding IS NULL
        """)

    log.info(f"Documentos pendientes de embedding: {len(pending_docs)}")

    async def embed_batch_docs(rows_batch):
        async with semaphore:
            texts   = [await embed_doc_text(dict(r)) for r in rows_batch]
            vectors = await embed_texts(texts)
            async with pool.acquire() as conn:
                for row, vec in zip(rows_batch, vectors):
                    await conn.execute(
                        "UPDATE resoluciones SET embedding = $1, embedding_at = NOW() WHERE id = $2",
                        fmt_vector(vec), row["id"],
                    )

    batches = [pending_docs[i:i+EMBED_BATCH]
               for i in range(0, len(pending_docs), EMBED_BATCH)]
    for batch in tqdm(batches, desc="Embeddings documentos", unit="lote"):
        await embed_batch_docs(batch)

    # Embeddings de chunks
    async with pool.acquire() as conn:
        pending_chunks = await conn.fetch(
            "SELECT id, texto FROM chunks WHERE embedding IS NULL"
        )

    log.info(f"Chunks pendientes de embedding: {len(pending_chunks)}")

    async def embed_batch_chunks(rows_batch):
        async with semaphore:
            texts   = [r["texto"] for r in rows_batch]
            vectors = await embed_texts(texts)
            async with pool.acquire() as conn:
                for row, vec in zip(rows_batch, vectors):
                    await conn.execute(
                        "UPDATE chunks SET embedding = $1 WHERE id = $2",
                        fmt_vector(vec), row["id"],
                    )

    batches = [pending_chunks[i:i+EMBED_BATCH]
               for i in range(0, len(pending_chunks), EMBED_BATCH)]
    for batch in tqdm(batches, desc="Embeddings chunks", unit="lote"):
        await embed_batch_chunks(batch)


# ── Paso 4: extracción LLM de campos estructurados ───────────────────────────
#
# Usa la Batch API de OpenAI (50% más barata que la API síncrona).
# Flujo:
#   1. Prepara un fichero JSONL con una petición por resolución
#   2. Sube el fichero y lanza el batch
#   3. Sondea hasta que el batch termina (puede tardar minutos u horas)
#   4. Descarga los resultados y guarda en BD
#
# Campos extraídos:
#   - organo_contratacion : texto libre, nombre del órgano/entidad licitadora
#   - importe_contrato    : número decimal en euros (sin IVA si se puede distinguir)
#   - tipo_contrato_llm   : uno de Servicios | Obras | Suministros | Concesión | Mixto | Desconocido

import json as _json
import tempfile
import os

EXTRACT_BATCH    = 50     # resoluciones por lote de sondeo al guardar resultados
EXTRACT_WORKERS  = 1      # un solo worker: la Batch API es asíncrona, no necesitamos más
BATCH_POLL_SECS  = 30     # segundos entre comprobaciones del estado del batch

SYSTEM_EXTRACT = """Eres un extractor de información jurídica especializado en contratación pública española.
Se te proporciona el texto de una resolución del TACRC (Tribunal Administrativo Central de Recursos Contractuales).
Extrae EXCLUSIVAMENTE la información solicitada. Si no puedes determinar un campo, usa null.
Responde SOLO con JSON válido, sin explicaciones ni markdown."""

USER_EXTRACT_TPL = """Texto de la resolución (primeros 3000 caracteres):
{texto}

Extrae:
1. organo_contratacion: nombre del órgano o entidad de contratación que licitó el contrato (no el TACRC).
   Ejemplos: "Ayuntamiento de Madrid", "Ministerio de Defensa", "Renfe Operadora".
2. importe_contrato: valor estimado o presupuesto base de licitación en euros como número decimal.
   Si aparece con IVA y sin IVA, usa el valor sin IVA. Si no hay importe, null.
3. tipo_contrato_llm: clasifica el contrato en UNO de estos valores exactos:
   "Servicios" | "Obras" | "Suministros" | "Concesión" | "Mixto" | "Desconocido"

Responde SOLO con este JSON:
{{"organo_contratacion": "...", "importe_contrato": 12345.67, "tipo_contrato_llm": "Servicios"}}"""


def _build_batch_request(row_id: int, numero: str, texto: str) -> dict:
    """Construye una línea JSONL para la Batch API."""
    texto_trunc = texto[:3000] if texto else ""
    return {
        "custom_id": str(row_id),
        "method":    "POST",
        "url":       "/v1/chat/completions",
        "body": {
            "model":       settings.llm_model,
            "temperature": 0.0,
            "max_tokens":  150,
            "messages": [
                {"role": "system", "content": SYSTEM_EXTRACT},
                {"role": "user",   "content": USER_EXTRACT_TPL.format(texto=texto_trunc)},
            ],
        },
    }


def _parse_llm_result(content: str) -> dict:
    """Parsea la respuesta JSON del LLM con tolerancia a errores."""
    try:
        # Limpiar posibles backticks
        clean = content.strip().replace("```json", "").replace("```", "").strip()
        parsed = _json.loads(clean)
        organo  = parsed.get("organo_contratacion") or None
        importe = parsed.get("importe_contrato")
        tipo    = parsed.get("tipo_contrato_llm") or "Desconocido"

        # Validar importe
        if importe is not None:
            try:
                importe = float(str(importe).replace(",", ".").replace(".", "", str(importe).count(".") - 1))
            except (ValueError, TypeError):
                importe = None

        # Validar tipo
        tipos_validos = {"Servicios", "Obras", "Suministros", "Concesión", "Mixto", "Desconocido"}
        if tipo not in tipos_validos:
            tipo = "Desconocido"

        return {"organo_contratacion": organo, "importe_contrato": importe, "tipo_contrato_llm": tipo}
    except Exception:
        return {"organo_contratacion": None, "importe_contrato": None, "tipo_contrato_llm": "Desconocido"}


async def ingest_llm_extraction(pool: asyncpg.Pool, force: bool = False):
    """
    Extrae órgano, importe y tipo de contrato de cada resolución con texto_pdf.
    Usa la Batch API de OpenAI para máxima eficiencia y mínimo coste.
    """
    where_pending = "" if force else "AND llm_extraido_at IS NULL"

    async with pool.acquire() as conn:
        pending = await conn.fetch(f"""
            SELECT id, numero, texto_pdf
            FROM resoluciones
            WHERE texto_pdf IS NOT NULL
              AND LENGTH(TRIM(texto_pdf)) > 100
              {where_pending}
            ORDER BY fecha DESC NULLS LAST
        """)

    total = len(pending)
    log.info(f"Resoluciones pendientes de extracción LLM: {total}")
    if not total:
        log.info("Nada que extraer.")
        return

    # Estimación de coste: ~150 tokens input + 50 output por resolución
    tokens_est = total * 200
    coste_est  = tokens_est / 1_000_000 * 0.30  # gpt-4o-mini batch: ~$0.15/1M input, $0.30/1M output aprox
    log.info(f"Estimación: ~{tokens_est:,} tokens, coste aprox ~${coste_est:.2f} USD")

    # ── Construir fichero JSONL ───────────────────────────────────────────────
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl",
                                     delete=False, encoding="utf-8") as f:
        batch_file_path = f.name
        for row in pending:
            req = _build_batch_request(row["id"], row["numero"], row["texto_pdf"] or "")
            f.write(_json.dumps(req, ensure_ascii=False) + "\n")

    log.info(f"Fichero JSONL preparado: {batch_file_path} ({os.path.getsize(batch_file_path)//1024} KB)")

    # ── Subir fichero y lanzar batch ─────────────────────────────────────────
    try:
        with open(batch_file_path, "rb") as f:
            batch_file = await openai.files.create(file=f, purpose="batch")

        log.info(f"Fichero subido: {batch_file.id}")

        batch = await openai.batches.create(
            input_file_id    = batch_file.id,
            endpoint         = "/v1/chat/completions",
            completion_window= "24h",
        )
        log.info(f"Batch lanzado: {batch.id} — esperando resultados…")

        # ── Sondeo hasta completar ────────────────────────────────────────────
        while True:
            batch = await openai.batches.retrieve(batch.id)
            counts = batch.request_counts
            pct = int((counts.completed + counts.failed) / max(counts.total, 1) * 100)
            log.info(
                f"Batch {batch.id}: {batch.status} | "
                f"✓{counts.completed} ✗{counts.failed} total={counts.total} ({pct}%)"
            )

            if batch.status in ("completed", "failed", "expired", "cancelled"):
                break

            await asyncio.sleep(BATCH_POLL_SECS)

        if batch.status != "completed":
            log.error(f"Batch terminó con estado: {batch.status}")
            return

        # ── Descargar y procesar resultados ──────────────────────────────────
        log.info(f"Batch completado. Descargando resultados…")
        result_content = await openai.files.content(batch.output_file_id)
        lines = result_content.text.strip().splitlines()

        ok = skip = errors = 0
        async with pool.acquire() as conn:
            for line in tqdm(lines, desc="Guardando extracciones LLM", unit="res"):
                try:
                    result = _json.loads(line)
                    row_id = int(result["custom_id"])

                    if result.get("error"):
                        log.warning(f"Error LLM para id={row_id}: {result['error']}")
                        errors += 1
                        continue

                    content = result["response"]["body"]["choices"][0]["message"]["content"]
                    parsed  = _parse_llm_result(content)

                    await conn.execute("""
                        UPDATE resoluciones SET
                            organo_contratacion = $1,
                            importe_contrato    = $2,
                            tipo_contrato_llm   = $3,
                            llm_extraido_at     = NOW()
                        WHERE id = $4
                    """,
                        parsed["organo_contratacion"],
                        parsed["importe_contrato"],
                        parsed["tipo_contrato_llm"],
                        row_id,
                    )
                    ok += 1
                except Exception as e:
                    log.warning(f"Error procesando línea: {e}")
                    errors += 1

        log.info(f"Extracción LLM finalizada: OK={ok} | Errores={errors} | Saltados={skip}")

        # Limpiar fichero temporal
        os.unlink(batch_file_path)

        # Borrar fichero de OpenAI para no acumular costes de almacenamiento
        try:
            await openai.files.delete(batch_file.id)
            if batch.output_file_id:
                await openai.files.delete(batch.output_file_id)
        except Exception:
            pass

    except Exception as e:
        log.error(f"Error en batch LLM: {e}")
        if os.path.exists(batch_file_path):
            os.unlink(batch_file_path)
        raise


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Pipeline de ingesta TACRC")
    p.add_argument("json_file",       type=Path)
    p.add_argument("--only-meta",     action="store_true")
    p.add_argument("--only-pdf",      action="store_true")
    p.add_argument("--only-embed",    action="store_true")
    p.add_argument("--only-extract",  action="store_true",
                   help="Solo extracción LLM (órgano, importe, tipo contrato)")
    p.add_argument("--force-extract", action="store_true",
                   help="Re-extraer aunque ya tenga llm_extraido_at")
    p.add_argument("--pdf-workers",   type=int, default=PDF_WORKERS,
                   help=f"PDFs en paralelo (default: {PDF_WORKERS})")
    return p.parse_args()


async def main():
    args = parse_args()
    global PDF_WORKERS
    PDF_WORKERS = args.pdf_workers

    if not args.json_file.exists():
        log.error(f"No se encuentra: {args.json_file}")
        sys.exit(1)

    log.info(f"Cargando {args.json_file}…")
    with open(args.json_file, encoding="utf-8") as f:
        items = json.load(f)
    log.info(f"{len(items)} resoluciones en el JSON")

    pool     = await get_pool()
    only_one = args.only_meta or args.only_pdf or args.only_embed or args.only_extract

    if not only_one or args.only_meta:
        log.info("── Paso 1: metadatos ──")
        n = await upsert_metadata(pool, items)
        log.info(f"   {n} registros insertados/actualizados")

    if not only_one or args.only_pdf:
        log.info("── Paso 2: extracción PDF (paralela) ──")
        await ingest_pdfs(pool)

    if not only_one or args.only_embed:
        log.info("── Paso 3: embeddings ──")
        await ingest_embeddings(pool)

    if not only_one or args.only_extract:
        log.info("── Paso 4: extracción LLM (órgano, importe, tipo contrato) ──")
        await ingest_llm_extraction(pool, force=args.force_extract)

    await pool.close()
    log.info("✓ Ingesta completada")


if __name__ == "__main__":
    asyncio.run(main())
