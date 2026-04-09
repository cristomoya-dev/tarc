# app.py — Frontend Streamlit para TACRC
import os
from urllib.parse import quote
import httpx
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

API = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="TACRC · Buscador de Resoluciones",
    page_icon="⚖️",
    layout="wide",
)

# Google Analytics se inyecta en el index.html de Streamlit vía entrypoint.sh

st.markdown(f"<style>{open('style.css').read()}</style>", unsafe_allow_html=True)

# ── Biblioteca de aspectos por tipo de contrato ───────────────────────────────

ASPECTOS = {
    "Comunes": [
        "Criterios de solvencia económica y financiera",
        "Criterios de solvencia técnica y profesional",
        "Habilitación empresarial o profesional requerida",
        "Criterios de adjudicación y su ponderación",
        "Criterios evaluables mediante juicio de valor",
        "Justificación de ofertas anormalmente bajas",
        "Condiciones de admisión de ofertas y causas de exclusión",
        "Límites de extensión de ofertas y consecuencias de superarlos",
        "Confidencialidad de la oferta: documentos protegidos",
        "Prohibiciones de contratar y causas de exclusión obligatorias",
        "Garantías: provisional, definitiva y complementaria",
        "División en lotes: justificación y condiciones",
        "Valor estimado del contrato y umbral de publicidad",
        "Subcontratación: condiciones, límites y obligaciones",
        "Modificaciones del contrato: supuestos previstos y límites",
        "Condiciones especiales de ejecución (cláusulas sociales)",
        "Obligaciones de contratación de personas con discapacidad",
        "Penalidades por incumplimiento y resolución del contrato",
        "Acceso al expediente y confidencialidad de las ofertas",
        "Seguros exigidos al contratista",
    ],
    "Servicios": [
        "Definición del objeto y prestaciones exigidas",
        "Medios personales y materiales mínimos exigidos",
        "Adscripción de medios: obligatoriedad y control",
        "Subrogación de trabajadores: obligaciones del pliego",
        "Convenio colectivo aplicable y costes salariales mínimos",
        "Desglose de costes salariales según convenio (art. 100.2 LCSP)",
        "Precio mínimo: conformidad con el coste laboral mínimo (art. 102.3 LCSP)",
        "Encargos a medios propios: requisitos y justificación",
        "Indicadores de calidad y sistema de evaluación del servicio",
        "Plan de trabajo y metodología: valoración y criterios",
        "Confidencialidad de datos personales tratados en la ejecución",
        "Cesión del contrato: condiciones y autorización",
    ],
    "Obras": [
        "Proyecto de obras: suficiencia y coherencia técnica",
        "Clasificación del contratista requerida (grupos y subgrupos)",
        "Revisión de precios: fórmula aplicable y condiciones",
        "Plazo de ejecución: justificación y penalidades por demora",
        "Plazo de garantía y obligaciones de conservación",
        "Control de calidad y pruebas de materiales",
        "Seguridad y salud: plan y coordinador",
        "Gestión de residuos de construcción y demolición",
        "Actas de replanteo y disponibilidad de terrenos",
        "Modificados de obra: supuestos y límites del 10/20 por ciento",
        "Recepción de la obra y plazo de garantía",
    ],
    "Suministros": [
        "Descripción técnica y especificaciones del bien",
        "Homologaciones, certificaciones y normas técnicas exigidas",
        "Plazo de entrega y penalidades por retraso",
        "Garantía del producto: plazos y cobertura",
        "Servicio postventa y asistencia técnica",
        "Recepción y conformidad: procedimiento y plazos",
        "Entrega parcial y recepciones parciales",
        "Devolución y sustitución de bienes defectuosos",
    ],
    "Concesiones": [
        "Retribución del concesionario y riesgo operacional",
        "Equilibrio económico-financiero del contrato",
        "Canon concesional: cálculo y actualización",
        "Duración de la concesión y prórroga",
        "Reversión de bienes al finalizar la concesión",
        "Inversiones mínimas y plan de negocio",
        "Tarifas a usuarios: régimen de aprobación y modificación",
        "Rescisión anticipada e indemnización al concesionario",
    ],
}

def aspectos_para_tipos(tipos: list) -> str:
    vistos = set()
    lineas = []
    for tipo in tipos:
        for aspecto in ASPECTOS.get(tipo, []):
            if aspecto not in vistos:
                vistos.add(aspecto)
                lineas.append(f"- {aspecto}")
    return "\n".join(lineas)

# ── Helpers ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_filtros():
    # No capturamos excepciones aquí: st.cache_data no cachea errores,
    # así que un fallo temporal no queda grabado 5 minutos.
    r = httpx.get(f"{API}/filtros", timeout=10)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=60)
def get_stats():
    try:
        r = httpx.get(f"{API}/stats", timeout=10)
        return r.json()
    except Exception:
        return {}

def buscar(params: dict) -> dict:
    try:
        r = httpx.get(f"{API}/buscar", params=params, timeout=30)
        if r.status_code != 200:
            return {"total": 0, "resultados": [], "error": f"HTTP {r.status_code}: {r.text[:500]}"}
        return r.json()
    except Exception as e:
        return {"total": 0, "resultados": [], "error": str(e)}

def chat_api(payload: dict) -> dict:
    try:
        r = httpx.post(f"{API}/chat", json=payload, timeout=60)
        return r.json()
    except Exception as e:
        return {"respuesta": f"Error: {e}", "fuentes": []}

def resumen_api(numero: str) -> dict:
    try:
        numero_enc = numero.replace("/", "_")
        r = httpx.get(f"{API}/resoluciones/{numero_enc}/resumen", timeout=60)
        if r.status_code == 422:
            return {"error": r.json().get("detail", "Sin texto PDF disponible.")}
        if r.status_code == 404:
            return {"error": "Resolución no encontrada."}
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def url_safe(url: str) -> str:
    return quote(url, safe=":/") if url else "#"

def fmt_fecha(fecha_str: str) -> str:
    """Convierte YYYY-MM-DD → DD/MM/YYYY para mostrar en español."""
    if not fecha_str:
        return ""
    try:
        from datetime import date as _date
        d = _date.fromisoformat(str(fecha_str)[:10])
        return d.strftime("%d/%m/%Y")
    except Exception:
        return str(fecha_str)[:10]

def tabla_html(rows_html: str, cabeceras: list, css_id: str) -> str:
    ths = "".join(f"<th>{c}</th>" for c in cabeceras)
    return f"""
    <style>
    .{css_id} {{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
    .{css_id} th {{background:#1a4f9e;color:white;padding:8px 10px;text-align:left;font-weight:500}}
    .{css_id} td {{padding:6px 10px;border-bottom:1px solid #e5e7eb;vertical-align:top}}
    .{css_id} tr:hover td {{background:#f0f4ff}}
    .{css_id} a {{color:#1a4f9e;text-decoration:none;font-weight:600}}
    .{css_id} a:hover {{text-decoration:underline}}
    </style>
    <table class="{css_id}">
    <thead><tr>{ths}</tr></thead>
    <tbody>{rows_html}</tbody>
    </table>"""

# ── Render de resumen estructurado ───────────────────────────────────────────

def _render_resumen(r: dict):
    fallo = r.get("fallo") or "—"
    if "Desestimación" in fallo:
        fallo_color = "#f59e0b"
    elif "Inadmisión" in fallo:
        fallo_color = "#6b7280"
    else:
        fallo_color = "#10b981"

    def _puntos(items: list) -> str:
        if not items:
            return "<li style='color:#9ca3af;font-size:12px'>Sin información</li>"
        return "".join(f"<li style='margin-bottom:3px;font-size:12px'>{item}</li>" for item in items)

    lbl = ("color:#6b7280;font-size:10px;text-transform:uppercase;"
           "letter-spacing:.05em;display:block;margin-bottom:2px")

    st.html(f"""
    <div style="border:1px solid #dde3ef;border-radius:8px;padding:16px 20px;
                background:#f8faff;margin-top:12px;font-family:sans-serif">

      <div style="display:flex;justify-content:space-between;align-items:baseline;
                  margin-bottom:12px">
        <strong style="color:#1a4f9e;font-size:15px">
          Resolución {r.get("numero","—")}
        </strong>
        <div style="background:{fallo_color}18;border:1px solid {fallo_color}44;
                    padding:3px 10px;border-radius:20px">
          <span style="font-size:12px;font-weight:600;color:{fallo_color}">{fallo}</span>
        </div>
      </div>

      <table style="width:100%;border-collapse:collapse;margin-bottom:12px;font-size:13px">
        <tr>
          <td style="width:33%;padding:4px 10px 4px 0;vertical-align:top">
            <span style="{lbl}">Tipo de contrato</span>
            <strong>{r.get("tipo_contrato") or "—"}</strong>
          </td>
          <td style="width:33%;padding:4px 10px;vertical-align:top">
            <span style="{lbl}">Valor estimado</span>
            <strong>{r.get("importe") or "—"}</strong>
          </td>
          <td style="width:34%;padding:4px 0 4px 10px;vertical-align:top">
            <span style="{lbl}">Contrato</span>
            {r.get("objeto") or "—"}
          </td>
        </tr>
        <tr>
          <td style="padding:4px 10px 0 0;vertical-align:top" colspan="2">
            <span style="{lbl}">Recurrente</span>
            {r.get("recurrente") or "—"}
          </td>
          <td style="padding:4px 0 0 10px;vertical-align:top">
            <span style="{lbl}">Órgano de contratación</span>
            {r.get("organo_contratacion") or "—"}
          </td>
        </tr>
      </table>

      <hr style="border:none;border-top:1px solid #dde3ef;margin:10px 0">

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
        <div>
          <p style="margin:0 0 4px 0;font-size:11px;font-weight:700;color:#1a4f9e;
                    text-transform:uppercase;letter-spacing:.04em">Motivos del recurso</p>
          <ol style="margin:0;padding-left:16px;line-height:1.6">
            {_puntos(r.get("razones_recurso", []))}
          </ol>
        </div>
        <div>
          <p style="margin:0 0 4px 0;font-size:11px;font-weight:700;color:#374151;
                    text-transform:uppercase;letter-spacing:.04em">Razones del Tribunal</p>
          <ol style="margin:0;padding-left:16px;line-height:1.6">
            {_puntos(r.get("razones_tribunal", []))}
          </ol>
        </div>
      </div>

    </div>
    """)


# ── Login (protege análisis de pliegos y estado del sistema) ─────────────────

LOGIN_PASSWORD = os.getenv("APP_PASSWORD")  # sin fallback: debe estar configurada explícitamente

def check_login():
    """Devuelve True si el usuario está autenticado."""
    return st.session_state.get("autenticado", False)

def mostrar_login(seccion: str):
    """Muestra el formulario de login. Devuelve True si se autentica."""
    if not LOGIN_PASSWORD:
        st.error("⚠️ La variable de entorno APP_PASSWORD no está configurada. Contacta al administrador.")
        return False
    st.warning(f"🔒 La sección **{seccion}** requiere autenticación.")
    with st.form(f"form_login_{seccion}", clear_on_submit=True):
        pwd = st.text_input("Contraseña", type="password", label_visibility="collapsed",
                            placeholder="Introduce la contraseña…")
        ok  = st.form_submit_button("Acceder", type="primary")
    if ok:
        if pwd == LOGIN_PASSWORD:
            st.session_state["autenticado"] = True
            st.rerun()
        else:
            st.error("Contraseña incorrecta.")
    return False

# ── Header ────────────────────────────────────────────────────────────────────

st.title("⚖️ TACRC · Resoluciones Contractuales")

try:
    filtros = get_filtros()
except Exception:
    filtros = {"sentidos": [], "leyes": [], "anios": []}
stats   = get_stats()
ingesta = stats.get("ingesta", {})

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total resoluciones",  ingesta.get("total", "—"))
col2.metric("Con texto PDF",       ingesta.get("con_texto_pdf", "—"))
col3.metric("Con embedding",       ingesta.get("con_embedding", "—"))
col4.metric("Más reciente",        ingesta.get("fecha_mas_reciente", "—"))

st.divider()

tab_buscar, tab_chat, tab_pliego, tab_dashboard = st.tabs([
    "🔍 Buscar resoluciones",
    "💬 Chat jurídico",
    "📋 Analizar pliego",
    "📊 Estado del sistema",
])

# ── TAB 1: Búsqueda ───────────────────────────────────────────────────────────

with tab_buscar:
    # Estado de paginación y resumen persistente entre reruns
    if "buscar_params"  not in st.session_state: st.session_state["buscar_params"]  = {}
    if "buscar_page"    not in st.session_state: st.session_state["buscar_page"]    = 1
    if "resumen_data"   not in st.session_state: st.session_state["resumen_data"]   = None
    if "resumen_numero" not in st.session_state: st.session_state["resumen_numero"] = None

    subtab_int, subtab_cam = st.tabs(["🔍 Búsqueda inteligente", "📋 Búsqueda por campos"])

    # ── Sub-tab 1: Búsqueda semántica / exacta ────────────────────────────────
    with subtab_int:
        with st.form("form_buscar_inteligente"):
            c_q, c_modo = st.columns([5, 1])
            q = c_q.text_input(
                "Búsqueda",
                placeholder="ej: 'solvencia económica insuficiente' o 'criterios adjudicación subjetivos'",
            )
            modo_busqueda = c_modo.radio(
                "Modo",
                ["Semántico", "Exacto"],
                index=0,
                help=(
                    "**Semántico**: busca por significado y conceptos relacionados.\n\n"
                    "**Exacto**: busca la palabra o frase literalmente en el texto. "
                    "Ideal para nombres propios, municipios o empresas."
                ),
            )
            c3, c4, c5 = st.columns(3)
            sentido_i   = c3.selectbox("Sentido",    ["(todos)"] + filtros.get("sentidos", []), key="bi_sentido")
            ley_i       = c4.selectbox("Ley",        ["(todos)"] + filtros.get("leyes",    []), key="bi_ley")
            page_size_i = c5.selectbox("Por página", [20, 50, 100], index=0,                    key="bi_ps")
            submitted_i = st.form_submit_button("Buscar", type="primary", use_container_width=True)

        if submitted_i:
            params_i = {"page_size": page_size_i}
            if q:
                params_i["q"]    = q
                params_i["modo"] = "exacto" if modo_busqueda == "Exacto" else "semantico"
            if sentido_i != "(todos)": params_i["sentido"] = sentido_i
            if ley_i     != "(todos)": params_i["ley"]     = ley_i
            st.session_state["buscar_params"]  = params_i
            st.session_state["buscar_page"]    = 1
            st.session_state["resumen_data"]   = None
            st.session_state["resumen_numero"] = None

    # ── Sub-tab 2: Búsqueda por campos / metadatos ────────────────────────────
    with subtab_cam:
        with st.form("form_buscar_campos"):
            c_num, c_txt = st.columns(2)
            numero_q = c_num.text_input(
                "Nº resolución",
                placeholder="ej: 0340/2026",
                help="Busca por número de resolución. Tiene prioridad sobre el campo de texto si ambos están rellenos.",
            )
            texto_q = c_txt.text_input(
                "Texto en descripción o contenido",
                placeholder="ej: Ayuntamiento de Totana",
            )
            c_des, c_has, c_sen, c_ley2, c_ps2 = st.columns(5)
            fecha_desde = c_des.date_input("Desde",      value=None, format="DD/MM/YYYY", key="bc_desde")
            fecha_hasta = c_has.date_input("Hasta",      value=None, format="DD/MM/YYYY", key="bc_hasta")
            sentido_c   = c_sen.selectbox("Sentido",    ["(todos)"] + filtros.get("sentidos", []), key="bc_sentido")
            ley_c       = c_ley2.selectbox("Ley",       ["(todos)"] + filtros.get("leyes",    []), key="bc_ley")
            page_size_c = c_ps2.selectbox("Por página", [20, 50, 100], index=0,                    key="bc_ps")
            submitted_c = st.form_submit_button("Buscar por campos", type="primary", use_container_width=True)

        if submitted_c:
            params_c = {"page_size": page_size_c}
            q_campos = numero_q.strip() or texto_q.strip()
            if q_campos:
                params_c["q"]    = q_campos
                params_c["modo"] = "exacto"
            if fecha_desde: params_c["fecha_desde"] = fecha_desde.strftime("%Y-%m-%d")
            if fecha_hasta: params_c["fecha_hasta"] = fecha_hasta.strftime("%Y-%m-%d")
            if sentido_c != "(todos)": params_c["sentido"] = sentido_c
            if ley_c     != "(todos)": params_c["ley"]     = ley_c
            st.session_state["buscar_params"]  = params_c
            st.session_state["buscar_page"]    = 1
            st.session_state["resumen_data"]   = None
            st.session_state["resumen_numero"] = None

    params       = st.session_state["buscar_params"]
    current_page = st.session_state["buscar_page"]

    if params:
        with st.spinner("Buscando…"):
            resultado = buscar({**params, "page": current_page})

        if "error" in resultado:
            st.error(f"Error al conectar con la API: {resultado['error']}")

        total       = resultado.get("total", 0)
        rows        = resultado.get("resultados", [])
        ps          = params.get("page_size", 20)
        total_pages = max(1, (total + ps - 1) // ps)
        if params.get("fecha_desde") or params.get("fecha_hasta"):
            modo_label = "búsqueda por campos"
        elif params.get("modo") == "exacto":
            modo_label = "búsqueda exacta"
        elif params.get("modo") == "semantico":
            modo_label = "búsqueda semántica"
        else:
            modo_label = "filtrado por metadatos"

        if rows:
            st.caption(
                f"**{total:,}** resoluciones encontradas · "
                f"página {current_page} de {total_pages} · {modo_label}"
            )

            # Caché de resúmenes entre reruns
            if "buscar_resumen_cache" not in st.session_state:
                st.session_state["buscar_resumen_cache"] = {}

            # ── Ordenación local ──────────────────────────────────────────────
            _sort_opts = {
                "Relevancia (por defecto)": (None, False),
                "Fecha ↓ (más reciente)":   ("fecha", True),
                "Fecha ↑ (más antigua)":    ("fecha", False),
            }
            sort_label = st.selectbox(
                "Ordenar por",
                list(_sort_opts.keys()),
                label_visibility="collapsed",
                key="buscar_sort",
            )
            sort_key, sort_desc = _sort_opts[sort_label]
            if sort_key:
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(sort_key) or ""),
                    reverse=sort_desc,
                )

            # ── Cabecera de la tabla ──────────────────────────────────────────
            h = st.columns([1.8, 1, 1, 1.8, 3.8, 1.1])
            for label, col in zip(
                ["Número", "Fecha", "Sentido", "Tipo de recurso", "Descripción", ""],
                h,
            ):
                col.markdown(f"<span style='font-size:12px;color:#6b7280;font-weight:600'>{label}</span>",
                             unsafe_allow_html=True)
            st.markdown("<hr style='margin:4px 0 8px 0;border-color:#e5e7eb'>", unsafe_allow_html=True)

            # ── Filas con popover de resumen ──────────────────────────────────
            for i, row in enumerate(rows):
                num = row.get("numero", "")
                url = row.get("pdf_url") or ""
                enlace = f"[{num}]({url_safe(url)})" if url else f"`{num}`"
                c = st.columns([1.8, 1, 1, 1.8, 3.8, 1.1])
                c[0].markdown(enlace)
                c[1].markdown(f"<span style='font-size:13px'>{row.get('fecha') or '—'}</span>",
                              unsafe_allow_html=True)
                c[2].markdown(f"<span style='font-size:13px'>{row.get('sentido') or '—'}</span>",
                              unsafe_allow_html=True)
                c[3].markdown(f"<span style='font-size:13px'>{(row.get('tipo_recurso') or '—')[:55]}</span>",
                              unsafe_allow_html=True)
                c[4].markdown(f"<span style='font-size:13px'>{(row.get('descripcion') or '—')[:300]}</span>",
                              unsafe_allow_html=True)
                with c[5].popover("Resumir", use_container_width=True):
                    cache = st.session_state["buscar_resumen_cache"]
                    if num not in cache:
                        with st.spinner("Analizando…"):
                            cache[num] = resumen_api(num)
                        st.session_state["buscar_resumen_cache"] = cache
                    data = cache[num]
                    if "error" in data:
                        st.warning(data["error"])
                    else:
                        _render_resumen(data)

            st.markdown("<hr style='margin:8px 0 12px 0;border-color:#e5e7eb'>", unsafe_allow_html=True)

            # ── Exportar CSV ──────────────────────────────────────────────────
            df = pd.DataFrame(rows)
            csv = df[["numero","fecha","sentido","ley_impugnada","tipo_recurso","descripcion","pdf_url"]]\
                    .to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇ Exportar CSV", csv, "tacrc_resultados.csv", "text/csv")

            # ── Controles de paginación ───────────────────────────────────────
            if total_pages > 1:
                cp, ci, cn = st.columns([1, 2, 1])
                if cp.button("← Anterior", disabled=current_page <= 1, use_container_width=True):
                    st.session_state["buscar_page"] -= 1
                    st.rerun()
                ci.markdown(
                    f"<p style='text-align:center;padding-top:6px'>"
                    f"Página <b>{current_page}</b> de <b>{total_pages}</b></p>",
                    unsafe_allow_html=True,
                )
                if cn.button("Siguiente →", disabled=current_page >= total_pages, use_container_width=True):
                    st.session_state["buscar_page"] += 1
                    st.rerun()
        else:
            st.info("No se encontraron resoluciones con esos criterios.")

# ── TAB 2: Chat RAG ───────────────────────────────────────────────────────────

with tab_chat:
    # ── Inicializar estado del chat ───────────────────────────────────────────
    if "chat_conv_id"      not in st.session_state: st.session_state["chat_conv_id"]      = None
    if "chat_mensajes"     not in st.session_state: st.session_state["chat_mensajes"]     = []
    if "chat_conv_titulo"  not in st.session_state: st.session_state["chat_conv_titulo"]  = ""
    if "chat_sugerencias"  not in st.session_state: st.session_state["chat_sugerencias"]  = []

    # ── Layout: sidebar historial | chat principal ────────────────────────────
    col_hist, col_main = st.columns([1, 3])

    # ── PANEL IZQUIERDO: historial ────────────────────────────────────────────
    with col_hist:
        st.markdown("**Conversaciones**")

        if st.button("＋ Nueva", key="btn_nueva_conv", use_container_width=True):
            st.session_state["chat_conv_id"]     = None
            st.session_state["chat_mensajes"]    = []
            st.session_state["chat_conv_titulo"] = ""

        try:
            convs = httpx.get(f"{API}/conversaciones?limit=40", timeout=5).json()
        except Exception:
            convs = []

        for conv in convs:
            cid    = conv.get("id")
            titulo = conv.get("titulo") or f"Conversación {cid}"
            turnos = conv.get("total_turnos", 0)
            fecha  = (conv.get("actualizado_at") or "")[:10]
            activa = st.session_state["chat_conv_id"] == cid

            bg  = "var(--color-background-info)"    if activa else "var(--color-background-secondary)"
            col_txt = "var(--color-text-info)"      if activa else "var(--color-text-primary)"

            c1, c2 = st.columns([5, 1])
            if c1.button(
                f"{titulo[:28]}{'…' if len(titulo)>28 else ''}",
                key=f"conv_{cid}",
                use_container_width=True,
                help=f"{fecha} · {turnos // 2} {'pregunta' if turnos // 2 == 1 else 'preguntas'}",
            ):
                # Cargar mensajes de esta conversación
                try:
                    msgs = httpx.get(f"{API}/conversaciones/{cid}", timeout=10).json()
                    st.session_state["chat_conv_id"]     = cid
                    st.session_state["chat_mensajes"]    = msgs
                    st.session_state["chat_conv_titulo"] = titulo
                except Exception:
                    st.error("Error cargando conversación")

            if c2.button("🗑", key=f"del_{cid}", help="Eliminar"):
                try:
                    httpx.delete(f"{API}/conversaciones/{cid}", timeout=5)
                    if st.session_state["chat_conv_id"] == cid:
                        st.session_state["chat_conv_id"]  = None
                        st.session_state["chat_mensajes"] = []
                    st.rerun()
                except Exception:
                    st.error("Error eliminando")

    # ── PANEL DERECHO: chat ───────────────────────────────────────────────────
    with col_main:
        # Título de la conversación actual
        conv_id = st.session_state.get("chat_conv_id")
        if conv_id:
            titulo_actual = st.session_state.get("chat_conv_titulo", "")
            nuevo_titulo = st.text_input(
                "Título", value=titulo_actual, label_visibility="collapsed",
                placeholder="Nombre de la conversación…", key="input_titulo_conv"
            )
            if nuevo_titulo != titulo_actual and nuevo_titulo.strip():
                try:
                    httpx.patch(f"{API}/conversaciones/{conv_id}/titulo",
                                params={"titulo": nuevo_titulo}, timeout=5)
                    st.session_state["chat_conv_titulo"] = nuevo_titulo
                except Exception:
                    pass
        else:
            st.caption("Nueva conversación — se guardará al enviar el primer mensaje")

        # ── Mostrar historial de mensajes ─────────────────────────────────────
        mensajes = st.session_state.get("chat_mensajes", [])
        for msg in mensajes:
            role     = msg.get("role", "user")
            contenido = msg.get("contenido", "")

            if role == "user":
                st.markdown(
                    f'<div style="display:flex;justify-content:flex-end;margin:8px 0">'
                    f'<div style="background:var(--color-background-info);color:var(--color-text-info);'
                    f'padding:10px 14px;border-radius:12px 12px 0 12px;max-width:85%;font-size:14px">'
                    f'{contenido}</div></div>',
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f'<div style="background:var(--color-background-secondary);border:1px solid '
                    f'var(--color-border-tertiary);border-radius:0 12px 12px 12px;padding:12px 16px;'
                    f'margin:8px 0;font-size:14px;line-height:1.7">{contenido}</div>',
                    unsafe_allow_html=True
                )
                # Mostrar fragmentos del mensaje asistente si los tiene
                frags = msg.get("fragmentos", [])
                if frags:
                    with st.expander(f"Ver {len(frags)} fragmentos recuperados"):
                        palabras = []
                        for m2 in mensajes:
                            if m2.get("role") == "user":
                                palabras = [w.strip("¿?.,;:()[]").lower()
                                           for w in m2.get("contenido","").split()
                                           if len(w.strip("¿?.,;:()[]")) > 3]
                                break
                        for frag in frags:
                            s = frag.get("sentido","") or ""
                            col_s = {"Estimación":"#10b981","Estimación parcial":"#34d399",
                                     "Desestimación":"#f59e0b","Inadmisión":"#6b7280"}.get(s,"#6b7280")
                            texto_f = frag.get("texto","")
                            if palabras:
                                import re as _re
                                pat = _re.compile(
                                    r'(\b(?:' + '|'.join(_re.escape(p) for p in palabras) + r')\b)',
                                    _re.IGNORECASE
                                )
                                texto_f = pat.sub(
                                    r'<mark style="background:#fef08a;color:#713f12;'
                                    r'border-radius:3px;padding:0 2px">\1</mark>',
                                    texto_f
                                )
                            pdf_link = ""
                            if frag.get("pdf_url"):
                                pdf_link = f'<a href="{url_safe(frag["pdf_url"])}" target="_blank" style="font-size:11px;color:#1a4f9e">PDF ↗</a>'
                            st.html(f"""
                            <div style="border-left:3px solid {col_s};padding:8px 12px;
                                        margin-bottom:8px;font-size:12px;line-height:1.6">
                              <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                                <span style="font-weight:600;color:#1a4f9e">{frag.get('numero','')}</span>
                                <span style="color:{col_s};font-size:11px">{s} {pdf_link}</span>
                              </div>
                              {texto_f}
                            </div>""")

        # Separador visual si hay historial
        if mensajes:
            st.markdown("---")

        # ── Preguntas sugeridas ───────────────────────────────────────────────
        sugerencias = st.session_state.get("chat_sugerencias", [])
        if sugerencias:
            st.caption("Preguntas sugeridas:")
            sug_cols = st.columns(len(sugerencias))
            for si, (sc, sug) in enumerate(zip(sug_cols, sugerencias)):
                if sc.button(
                    sug,
                    key=f"sug_{si}_{hash(sug) % 10000}",
                    use_container_width=True,
                    help="Haz clic para enviar esta pregunta",
                ):
                    # Enviar la sugerencia como nueva pregunta
                    payload = {
                        "pregunta":        sug,
                        "top_k":           8,
                        "conversacion_id": st.session_state.get("chat_conv_id"),
                    }
                    with st.spinner("Consultando resoluciones…"):
                        resp_sug = chat_api(payload)
                    if resp_sug.get("conversacion_id"):
                        st.session_state["chat_conv_id"] = resp_sug["conversacion_id"]
                    st.session_state["chat_mensajes"].append({
                        "role": "user", "contenido": sug,
                        "fuentes": [], "fragmentos": [],
                    })
                    st.session_state["chat_mensajes"].append({
                        "role":       "assistant",
                        "contenido":  resp_sug.get("respuesta", ""),
                        "fuentes":    resp_sug.get("fuentes", []),
                        "fragmentos": resp_sug.get("fragmentos", []),
                    })
                    st.session_state["chat_sugerencias"] = resp_sug.get("sugerencias", [])
                    st.rerun()

        # ── Formulario de nueva pregunta ──────────────────────────────────────
        with st.form("form_chat", clear_on_submit=True):
            pregunta = st.text_area(
                "Tu pregunta",
                placeholder="ej: ¿Cuándo es insuficiente el informe de auditor para acreditar solvencia?",
                height=90,
                label_visibility="collapsed",
            )
            c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
            chat_anio    = c1.selectbox("Año",     ["(todos)"] + [str(a) for a in filtros.get("anios", [])], key="chat_anio")
            chat_sentido = c2.selectbox("Sentido", ["(todos)"] + filtros.get("sentidos", []), key="chat_sentido")
            top_k        = c3.slider("Fragmentos", 4, 20, 8)
            enviar       = c4.form_submit_button("Enviar →", type="primary")

        if enviar and pregunta.strip():
            payload = {
                "pregunta":        pregunta,
                "top_k":           top_k,
                "conversacion_id": st.session_state.get("chat_conv_id"),
            }
            if chat_anio != "(todos)":    payload["anio"]    = int(chat_anio)
            if chat_sentido != "(todos)":
                payload["sentido"] = chat_sentido

            with st.spinner("Consultando resoluciones…"):
                resp = chat_api(payload)

            # Actualizar session_state con la nueva conversación/mensajes
            if resp.get("conversacion_id"):
                st.session_state["chat_conv_id"] = resp["conversacion_id"]

            # Añadir los dos nuevos mensajes al historial local
            st.session_state["chat_mensajes"].append({
                "role": "user", "contenido": pregunta,
                "fuentes": [], "fragmentos": [],
            })
            st.session_state["chat_mensajes"].append({
                "role":       "assistant",
                "contenido":  resp.get("respuesta", ""),
                "fuentes":    resp.get("fuentes", []),
                "fragmentos": resp.get("fragmentos", []),
            })
            # Guardar sugerencias del último turno
            st.session_state["chat_sugerencias"] = resp.get("sugerencias", [])
            st.rerun()


# ── TAB 3: Análisis de pliego ─────────────────────────────────────────────────

with tab_pliego:
    st.markdown(
        "Sube un pliego y opcionalmente la memoria del contrato. "
        "El sistema analiza la conformidad con la doctrina del TACRC."
    )

    # ── Uploaders ─────────────────────────────────────────────────────────────
    col_up1, col_up2 = st.columns(2)
    uploaded        = col_up1.file_uploader("📄 Pliego (PCAP / PPT)", type=["pdf"], key="up_pliego")
    uploaded_memoria = col_up2.file_uploader(
        "📋 Memoria del contrato (opcional)",
        type=["pdf"],
        key="up_memoria",
        help="La memoria justifica las cláusulas del pliego. Subirla mejora mucho la precisión del análisis.",
    )

    # ── Selector de tipo de contrato ──────────────────────────────────────────
    st.markdown("#### Tipo de contrato")
    tipos_extra = [t for t in ASPECTOS.keys() if t != "Comunes"]
    cols = st.columns(len(tipos_extra))
    seleccionados = ["Comunes"]
    for i, tipo in enumerate(tipos_extra):
        if cols[i].checkbox(tipo, key=f"chk_{tipo}"):
            seleccionados.append(tipo)

    aspectos_auto = aspectos_para_tipos(seleccionados)
    st.markdown("#### Aspectos a analizar")
    st.caption("Edita libremente.")
    aspectos = st.text_area(
        "aspectos", value=aspectos_auto, height=260,
        label_visibility="collapsed", key="aspectos_pliego",
    )

    c1, _ = st.columns([1, 3])
    top_k_p = c1.slider("Resoluciones por aspecto", 3, 15, 6, key="pliego_topk")

    # ── Botones ───────────────────────────────────────────────────────────────
    b1, b2, b3 = st.columns(3)
    btn_analizar = b1.button(
        "🔍 Analizar pliego",
        type="primary",
        disabled=uploaded is None,
        key="btn_analizar_pliego",
    )
    btn_riesgo = b2.button(
        "⚠️ Análisis de riesgo",
        type="secondary",
        disabled=uploaded is None,
        key="btn_riesgo_pliego",
    )
    btn_limpiar_todo = b3.button(
        "🗑️ Limpiar resultados",
        key="btn_limpiar_todo",
    )

    if btn_limpiar_todo:
        for k in list(st.session_state.keys()):
            if k.startswith("hist_") or k in ("riesgo_resp", "riesgo_nombre",
                                               "analisis_resp", "analisis_nombre"):
                del st.session_state[k]
        st.rerun()

    # ── ANALIZAR PLIEGO ───────────────────────────────────────────────────────
    if btn_analizar and uploaded:
        lista = [a.strip("- ").strip() for a in aspectos.strip().splitlines() if a.strip()]
        st.info(f"Analizando **{len(lista)} aspectos**…")

        files = {"pdf": (uploaded.name, uploaded.getvalue(), "application/pdf")}
        if uploaded_memoria:
            files["memoria"] = (uploaded_memoria.name, uploaded_memoria.getvalue(), "application/pdf")
        data = {"aspectos": aspectos, "top_k": str(top_k_p)}

        with st.spinner("Consultando doctrina TACRC…"):
            try:
                r = httpx.post(f"{API}/analizar_pliego", files=files, data=data, timeout=180)
                if r.status_code == 404:
                    # Fallback: usar riesgo_pliego si analizar_pliego no existe
                    r = httpx.post(f"{API}/riesgo_pliego", files=files, data=data, timeout=180)
                resp = r.json()
                st.session_state["analisis_resp"]   = resp
                st.session_state["analisis_nombre"] = uploaded.name
            except Exception as e:
                st.error(f"Error: {e}")

    if "analisis_resp" in st.session_state:
        resp = st.session_state["analisis_resp"]
        st.markdown(f"### Análisis: `{st.session_state.get('analisis_nombre','')}`")
        # Puede venir de analizar_pliego (campo "analisis") o riesgo_pliego (campo "resumen")
        texto = resp.get("analisis") or resp.get("resumen") or ""
        st.markdown(texto)

        fuentes = resp.get("fuentes", [])
        if fuentes:
            with st.expander(f"Resoluciones consultadas ({len(fuentes)})"):
                rows_html = ""
                for f in fuentes:
                    u = url_safe(f.get("pdf_url", ""))
                    n = f.get("numero", "")
                    enlace = f'<a href="{u}" target="_blank">{n}</a>' if u != "#" else n
                    rows_html += f"""<tr>
                        <td>{enlace}</td>
                        <td style="white-space:nowrap">{f.get('fecha','') or ''}</td>
                        <td>{f.get('sentido','') or ''}</td>
                        <td>{(f.get('descripcion','') or '')[:200]}</td>
                    </tr>"""
                st.html(tabla_html(rows_html,
                    ["Resolución","Fecha","Sentido","Descripción"], "fuentes-analisis"))

    # ── ANÁLISIS DE RIESGO ────────────────────────────────────────────────────
    if btn_riesgo and uploaded:
        lista = [a.strip("- ").strip() for a in aspectos.strip().splitlines() if a.strip()]
        st.info(f"Calculando riesgo para **{len(lista)} aspectos**… 2-3 minutos.")

        files = {"pdf": (uploaded.name, uploaded.getvalue(), "application/pdf")}
        if uploaded_memoria:
            files["memoria"] = (uploaded_memoria.name, uploaded_memoria.getvalue(), "application/pdf")
        data = {"aspectos": aspectos, "top_k": str(top_k_p)}

        with st.status("Analizando riesgo de recurso…", expanded=True) as _status:
            st.write(f"🔍 Buscando resoluciones TACRC relevantes para {len(lista)} aspectos…")
            st.write("⚖️ Evaluando cada aspecto frente a la doctrina del tribunal…")
            st.write("⏱️ Proceso estimado: 2-3 min. No cierres esta ventana.")
            try:
                r = httpx.post(f"{API}/riesgo_pliego", files=files, data=data, timeout=240)
                resp = r.json()
                st.session_state["riesgo_resp"]   = resp
                st.session_state["riesgo_nombre"] = uploaded.name
                # Limpiar hilos anteriores al hacer nuevo análisis
                for k in list(st.session_state.keys()):
                    if k.startswith("hist_"):
                        del st.session_state[k]
                n_asp = len(resp.get("aspectos", []))
                _status.update(
                    label=f"✅ Análisis completado — {n_asp} aspectos evaluados",
                    state="complete",
                    expanded=False,
                )
            except Exception as e:
                _status.update(label="❌ Error en el análisis", state="error", expanded=True)
                st.error(f"Error: {e}")

    # ── MOSTRAR RESULTADOS DE RIESGO (persisten en session_state) ─────────────
    if "riesgo_resp" in st.session_state:
        COLOR_NIVEL = {
            "ALTO": "#ef4444", "MEDIO": "#f59e0b",
            "BAJO": "#10b981", "SIN_DATOS": "#6b7280",
        }
        resp          = st.session_state["riesgo_resp"]
        riesgo_global = resp.get("riesgo_global", "MEDIO")
        score_global  = resp.get("score_global", 0)
        col_global    = COLOR_NIVEL.get(riesgo_global, "#6b7280")
        n_alto  = sum(1 for a in resp.get("aspectos", []) if a.get("nivel") == "ALTO")
        n_medio = sum(1 for a in resp.get("aspectos", []) if a.get("nivel") == "MEDIO")
        n_bajo  = sum(1 for a in resp.get("aspectos", []) if a.get("nivel") == "BAJO")

        st.html(f"""
        <div style="border:2px solid {col_global};border-radius:12px;padding:18px 22px;margin:14px 0">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <div>
              <div style="font-size:12px;color:#6b7280;margin-bottom:4px">
                {st.session_state.get("riesgo_nombre","pliego.pdf")} · Riesgo global
              </div>
              <div style="font-size:26px;font-weight:700;color:{col_global}">{riesgo_global}</div>
              <div style="font-size:13px;color:#374151;margin-top:6px">{resp.get("resumen","")}</div>
            </div>
            <div style="text-align:center;min-width:80px">
              <div style="font-size:42px;font-weight:700;color:{col_global}">{score_global}</div>
              <div style="font-size:11px;color:#6b7280">/ 100</div>
            </div>
          </div>
          <div style="display:flex;gap:10px;margin-top:12px">
            <span style="background:#fef2f2;color:#ef4444;padding:2px 10px;border-radius:20px;font-size:12px">{n_alto} alto</span>
            <span style="background:#fffbeb;color:#f59e0b;padding:2px 10px;border-radius:20px;font-size:12px">{n_medio} medio</span>
            <span style="background:#f0fdf4;color:#10b981;padding:2px 10px;border-radius:20px;font-size:12px">{n_bajo} bajo</span>
          </div>
        </div>
        """)

        st.markdown("#### Análisis por aspecto")
        aspectos_resp = sorted(
            resp.get("aspectos", []),
            key=lambda x: {"ALTO": 0, "MEDIO": 1, "BAJO": 2, "SIN_DATOS": 3}.get(x.get("nivel",""), 3)
        )

        for idx, asp in enumerate(aspectos_resp):
            nivel   = asp.get("nivel", "SIN_DATOS")
            prob    = asp.get("probabilidad", 0)
            col     = COLOR_NIVEL.get(nivel, "#6b7280")
            hist_key = f"hist_{idx}"
            if hist_key not in st.session_state:
                st.session_state[hist_key] = []

            icon = "🔴" if nivel=="ALTO" else "🟡" if nivel=="MEDIO" else "🟢" if nivel=="BAJO" else "⚪"
            with st.expander(
                f"{icon} {asp.get('aspecto','')}  —  {nivel} ({prob}%)",
                expanded=(nivel == "ALTO"),
            ):
                st.markdown(
                    f'<div style="border-left:3px solid {col};padding:10px 14px;'
                    f'background:{col}0d;border-radius:0 6px 6px 0;margin-bottom:10px;'
                    f'font-size:13px;line-height:1.7">{asp.get("razonamiento","")}</div>',
                    unsafe_allow_html=True,
                )
                res_nums = asp.get("resoluciones", [])
                if res_nums:
                    st.caption("Resoluciones: " + " · ".join(res_nums[:5]))

                # ── Botones acción ─────────────────────────────────────────
                b1, b2, b3 = st.columns(3)
                pedir_red = b1.button("✏️ Redacción alternativa", key=f"red_{idx}")
                pedir_ej  = b2.button("📋 Ejemplos conformes",    key=f"ej_{idx}")
                limpiar   = b3.button("🗑️ Limpiar",               key=f"limpiar_{idx}")

                if limpiar:
                    st.session_state[hist_key] = []
                    # No st.rerun() aquí — lo mostramos vacío en este mismo ciclo

                # Llamada API para botones de acción (sin st.rerun al final)
                for modo_btn, triggered in [("redaccion", pedir_red), ("ejemplos", pedir_ej)]:
                    if triggered:
                        payload = {
                            "aspecto":           asp.get("aspecto",""),
                            "nivel":             nivel,
                            "razonamiento":      asp.get("razonamiento",""),
                            "contexto_doctrina": asp.get("contexto_doctrina",""),
                            "historial":         st.session_state[hist_key],
                            "pregunta":          modo_btn,
                            "modo":              modo_btn,
                        }
                        with st.spinner("Consultando TACRC…"):
                            try:
                                r2  = httpx.post(f"{API}/seguimiento_aspecto", json=payload, timeout=90)
                                seg = r2.json()
                                # Guardar ANTES de mostrar, sin rerun
                                st.session_state[hist_key] = seg.get("historial", [])
                            except Exception as e:
                                st.error(f"Error: {e}")

                # ── Historial ──────────────────────────────────────────────
                for msg in st.session_state.get(hist_key, []):
                    role = msg.get("role", "user")
                    txt  = msg.get("content", "")
                    if role == "user":
                        st.markdown(
                            f'<div style="text-align:right;margin:4px 0">'
                            f'<span style="background:#eff6ff;color:#1a4f9e;padding:5px 11px;'
                            f'border-radius:12px 12px 0 12px;font-size:13px;display:inline-block">'
                            f'{txt}</span></div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f'<div style="background:#f9fafb;border:1px solid #e5e7eb;'
                            f'border-radius:0 12px 12px 12px;padding:9px 13px;margin:4px 0;'
                            f'font-size:13px;line-height:1.7">{txt}</div>',
                            unsafe_allow_html=True,
                        )

                # ── Input pregunta libre ───────────────────────────────────
                preg_key = f"preg_input_{idx}"
                preg = st.text_input(
                    "Pregunta sobre este aspecto",
                    placeholder="ej: ¿Qué redacción exige el TACRC para este requisito?",
                    label_visibility="collapsed",
                    key=preg_key,
                )
                if st.button("Enviar →", key=f"enviar_{idx}") and preg.strip():
                    payload = {
                        "aspecto":           asp.get("aspecto",""),
                        "nivel":             nivel,
                        "razonamiento":      asp.get("razonamiento",""),
                        "contexto_doctrina": asp.get("contexto_doctrina",""),
                        "historial":         st.session_state[hist_key],
                        "pregunta":          preg,
                        "modo":              "chat",
                    }
                    with st.spinner("Consultando…"):
                        try:
                            r2  = httpx.post(f"{API}/seguimiento_aspecto", json=payload, timeout=90)
                            seg = r2.json()
                            st.session_state[hist_key] = seg.get("historial", [])
                        except Exception as e:
                            st.error(f"Error: {e}")

        # Fuentes globales
        fuentes = resp.get("fuentes", [])
        if fuentes:
            with st.expander(f"Ver {len(fuentes)} resoluciones consultadas"):
                rows_f = ""
                for f in fuentes:
                    u = url_safe(f.get("pdf_url",""))
                    n = f.get("numero","")
                    enlace = f'<a href="{u}" target="_blank">{n}</a>' if u != "#" else n
                    s = f.get("sentido","") or ""
                    col_s = "#10b981" if "estimaci" in s.lower() else "#f59e0b" if "desestim" in s.lower() else "#6b7280"
                    rows_f += f"""<tr>
                      <td>{enlace}</td>
                      <td style="white-space:nowrap">{f.get('fecha','') or ''}</td>
                      <td style="color:{col_s};font-weight:500">{s}</td>
                      <td>{(f.get('descripcion','') or '')[:160]}</td>
                    </tr>"""
                st.html(tabla_html(rows_f,
                    ["Resolución","Fecha","Sentido","Descripción"], "fuentes-riesgo"))


# ── TAB 4: Dashboard de estado ───────────────────────────────────────────────

with tab_dashboard:
    if not check_login():
        mostrar_login("Estado del sistema")
    else:
      st.markdown("Estado de la ingesta y métricas del sistema.")
      if st.button("🔓 Cerrar sesión", key="logout_dashboard"):
          st.session_state["autenticado"] = False
          st.rerun()
      if st.button("Actualizar", key="refresh_dashboard"):
        st.cache_data.clear()

    stats   = get_stats()
    ingesta = stats.get("ingesta", {})
    dist    = stats.get("distribucion", [])

    # ── Métricas principales ──────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total resoluciones",      ingesta.get("total", 0))
    c2.metric("Con texto PDF",           ingesta.get("con_texto_pdf", 0))
    c3.metric("Con embedding",           ingesta.get("con_embedding", 0))
    c4.metric("Pendientes PDF",          ingesta.get("pendientes_pdf", 0))
    c5.metric("Pendientes embedding",    ingesta.get("pendientes_embedding", 0))

    # ── Barras de progreso ────────────────────────────────────────────────────
    total = ingesta.get("total", 1) or 1
    pct_pdf   = int(ingesta.get("con_texto_pdf", 0)) / total
    pct_embed = int(ingesta.get("con_embedding", 0)) / total

    st.markdown("#### Progreso de ingesta")
    col1, col2 = st.columns(2)
    with col1:
        st.caption(f"Texto PDF — {pct_pdf:.1%}")
        st.progress(pct_pdf)
    with col2:
        st.caption(f"Embeddings — {pct_embed:.1%}")
        st.progress(pct_embed)

    st.divider()

    # ── Distribución por año y sentido ────────────────────────────────────────
    if dist:
        st.markdown("#### Distribución por año y sentido")

        # Agrupar por año
        from collections import defaultdict
        por_anio = defaultdict(dict)
        sentidos_set = set()
        for row in dist:
            anio    = str(row.get("anio", ""))
            sentido = row.get("sentido", "") or "Sin clasificar"
            total_n = row.get("total", 0)
            por_anio[anio][sentido] = total_n
            sentidos_set.add(sentido)

        anios    = sorted(por_anio.keys(), reverse=True)[:10]
        sentidos = sorted(sentidos_set)

        # Tabla HTML con colores semafóricos por sentido
        COLOR_MAP = {
            "Estimación":         "#10b981",
            "Estimación parcial": "#34d399",
            "Desestimación":      "#f59e0b",
            "Desestimación parcial": "#fbbf24",
            "Inadmisión":         "#6b7280",
            "Archivo":            "#9ca3af",
        }

        header_ths = "".join(f"<th>{s}</th>" for s in sentidos)
        rows_html  = ""
        for anio in anios:
            tds = ""
            for s in sentidos:
                n   = por_anio[anio].get(s, 0)
                col = COLOR_MAP.get(s, "#e5e7eb")
                bg  = col + "22"  # 13% opacity
                tds += f'<td style="text-align:center;background:{bg};color:{col};font-weight:500">{n if n else "—"}</td>'
            rows_html += f"<tr><td style='font-weight:500'>{anio}</td>{tds}</tr>"

        st.html(f"""
        <style>
        .dist-table {{border-collapse:collapse;width:100%;font-size:13px}}
        .dist-table th {{background:#1a4f9e;color:white;padding:7px 10px;text-align:center;font-weight:500}}
        .dist-table th:first-child {{text-align:left}}
        .dist-table td {{padding:6px 10px;border-bottom:1px solid #e5e7eb}}
        .dist-table tr:hover td {{background:#f9fafb}}
        </style>
        <table class="dist-table">
        <thead><tr><th>Año</th>{header_ths}</tr></thead>
        <tbody>{rows_html}</tbody>
        </table>
        """)

    st.divider()

    # ── Comandos de gestión ───────────────────────────────────────────────────
    st.markdown("#### Comandos de ingesta")
    st.code("""# Continuar extracción de PDFs pendientes
docker compose run --rm ingest /data/json/resoluciones.json --only-pdf

# Generar embeddings de lo que ya tiene texto
docker compose run --rm ingest /data/json/resoluciones.json --only-embed

# Ingesta completa con más workers (más rápido)
docker compose run --rm ingest /data/json/resoluciones.json --only-pdf --pdf-workers 12

# Ver logs en tiempo real
docker compose logs ingest -f""", language="bash")

    st.markdown("#### Rango de fechas en BD")
    col1, col2 = st.columns(2)
    col1.metric("Resolución más antigua", ingesta.get("fecha_mas_antigua", "—"))
    col2.metric("Resolución más reciente", ingesta.get("fecha_mas_reciente", "—"))
