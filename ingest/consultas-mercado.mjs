// Ingesta de Consultas al Mercado (RFI) — API pública de consulta-mercado.
//   node ingest/consultas-mercado.mjs
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
//      (opcional) CM_DIAS=45  ventana rolling hacia atrás
//
// API (descubierta desde el bundle de la SPA, jul 2026 — sin ticket ni captcha):
//   1) GET https://servicios-prd.mercadopublico.cl/v1/auth/publico
//      → { payload: { access_token } }  (JWT público, ~8 h)
//   2) GET https://servicios-consultas-prd.mercadopublico.cl/v1/consulta-mercado
//        ?desde=YYYY-MM-DD HH:mm:ss & hasta=... & estado=100|200
//        & ordenarPor=100 & pagina=N
//      → { payload: { resultCount, pageCount, page, resultados: [...] } }
//   Estados: 100 = Publicada · 200 = Cerrada. pageSize fijo (15).
//   Fechas SIN zona horaria (hora local Chile).
import { sb, startRun, finishRun, upsertChunked } from './lib/db.mjs';

const AUTH_URL = 'https://servicios-prd.mercadopublico.cl/v1/auth/publico';
const API_BASE = 'https://servicios-consultas-prd.mercadopublico.cl/v1/consulta-mercado';
const DIAS = Number(process.env.CM_DIAS ?? 45);
const ESTADO_TEXTO = { 100: 'Publicada', 200: 'Cerrada' };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function fmt(d) {
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

async function getToken() {
  const res = await fetch(AUTH_URL, { headers: { Accept: 'application/json' } });
  const json = await res.json();
  const token = json?.payload?.access_token;
  if (!token) throw new Error('No se obtuvo access_token público');
  return token;
}

async function listar(token, estado, desde, hasta, pagina) {
  const qs = new URLSearchParams({ desde, hasta, estado: String(estado), ordenarPor: '100', pagina: String(pagina) });
  for (let attempt = 1; ; attempt++) {
    const res = await fetch(`${API_BASE}?${qs}`, {
      headers: { Accept: 'application/json', Authorization: `Bearer ${token}` },
    });
    const json = await res.json().catch(() => null);
    if (json?.success === 'OK') return json.payload;
    if (attempt >= 3) throw new Error(json?.errores?.[0]?.descripcion ?? `HTTP ${res.status}`);
    await sleep(attempt * 2000);
  }
}

function mapRfi(r) {
  return {
    codigo:            r.codigoConsulta,
    id_api:            r.id ?? null,
    nombre:            r.nombre ?? null,
    descripcion:       r.descripcion ?? null,
    motivo:            r.motivo ?? null,
    estado:            r.estado ?? null,
    estado_texto:      ESTADO_TEXTO[r.estado] ?? null,
    fecha_publicacion: r.fechaPublicacion ?? null,   // hora local Chile, sin TZ
    fecha_cierre:      r.fechaCierre ?? null,
    organismo_codigo:  r.codigoInstitucion ?? null,
    organismo_nombre:  r.nombreInstitucion ?? null,
    organismo_rut:     r.rutOrganismo ?? null,
    unidad_codigo:     r.codigoUnidadCompra ?? null,
    unidad_nombre:     r.nombreUnidadCompra ?? null,
    tipo_organismo:    r.tipoOrganismo ?? null,
    raw:               r,
    last_seen:         new Date().toISOString(),
  };
}

async function main() {
  const runId = await startRun('consultas_mercado');
  let upserts = 0, requests = 0;

  try {
    const token = await getToken();
    requests++;

    const hasta = fmt(new Date());
    const desde = fmt(new Date(Date.now() - DIAS * 864e5));
    console.log(`[rfi] ventana ${desde} → ${hasta}`);

    for (const estado of [100, 200]) {
      let pagina = 1, pageCount = 1;
      do {
        const p = await listar(token, estado, desde, hasta, pagina);
        requests++;
        pageCount = p?.pageCount ?? 1;
        const rows = (p?.resultados ?? []).filter((r) => r.codigoConsulta).map(mapRfi);
        if (rows.length) upserts += await upsertChunked('consultas_mercado', rows, 'codigo');
        console.log(`[rfi] estado=${estado} pág ${pagina}/${pageCount} · ${rows.length} filas`);
        pagina++;
        await sleep(300);
      } while (pagina <= pageCount);
    }

    await finishRun(runId, { status: 'success', rows_upserted: upserts, requests_made: requests });
    console.log(`[rfi] OK · upserts=${upserts} requests=${requests}`);
  } catch (err) {
    await finishRun(runId, { status: 'failed', rows_upserted: upserts, requests_made: requests, error_message: err.message });
    console.error(`[rfi] ERROR: ${err.message}`);
    process.exitCode = 1;
  }
}

main();
