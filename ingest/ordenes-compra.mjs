// Ingesta de Órdenes de Compra (API v1) — solo HEADER por fecha (endpoint
// correcto: `ordenesdecompra.json?fecha=ddmmyyyy`; NO `OrdenCompra.json`, que da 404).
// Alimenta el directorio de proveedores (proveedor_rut + total + fecha_envio).
//
//   node ingest/ordenes-compra.mjs
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, MP_TICKET_V1
//   Modo diario (default): trae los últimos OC_DIAS (def. 3) días.
//   Modo backfill: OC_BACKFILL_DESDE=2026-01-01 [OC_BACKFILL_HASTA=2026-07-01]
//      ⚠ El backfill de 2026 son ~1,5-2M filas → correr recién en plan Pro.
//
// ⚠ CONFIRMADO (jul 2026): el listado por fecha es LIVIANO — solo trae
//   {Codigo, Nombre, CodigoEstado}. Proveedor/montos/fechas van únicamente en el
//   detalle por código. Este script registra la existencia diaria de cada OC;
//   el detalle se completa por otra vía (copia desde el Supabase externo del
//   usuario y/o fan-out selectivo). No se guarda `raw` (a ~2M filas duplica
//   el Nombre sin aportar).
import { fetchV1, ddmmyyyy } from './lib/mp.mjs';
import { startRun, finishRun, upsertChunked } from './lib/db.mjs';

const TICKET = process.env.MP_TICKET_V1 || process.env.MP_TICKET_V2 || process.env.MP_TICKET;
if (!TICKET) throw new Error('Falta el ticket (MP_TICKET_V1 / MP_TICKET_V2)');

const now = () => new Date().toISOString();

// Solo las columnas que el listado liviano REALMENTE trae. No incluir el resto:
// un upsert con nulls pisaría datos enriquecidos por otras vías (copia del
// externo / fan-out de detalle) cuando la OC reaparece en el re-pull de 3 días.
function mapOCLight(o) {
  return {
    codigo:        o.Codigo,
    nombre:        o.Nombre ?? null,
    codigo_estado: o.CodigoEstado ?? null,
    last_seen:     now(),
  };
}

function dateRange(desdeStr, hastaStr) {
  const out = [];
  const d   = new Date(desdeStr + 'T12:00:00Z');
  const end = new Date(hastaStr + 'T12:00:00Z');
  while (d <= end) { out.push(new Date(d)); d.setUTCDate(d.getUTCDate() + 1); }
  return out;
}

async function main() {
  const runId = await startRun('ordenes_compra');
  let requests = 0, upserts = 0;

  try {
    // Rango de fechas a consultar.
    let fechas;
    if (process.env.OC_BACKFILL_DESDE) {
      const hasta = process.env.OC_BACKFILL_HASTA ?? new Date().toISOString().slice(0, 10);
      fechas = dateRange(process.env.OC_BACKFILL_DESDE, hasta);
      console.log(`[oc] BACKFILL ${process.env.OC_BACKFILL_DESDE} → ${hasta} (${fechas.length} días)`);
    } else {
      // Parte desde AYER: el día en curso está incompleto y suele estresar al servidor.
      const dias = Number(process.env.OC_DIAS ?? 3);
      const hoy = new Date();
      fechas = Array.from({ length: dias }, (_, i) => {
        const d = new Date(hoy); d.setUTCDate(hoy.getUTCDate() - (i + 1)); return d;
      });
      console.log(`[oc] modo diario · últimos ${dias} días (desde ayer)`);
    }

    // Un día que falla no mata la corrida: se registra y se sigue (status partial).
    const fallidas = [];
    for (const fecha of fechas) {
      const f = ddmmyyyy(fecha);
      try {
        const json = await fetchV1('ordenesdecompra.json', { fecha: f }, TICKET, { retries: 5 });
        requests++;
        const items = json?.Listado ?? [];
        if (items.length) {
          const rows = items.filter((o) => o.Codigo).map(mapOCLight);
          upserts += await upsertChunked('ordenes_compra', rows, 'codigo');
        }
        console.log(`[oc] fecha=${f} · items=${items.length}`);
      } catch (e) {
        requests++;
        fallidas.push(f);
        console.warn(`[oc] fecha=${f} FALLÓ tras reintentos: ${e.message}`);
      }
    }

    await finishRun(runId, {
      status: fallidas.length ? 'partial' : 'success',
      rows_upserted: upserts,
      requests_made: requests,
      cursor: fallidas.length ? { fechas_fallidas: fallidas } : null,
    });
    console.log(`[oc] ${fallidas.length ? `PARTIAL (fallaron: ${fallidas.join(',')})` : 'OK'} · upserts=${upserts} requests=${requests}`);
  } catch (err) {
    await finishRun(runId, { status: 'failed', rows_upserted: upserts, requests_made: requests, error_message: err.message });
    console.error(`[oc] ERROR: ${err.message}`);
    process.exitCode = 1;
  }
}

main();
