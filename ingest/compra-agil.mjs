// Ingesta de Compra Ágil (API v2, incremental).
//   node ingest/compra-agil.mjs
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, MP_TICKET_V2
//      (opcional) CA_BACKFILL_DESDE=2026-05-01T00:00:00Z  · CA_MAX_DETALLE=400
//
// ⚠ El mapeo de campos es un primer cut: guardamos SIEMPRE `raw` (JSONB) para no
//   perder nada. Validar los nombres contra la primera respuesta real y ajustar.
import { fetchV2, QuotaExceededError, sleep } from './lib/mp.mjs';
import { sb, startRun, finishRun, lastOkCursor, upsertChunked } from './lib/db.mjs';

// Mercado Público usa un mismo ticket para v1 y v2; aceptamos cualquiera de los dos.
const TICKET = process.env.MP_TICKET_V2 || process.env.MP_TICKET_V1 || process.env.MP_TICKET;
if (!TICKET) throw new Error('Falta el ticket (MP_TICKET_V2 / MP_TICKET_V1)');

const MAX_DETALLE = Number(process.env.CA_MAX_DETALLE ?? 400);
const ESTADOS_CON_RESULTADO = new Set(['cerrada', 'proveedor_seleccionado', 'desierta', 'cancelada']);

const now = () => new Date().toISOString();

function mapHeader(it) {
  return {
    codigo:               it.codigo,
    nombre:               it.nombre ?? null,
    estado_id:            it.estado?.id_estado ?? null,
    estado_codigo:        it.estado?.codigo ?? null,
    estado_glosa:         it.estado?.glosa ?? null,
    estado_convocatoria:  it.convocatoria?.estado_convocatoria ?? null,
    fecha_publicacion:    it.fechas?.fecha_publicacion ?? null,
    fecha_cierre:         it.fechas?.fecha_cierre ?? null,
    fecha_ultimo_cambio:  it.fechas?.fecha_ultimo_cambio ?? null,
    fecha_cancelacion:    it.fechas?.fecha_cancelacion ?? null,
    monto_disponible_clp: it.montos?.monto_disponible_clp ?? null,
    institucion_rut:      it.institucion?.rut ?? null,
    organismo_comprador:  it.institucion?.organismo_comprador ?? null,
    unidad_compra:        it.institucion?.unidad_compra ?? null,
    region:               it.institucion?.region ?? null,
    nombre_region:        it.institucion?.nombre_region ?? null,
    total_ofertas:        it.resumen?.total_ofertas_recibidas ?? null,
    motivo_cancelacion:   it.motivos?.motivo_cancelacion ?? null,
    motivo_desierta:      it.motivos?.motivo_desierta ?? null,
    motivo_seleccion:     it.motivos?.motivo_seleccion ?? null,
    // raw eliminado (limpieza jul 2026): mapeo validado, columna dropeada
    last_seen:            now(),
  };
}

function mapCotizacion(caCodigo, c) {
  return {
    compra_agil_codigo: caCodigo,
    id_cotizacion:      c.id_cotizacion,
    rut_proveedor:      c.rut_proveedor ?? null,
    razon_social:       c.razon_social ?? null,
    es_emt:             c.es_emt ?? null,
    valor_neto:         c.valor_neto ?? null,
    total_impuesto:     c.total_impuesto ?? null,
    monto_total:        c.monto_total ?? null,
    seleccionado:       c.seleccion?.proveedor_seleccionado ?? null,
    fecha_creacion:     c.fecha_creacion ?? null,
    // raw eliminado (limpieza jul 2026)
  };
}

async function main() {
  const runId = await startRun('compra_agil');
  let requests = 0, upserts = 0, quota429 = 0;
  let maxCambio = null;

  try {
    // Ventana incremental: desde el último cambio visto (con solapamiento de 10 min).
    const cursor = await lastOkCursor('compra_agil');
    const desde = cursor?.last_cambio
      ? new Date(new Date(cursor.last_cambio).getTime() - 10 * 60_000).toISOString()
      : (process.env.CA_BACKFILL_DESDE ?? new Date(Date.now() - 30 * 864e5).toISOString());

    console.log(`[compra_agil] cambio_desde=${desde}`);

    // ── 1) Listado paginado (headers) ──
    let page = 1, totalPages = 1;
    do {
      const json = await fetchV2('/v2/compra-agil', {
        cambio_desde: desde,
        tamano_pagina: 50,
        numero_pagina: page,
        ordenar_por: 'FechaUltimaModificacion',
      }, TICKET);
      requests++;

      const items = json?.payload?.items ?? [];
      totalPages = json?.payload?.paginacion?.total_paginas ?? page;

      if (items.length) {
        upserts += await upsertChunked('compra_agil', items.map(mapHeader), 'codigo');
        for (const it of items) {
          if (it.fechas?.fecha_ultimo_cambio && (!maxCambio || it.fechas.fecha_ultimo_cambio > maxCambio)) {
            maxCambio = it.fechas.fecha_ultimo_cambio;
          }
        }
      }
      console.log(`[compra_agil] pág ${page}/${totalPages} · items=${items.length}`);
      page++;
    } while (page <= totalPages);

    // ── 2) Detalle (cotizaciones/resultados): procesos en estado terminal que aún
    //    no tienen detalle. Se resuelve por estado en la BD (no por la ventana
    //    incremental), así el backfill drena en varias corridas y capta también los
    //    que recién cierran. Cada uno se marca con last_detail_at para no repetir.
    const { data: pend } = await sb
      .from('compra_agil')
      .select('codigo')
      .in('estado_codigo', [...ESTADOS_CON_RESULTADO])
      .is('last_detail_at', null)
      .order('fecha_ultimo_cambio', { ascending: false })
      .limit(MAX_DETALLE);
    const objetivo = (pend ?? []).map((r) => r.codigo);
    console.log(`[compra_agil] detalle para ${objetivo.length} procesos (terminales sin detalle)`);
    for (const codigo of objetivo) {
      const json = await fetchV2(`/v2/compra-agil/${encodeURIComponent(codigo)}`, null, TICKET);
      requests++;
      const p = json?.payload;
      if (!p) continue;

      const cots = (p.proveedores_cotizando ?? [])
        .filter((c) => c.id_cotizacion != null)
        .map((c) => mapCotizacion(codigo, c));
      if (cots.length) upserts += await upsertChunked('compra_agil_cotizaciones', cots, 'compra_agil_codigo,id_cotizacion');

      await sb.from('compra_agil').update({
        orden_compra_id: p.orden_compra?.id_orden_compra ?? null,
        oc_codigo:       p.orden_compra?.codigo_orden_compra ?? null,
        last_detail_at:  now(),
      }).eq('codigo', codigo);

      await sleep(150); // cortesía con la API
    }

    await finishRun(runId, {
      status: 'success',
      rows_upserted: upserts,
      requests_made: requests,
      cursor: { last_cambio: maxCambio ?? cursor?.last_cambio ?? desde },
    });
    console.log(`[compra_agil] OK · upserts=${upserts} requests=${requests}`);
  } catch (err) {
    const isQuota = err instanceof QuotaExceededError;
    if (isQuota) quota429 = 1;
    await finishRun(runId, {
      status: isQuota ? 'partial' : 'failed',
      rows_upserted: upserts,
      requests_made: requests,
      http_429_count: quota429,
      cursor: maxCambio ? { last_cambio: maxCambio } : null,
      error_message: err.message,
    });
    console.error(`[compra_agil] ${isQuota ? 'CUOTA AGOTADA' : 'ERROR'}: ${err.message}`);
    process.exitCode = isQuota ? 0 : 1; // 429 no es fallo: retomamos mañana
  }
}

main();
