// Ingesta de Licitaciones (API v1). Listado de activas + fan-out de detalle
// (items + adjudicación) respetando el throttle de ~2 s/req del ticket.
//   node ingest/licitaciones.mjs
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, MP_TICKET_V1
//      (opcional) LIC_MAX_DETALLE=1500  · LIC_REFRESH_DAYS=3
//
// ⚠ Guardamos SIEMPRE `raw` (JSONB). Validar los nombres de campo contra la
//   primera respuesta real (la API v1 usa PascalCase) y ajustar si hace falta.
import { fetchV1, ddmmyyyy } from './lib/mp.mjs';
import { sb, startRun, finishRun, upsertChunked } from './lib/db.mjs';

// Mercado Público usa un mismo ticket para v1 y v2; aceptamos cualquiera de los dos.
const TICKET = process.env.MP_TICKET_V1 || process.env.MP_TICKET_V2 || process.env.MP_TICKET;
if (!TICKET) throw new Error('Falta el ticket (MP_TICKET_V1 / MP_TICKET_V2)');

const MAX_DETALLE  = Number(process.env.LIC_MAX_DETALLE ?? 1500);
const REFRESH_DAYS = Number(process.env.LIC_REFRESH_DAYS ?? 3);
const ADJ_DIAS     = Number(process.env.LIC_ADJ_DIAS || 3);          // días hacia atrás de adjudicadas ('' → 3)
const MAX_ADJ_DET  = Number(process.env.LIC_MAX_ADJ_DETALLE || 900); // presupuesto de detalle para adjudicadas

const now = () => new Date().toISOString();

// Código BIP de la licitación (opcional, solo obras/inversión). El campo
// CodigoBIP trae basura de digitación a veces ("000000", "1"); si no es
// válido, se intenta extraer del nombre/descripción ("...CODIGO BIP 40028006").
function extraerCodigoBip(L) {
  const campo = String(L.CodigoBIP ?? '').trim();
  if (/^\d{7,8}$/.test(campo) && !/^(\d)\1+$/.test(campo)) return campo;
  const texto = `${L.Nombre ?? ''} ${L.Descripcion ?? ''}`;
  const m = texto.match(/BIP\D{0,10}(\d{7,8})/i);
  return m ? m[1] : null;
}

function mapLicitacion(L) {
  return {
    codigo_bip:         extraerCodigoBip(L),
    codigo_externo:     L.CodigoExterno,
    nombre:             L.Nombre ?? null,
    codigo_estado:      L.CodigoEstado ?? null,
    estado:             L.Estado ?? null,
    tipo:               L.Tipo ?? null,
    tipo_convocatoria:  L.TipoConvocatoria ?? null,
    moneda:             L.Moneda ?? null,
    monto_estimado:     L.MontoEstimado ?? null,
    descripcion:        L.Descripcion ?? null,
    fecha_cierre:       L.FechaCierre ?? L.Fechas?.FechaCierre ?? null,
    fecha_publicacion:  L.Fechas?.FechaPublicacion ?? null,
    fecha_adjudicacion: L.Fechas?.FechaAdjudicacion ?? null,
    organismo_codigo:   L.Comprador?.CodigoOrganismo ?? null,
    organismo_nombre:   L.Comprador?.NombreOrganismo ?? null,
    unidad_rut:         L.Comprador?.RutUnidad ?? null,
    unidad_nombre:      L.Comprador?.NombreUnidad ?? null,
    region_unidad:      L.Comprador?.RegionUnidad ?? null,
    comuna_unidad:      L.Comprador?.ComunaUnidad ?? null,
    adjudicacion:       L.Adjudicacion ?? null,
    fechas:             L.Fechas ?? null,
    raw:                L,
    last_seen:          now(),
    last_detail_at:     now(),
  };
}

function mapItems(codigo, L) {
  return (L.Items?.Listado ?? []).map((it) => ({
    licitacion_codigo:    codigo,
    correlativo:          it.Correlativo,
    codigo_producto:      it.CodigoProducto ?? null,
    codigo_categoria:     it.CodigoCategoria ?? null,
    categoria:            it.Categoria ?? null,
    nombre_producto:      it.NombreProducto ?? null,
    descripcion:          it.Descripcion ?? null,
    unidad_medida:        it.UnidadMedida ?? null,
    cantidad:             it.Cantidad ?? null,
    adj_rut_proveedor:    it.Adjudicacion?.RutProveedor ?? null,
    adj_nombre_proveedor: it.Adjudicacion?.NombreProveedor ?? null,
    adj_cantidad:         it.Adjudicacion?.CantidadAdjudicada ?? null,
    adj_monto_unitario:   it.Adjudicacion?.MontoUnitario ?? null,
  })).filter((r) => r.correlativo != null);
}

async function loadExisting(codigos) {
  const map = new Map();
  for (let i = 0; i < codigos.length; i += 300) {
    const { data, error } = await sb
      .from('licitaciones')
      .select('codigo_externo, codigo_estado, last_detail_at')
      .in('codigo_externo', codigos.slice(i, i + 300));
    if (error) throw error;
    for (const r of data) map.set(r.codigo_externo, r);
  }
  return map;
}

async function main() {
  const runId = await startRun('licitaciones');
  let requests = 0, upserts = 0;

  try {
    // ── 1) Listado de activas (liviano: CodigoExterno, Nombre, CodigoEstado, FechaCierre) ──
    const listJson = await fetchV1('licitaciones.json', { estado: 'activas' }, TICKET);
    requests++;
    const activas = listJson?.Listado ?? [];
    console.log(`[licitaciones] activas=${activas.length}`);

    const codigos = activas.map((l) => l.CodigoExterno).filter(Boolean);
    const existing = await loadExisting(codigos);

    // Decide qué necesita detalle (nuevas / cambió estado / detalle viejo).
    const cutoff = Date.now() - REFRESH_DAYS * 864e5;
    const necesitan = activas.filter((l) => {
      const ex = existing.get(l.CodigoExterno);
      if (!ex) return true;
      if (ex.codigo_estado !== l.CodigoEstado) return true;
      if (!ex.last_detail_at) return true;
      return new Date(ex.last_detail_at).getTime() < cutoff;
    });

    // Marca a TODAS las activas como vistas (header liviano), sin pisar otras columnas.
    if (activas.length) {
      await upsertChunked('licitaciones',
        activas.map((l) => ({ codigo_externo: l.CodigoExterno, codigo_estado: l.CodigoEstado, last_seen: now() })),
        'codigo_externo');
    }

    // ── 2) Fan-out de detalle (throttle 2 s en fetchV1) ──
    const objetivo = necesitan.slice(0, MAX_DETALLE);
    const pendientes = necesitan.length - objetivo.length;
    console.log(`[licitaciones] detalle: ${objetivo.length} ahora · ${pendientes} quedan para la próxima corrida`);

    for (const [i, l] of objetivo.entries()) {
      try {
        const det = await fetchV1('licitaciones.json', { codigo: l.CodigoExterno }, TICKET);
        requests++;
        const L = det?.Listado?.[0];
        if (!L) continue;

        await upsertChunked('licitaciones', [mapLicitacion(L)], 'codigo_externo');
        const items = mapItems(l.CodigoExterno, L);
        if (items.length) await upsertChunked('licitacion_items', items, 'licitacion_codigo,correlativo');
        upserts += 1 + items.length;
      } catch (e) {
        console.warn(`[licitaciones] ${l.CodigoExterno} error: ${e.message}`);
      }
      if ((i + 1) % 100 === 0) console.log(`[licitaciones] ${i + 1}/${objetivo.length}`);
    }

    // ── 3) RESULTADOS: adjudicadas por fecha (el listado de activas nunca las
    //    trae de vuelta — sin este paso las adjudicaciones quedaban en null).
    //    licitaciones.json?fecha=ddmmyyyy&estado=adjudicada → detalle con
    //    Items.Adjudicacion por línea. ~300/día a nivel nacional.
    let adjDetalles = 0;
    const adjCodigos = new Map(); // codigo -> CodigoEstado del listado
    for (let d = 1; d <= ADJ_DIAS; d++) {
      const fecha = new Date(Date.now() - d * 864e5);
      try {
        const j = await fetchV1('licitaciones.json', { fecha: ddmmyyyy(fecha), estado: 'adjudicada' }, TICKET);
        requests++;
        for (const l of (j?.Listado ?? [])) {
          if (l.CodigoExterno) adjCodigos.set(l.CodigoExterno, l.CodigoEstado ?? 8);
        }
      } catch (e) {
        console.warn(`[licitaciones] adjudicadas ${ddmmyyyy(fecha)} error: ${e.message}`);
      }
    }

    const adjExisting = await loadExisting([...adjCodigos.keys()]);
    const adjPend = [...adjCodigos.entries()].filter(([codigo, estado]) => {
      const ex = adjExisting.get(codigo);
      // ya la detallamos DESPUÉS de adjudicada → no repetir
      return !(ex && ex.codigo_estado === estado && ex.last_detail_at);
    }).slice(0, MAX_ADJ_DET);
    console.log(`[licitaciones] adjudicadas: ${adjCodigos.size} en ${ADJ_DIAS} días · detalle para ${adjPend.length}`);

    for (const [i, [codigo]] of adjPend.entries()) {
      try {
        const det = await fetchV1('licitaciones.json', { codigo }, TICKET);
        requests++;
        const L = det?.Listado?.[0];
        if (!L) continue;
        await upsertChunked('licitaciones', [mapLicitacion(L)], 'codigo_externo');
        const items = mapItems(codigo, L);
        if (items.length) await upsertChunked('licitacion_items', items, 'licitacion_codigo,correlativo');
        upserts += 1 + items.length;
        adjDetalles++;
      } catch (e) {
        console.warn(`[licitaciones] adj ${codigo} error: ${e.message}`);
      }
      if ((i + 1) % 100 === 0) console.log(`[licitaciones] adj ${i + 1}/${adjPend.length}`);
    }

    await finishRun(runId, {
      status: pendientes > 0 ? 'partial' : 'success',
      rows_upserted: upserts,
      requests_made: requests,
      cursor: { activas: activas.length, pendientes, adjudicadas: adjDetalles },
    });
    console.log(`[licitaciones] OK · upserts=${upserts} requests=${requests} pendientes=${pendientes} adjudicadas=${adjDetalles}`);
  } catch (err) {
    await finishRun(runId, { status: 'failed', rows_upserted: upserts, requests_made: requests, error_message: err.message });
    console.error(`[licitaciones] ERROR: ${err.message}`);
    process.exitCode = 1;
  }
}

main();
