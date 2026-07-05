// Ingesta de OC desde DATOS ABIERTOS de ChileCompra (cobertura nacional).
// Descarga el ZIP mensual, agrega por (proveedor_rut, mes) con desglose por
// modalidad (convenio marco / trato directo / compra ágil / licitación) y
// reemplaza ese mes en oc_proveedor_mensual.
//
//   node ingest/oc-datos-abiertos.mjs
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
//   OC_DA_MESES=2026-1,2026-2   meses a procesar (def: mes actual; y si estamos
//                               en los primeros 5 días, también el anterior)
//   OC_DA_LOCAL_CSV=path        usar un CSV ya descargado (debug/test)
//   OC_DA_DRY=1                 no escribe en BD, solo muestra stats
//
// Fuente: https://transparenciachc.blob.core.windows.net/oc-da/{YYYY-M}.zip
// CSV: ';' + comillas, Windows-1252, nivel ÍTEM (el header de la OC se repite
// por cada ítem → se deduplica por Codigo). Montos ya normalizados a CLP con
// coma decimal. Campos con saltos de línea embebidos → csv-parse (streaming).
import { createReadStream, mkdtempSync, createWriteStream } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { execFileSync } from 'node:child_process';
import { parse } from 'csv-parse';
import { sb, startRun, finishRun } from './lib/db.mjs';

const BASE = 'https://transparenciachc.blob.core.windows.net/oc-da';
const DRY = process.env.OC_DA_DRY === '1';

function mesesObjetivo() {
  if (process.env.OC_DA_MESES) {
    return process.env.OC_DA_MESES.split(',').map((s) => s.trim()).filter(Boolean);
  }
  const hoy = new Date();
  const out = [`${hoy.getUTCFullYear()}-${hoy.getUTCMonth() + 1}`];
  if (hoy.getUTCDate() <= 5) {
    const prev = new Date(Date.UTC(hoy.getUTCFullYear(), hoy.getUTCMonth() - 1, 1));
    out.push(`${prev.getUTCFullYear()}-${prev.getUTCMonth() + 1}`);
  }
  return out;
}

const parseMonto = (s) => {
  if (!s || s === 'NA') return 0;
  const n = parseFloat(String(s).replace(/\./g, '').replace(',', '.'));
  // Los montos vienen sin separador de miles; el replace de puntos cubre el
  // caso defensivo. Si el valor era "1234,56" queda 1234.56.
  return Number.isFinite(n) ? n : 0;
};

// Modalidad de una OC según los campos del dataset.
function modalidad(rec) {
  const cm = rec.Codigo_ConvenioMarco;
  if (cm && cm !== 'NA' && cm !== '') return 'cm';
  if (rec.EsTratoDirecto === 'Si') return 'td';
  if (rec.EsCompraAgil === 'Si') return 'ca';
  if (rec.CodigoLicitacion && rec.CodigoLicitacion !== '') return 'lic';
  return 'otros';
}

async function descargarMes(mes, dir) {
  const url = `${BASE}/${mes}.zip`;
  console.log(`[oc-da] descargando ${url}`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Descarga ${mes}: HTTP ${res.status}`);
  const zipPath = join(dir, `${mes}.zip`);
  await pipeline(Readable.fromWeb(res.body), createWriteStream(zipPath));
  execFileSync('unzip', ['-o', '-q', zipPath, '-d', dir]);
  return join(dir, `${mes}.csv`);
}

async function agregarMes(csvPath) {
  // (rut) -> acumulador; dedup de OC por Codigo (primera aparición manda)
  const provs = new Map();
  const vistas = new Set();
  let filas = 0;

  const parser = createReadStream(csvPath, { encoding: 'latin1' }).pipe(parse({
    delimiter: ';',
    columns: true,
    bom: true,
    relax_quotes: true,
    relax_column_count: true,
    skip_records_with_error: true,
  }));

  for await (const rec of parser) {
    filas++;
    const codigo = rec.Codigo;
    const rut = rec.RutSucursal;
    if (!codigo || !rut || vistas.has(codigo)) continue;
    vistas.add(codigo);

    // Excluir OC canceladas del agregado (no son negocio adjudicado)
    const estado = rec.Estado ?? '';
    if (/cancelad/i.test(estado)) continue;

    let p = provs.get(rut);
    if (!p) {
      p = { nombre: rec.NombreProveedor ?? null, actividad: rec.ActividadProveedor || null,
            region: rec.RegionProveedor || null,
            contratos: 0, monto: 0,
            cm: [0, 0], td: [0, 0], ca: [0, 0], lic: [0, 0] };
      provs.set(rut, p);
    }
    const monto = parseMonto(rec.MontoTotalOC_PesosChilenos);
    p.contratos++; p.monto += monto;
    const m = modalidad(rec);
    if (p[m]) { p[m][0]++; p[m][1] += monto; }
  }
  return { provs, filas, ocs: vistas.size };
}

async function guardarMes(mes, provs) {
  const [y, m] = mes.split('-').map(Number);
  const mesDate = `${y}-${String(m).padStart(2, '0')}-01`;
  const rows = [...provs.entries()].map(([rut, p]) => ({
    rut, mes: mesDate,
    nombre: p.nombre, actividad: p.actividad, region: p.region,
    contratos: p.contratos, monto_clp: p.monto,
    contratos_cm: p.cm[0],  monto_cm: p.cm[1],
    contratos_td: p.td[0],  monto_td: p.td[1],
    contratos_ca: p.ca[0],  monto_ca: p.ca[1],
    contratos_lic: p.lic[0], monto_lic: p.lic[1],
    actualizado_at: new Date().toISOString(),
  }));

  if (DRY) {
    console.log(`[oc-da] DRY ${mes}: ${rows.length} proveedores (no se escribe)`);
    const top = rows.sort((a, b) => b.monto_clp - a.monto_clp).slice(0, 5);
    for (const t of top) console.log(`   ${t.rut} ${t.nombre?.slice(0, 40)} · ${t.contratos} OC · $${Math.round(t.monto_clp).toLocaleString('es-CL')}`);
    return rows.length;
  }

  // El archivo mensual se regenera completo → reemplazo limpio del mes.
  const { error: delErr } = await sb.from('oc_proveedor_mensual').delete().eq('mes', mesDate);
  if (delErr) throw delErr;
  for (let i = 0; i < rows.length; i += 500) {
    const { error } = await sb.from('oc_proveedor_mensual').insert(rows.slice(i, i + 500));
    if (error) throw error;
  }
  return rows.length;
}

async function main() {
  const meses = mesesObjetivo();
  console.log(`[oc-da] meses: ${meses.join(', ')}${DRY ? ' (DRY RUN)' : ''}`);
  const runId = DRY ? null : await startRun('oc_datos_abiertos');
  let totalRows = 0, requests = 0;

  try {
    const dir = mkdtempSync(join(tmpdir(), 'oc-da-'));
    const omitidos = [];
    for (const mes of meses) {
      let csvPath;
      try {
        csvPath = process.env.OC_DA_LOCAL_CSV ?? await descargarMes(mes, dir);
        if (!process.env.OC_DA_LOCAL_CSV) requests++;
      } catch (e) {
        // A inicios de mes el ZIP del mes en curso puede no existir todavía (404):
        // no es un fallo del pipeline, se omite y se intenta mañana.
        if (/HTTP 404/.test(e.message)) {
          console.warn(`[oc-da] ${mes}: archivo aún no publicado (404), se omite`);
          omitidos.push(mes);
          continue;
        }
        throw e;
      }
      const { provs, filas, ocs } = await agregarMes(csvPath);
      console.log(`[oc-da] ${mes}: ${filas.toLocaleString()} filas ítem · ${ocs.toLocaleString()} OC únicas · ${provs.size.toLocaleString()} proveedores`);
      totalRows += await guardarMes(mes, provs);
    }
    if (runId) await finishRun(runId, { status: 'success', rows_upserted: totalRows, requests_made: requests, cursor: { meses, omitidos } });
    console.log(`[oc-da] OK · ${totalRows} filas agregadas${omitidos.length ? ` · omitidos: ${omitidos.join(',')}` : ''}`);
  } catch (err) {
    if (runId) await finishRun(runId, { status: 'failed', rows_upserted: totalRows, requests_made: requests, error_message: err.message });
    console.error(`[oc-da] ERROR: ${err.message}`);
    process.exitCode = 1;
  }
}

main();
