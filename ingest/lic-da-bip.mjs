// Extrae el mapeo licitación→BIP desde los CSVs históricos de licitaciones
// (datos abiertos lic-da). El dataset NO trae columna BIP, pero ~1.000
// menciones/mes van en el nombre/descripción ("...CODIGO BIP 40028006").
//   node ingest/lic-da-bip.mjs
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
//   (opcional) LIC_DA_MESES=2026-1,2026-2   meses (def: mes anterior)
//   (opcional) LIC_DA_LOCAL_CSV=path · LIC_DA_DRY=1
//
// Fuente: https://transparenciachc.blob.core.windows.net/lic-da/{YYYY-M}.zip
// CSV ~240MB/mes (nivel ítem), ';', Windows-1252, filas repetidas por ítem
// → dedup por CodigoExterno. Solo se guardan las que mencionan BIP.
import { createReadStream, createWriteStream, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { execFileSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import { parse } from 'csv-parse';
import { startRun, finishRun, upsertChunked } from './lib/db.mjs';

const BASE = 'https://transparenciachc.blob.core.windows.net/lic-da';
const DRY = process.env.LIC_DA_DRY === '1';

function mesesObjetivo() {
  if (process.env.LIC_DA_MESES) {
    return process.env.LIC_DA_MESES.split(',').map((s) => s.trim()).filter(Boolean);
  }
  const prev = new Date();
  prev.setUTCMonth(prev.getUTCMonth() - 1);
  return [`${prev.getUTCFullYear()}-${prev.getUTCMonth() + 1}`];
}

function extraerBip(texto) {
  const m = String(texto ?? '').match(/BIP\D{0,10}(\d{7,8})/i);
  if (!m) return null;
  if (/^(\d)\1+$/.test(m[1])) return null;   // basura tipo 1111111
  return m[1];
}

async function descargarMes(mes, dir) {
  const url = `${BASE}/${mes}.zip`;
  console.log(`[lic-da-bip] descargando ${url}`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Descarga ${mes}: HTTP ${res.status}`);
  const zipPath = join(dir, `${mes}.zip`);
  await pipeline(Readable.fromWeb(res.body), createWriteStream(zipPath));
  execFileSync('unzip', ['-o', '-q', zipPath, '-d', dir]);
  // El nombre interno varía (lic_2026-5.csv); tomar el csv recién extraído.
  const csv = readdirSync(dir).find((f) => f.endsWith('.csv') && f.includes(mes.replace('-', '-')));
  if (!csv) throw new Error(`No se encontró CSV en el zip de ${mes}`);
  return join(dir, csv);
}

async function procesarMes(mes, csvPath) {
  const [y, m] = mes.split('-').map(Number);
  const mesDate = `${y}-${String(m).padStart(2, '0')}-01`;
  const porLicitacion = new Map();
  let filas = 0;

  const parser = createReadStream(csvPath, { encoding: 'latin1' }).pipe(parse({
    delimiter: ';', columns: true, bom: true,
    relax_quotes: true, relax_column_count: true, skip_records_with_error: true,
  }));

  for await (const r of parser) {
    filas++;
    const codigo = r.CodigoExterno;
    if (!codigo || porLicitacion.has(codigo)) continue;
    const bip = extraerBip(`${r.Nombre ?? ''} ${r.Descripcion ?? ''}`);
    if (!bip) continue;
    porLicitacion.set(codigo, {
      codigo_licitacion: codigo,
      codigo_bip: bip,
      nombre: r.Nombre ?? null,
      fuente: 'texto',
      mes: mesDate,
    });
  }

  const rows = [...porLicitacion.values()];
  console.log(`[lic-da-bip] ${mes}: ${filas.toLocaleString()} filas ítem · ${rows.length} licitaciones con BIP`);
  if (DRY) {
    for (const r of rows.slice(0, 5)) console.log(`   ${r.codigo_licitacion} → BIP ${r.codigo_bip} · ${r.nombre?.slice(0, 55)}`);
    return 0;
  }
  return upsertChunked('licitacion_bip', rows, 'codigo_licitacion');
}

async function main() {
  const meses = mesesObjetivo();
  console.log(`[lic-da-bip] meses: ${meses.join(', ')}${DRY ? ' (DRY RUN)' : ''}`);
  const runId = DRY ? null : await startRun('lic_da_bip');
  let upserts = 0, requests = 0;

  try {
    const dir = mkdtempSync(join(tmpdir(), 'lic-da-'));
    for (const mes of meses) {
      let csvPath;
      try {
        csvPath = process.env.LIC_DA_LOCAL_CSV ?? await descargarMes(mes, dir);
        if (!process.env.LIC_DA_LOCAL_CSV) requests++;
      } catch (e) {
        if (/HTTP 404/.test(e.message)) { console.warn(`[lic-da-bip] ${mes}: aún no publicado (404), se omite`); continue; }
        throw e;
      }
      upserts += await procesarMes(mes, csvPath);
    }
    if (runId) await finishRun(runId, { status: 'success', rows_upserted: upserts, requests_made: requests, cursor: { meses } });
    console.log(`[lic-da-bip] OK · ${upserts} enlaces`);
  } catch (err) {
    if (runId) await finishRun(runId, { status: 'failed', rows_upserted: upserts, requests_made: requests, error_message: err.message });
    console.error(`[lic-da-bip] ERROR: ${err.message}`);
    process.exitCode = 1;
  }
}

main();
