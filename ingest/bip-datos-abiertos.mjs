// Ingesta de BIP (Banco Integrado de Proyectos) desde datos abiertos BIDAT.
//   node ingest/bip-datos-abiertos.mjs
// Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
//   (opcional) BIP_ANOS=2025,2026   años a cargar (def: año actual)
//
// Fuente (MDS, pública, sin auth ni captcha; corte trimestral):
//   ficha:    https://bidat.gob.cl/details/ficha/dataset/registro-de-proyectos-de-inversion
//   recursos: links /contenido-web/recurso/{id-cifrado} → inversiones_{YYYY}.csv
// Los links cifrados ROTAN cuando regeneran el dataset → siempre se parsea la
// ficha para obtener los vigentes, y el año se identifica por Content-Disposition.
// CSV: UTF-8, ';', ~12k iniciativas/año, saltos de línea embebidos en
// EBI_DESCRIPCION (usar csv-parse). Montos en miles de CLP (moneda 1).
import { createWriteStream, createReadStream, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { parse } from 'csv-parse';
import { startRun, finishRun, upsertChunked } from './lib/db.mjs';

const FICHA = 'https://bidat.gob.cl/details/ficha/dataset/registro-de-proyectos-de-inversion';

function anosObjetivo() {
  if (process.env.BIP_ANOS) {
    return process.env.BIP_ANOS.split(',').map((s) => parseInt(s.trim(), 10)).filter(Boolean);
  }
  return [new Date().getUTCFullYear()];
}

const toNum = (v) => {
  if (v == null || v === '') return null;
  const n = parseFloat(String(v).replace(',', '.'));
  return Number.isFinite(n) ? n : null;
};
const toInt = (v) => { const n = parseInt(v, 10); return Number.isFinite(n) ? n : null; };

// Descubre los recursos vigentes de la ficha y su año (por Content-Disposition).
async function descubrirRecursos() {
  const res = await fetch(FICHA);
  if (!res.ok) throw new Error(`Ficha BIDAT: HTTP ${res.status}`);
  const html = await res.text();
  const urls = [...new Set(
    [...html.matchAll(/href="(https:\/\/bidat\.gob\.cl\/contenido-web\/recurso\/[^"]+)"/g)].map((m) => m[1]),
  )];
  console.log(`[bip] ${urls.length} recursos en la ficha`);

  const recursos = [];
  for (const url of urls) {
    const head = await fetch(url, { method: 'HEAD' });
    const disp = head.headers.get('content-disposition') ?? '';
    const m = disp.match(/filename=.*?inversiones_(\d{4})\.csv/i);
    if (m) recursos.push({ ano: parseInt(m[1], 10), url });
  }
  return recursos;
}

function mapRow(r, ano) {
  return {
    codigo:           r.EBI_CODIGO,
    parte:            toInt(r.EBI_PARTE) ?? 0,
    ano_postula:      toInt(r.EBI_ANO_POSTULA) ?? ano,
    etapa_postula:    r.EBI_ETAPA_POSTULA ?? '',
    nombre:           r.EBI_NOMBRE ?? null,
    descripcion:      r.EBI_DESCRIPCION ?? null,
    etapa_actual:     r.EBI_ETAPA_ACTUAL || null,
    reg_clave:        toInt(r.REG_CLAVE),
    rate:             r.EBI_RATE || null,
    fecha_resultados: r.EBI_FECHA_RESULTADOS || null,
    fuentes_finan:    r.EBI_FUENTES_FINAN || null,
    ins_responsable:  r.EBI_INS_RESPONSABLE || null,
    costo_total:      toNum(r.EBI_COSTO_TOTAL),
    solicitado:       toNum(r.EBI_SOLICITADO),
    asignado_vigente: toNum(r.EBI_ASIGNADO_VIGENTE),
    moneda:           r.EBI_MONEDA || null,
    ano_archivo:      ano,
    actualizado_at:   new Date().toISOString(),
  };
}

async function procesarAno(rec, dir) {
  const csvPath = join(dir, `inversiones_${rec.ano}.csv`);
  console.log(`[bip] descargando ${rec.ano}...`);
  const res = await fetch(rec.url);
  if (!res.ok) throw new Error(`Descarga ${rec.ano}: HTTP ${res.status}`);
  await pipeline(Readable.fromWeb(res.body), createWriteStream(csvPath));

  const porPk = new Map(); // dedupe dentro del archivo (última fila manda)
  const parser = createReadStream(csvPath, { encoding: 'utf8' }).pipe(parse({
    delimiter: ';', columns: true, bom: true,
    relax_quotes: true, relax_column_count: true, skip_records_with_error: true,
  }));
  for await (const r of parser) {
    if (!r.EBI_CODIGO) continue;
    const row = mapRow(r, rec.ano);
    porPk.set(`${row.codigo}|${row.parte}|${row.ano_postula}|${row.etapa_postula}`, row);
  }
  const rows = [...porPk.values()];
  console.log(`[bip] ${rec.ano}: ${rows.length} iniciativas`);
  return upsertChunked('bip_iniciativas', rows, 'codigo,parte,ano_postula,etapa_postula');
}

async function main() {
  const anos = anosObjetivo();
  console.log(`[bip] años objetivo: ${anos.join(', ')}`);
  const runId = await startRun('bip');
  let upserts = 0, requests = 0;

  try {
    const recursos = await descubrirRecursos();
    requests += recursos.length + 1;
    const objetivo = recursos.filter((r) => anos.includes(r.ano));
    if (!objetivo.length) throw new Error(`Ningún recurso coincide con años ${anos.join(',')} (hay: ${recursos.map((r) => r.ano).join(',')})`);

    const dir = mkdtempSync(join(tmpdir(), 'bip-'));
    for (const rec of objetivo) {
      upserts += await procesarAno(rec, dir);
      requests++;
    }

    await finishRun(runId, { status: 'success', rows_upserted: upserts, requests_made: requests, cursor: { anos } });
    console.log(`[bip] OK · ${upserts} filas`);
  } catch (err) {
    await finishRun(runId, { status: 'failed', rows_upserted: upserts, requests_made: requests, error_message: err.message });
    console.error(`[bip] ERROR: ${err.message}`);
    process.exitCode = 1;
  }
}

main();
