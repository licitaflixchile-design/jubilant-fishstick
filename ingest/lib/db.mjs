// Cliente Supabase (service_role) + helpers de observabilidad para la ingesta.
import { createClient } from '@supabase/supabase-js';

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!url || !key) throw new Error('Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY');

export const sb = createClient(url, key, { auth: { persistSession: false } });

/** Registra el inicio de una corrida y devuelve su id. */
export async function startRun(dataset) {
  const { data, error } = await sb
    .from('ingesta_runs')
    .insert({ dataset, status: 'running' })
    .select('id')
    .single();
  if (error) throw error;
  return data.id;
}

/** Cierra una corrida con su resultado. */
export async function finishRun(id, patch) {
  await sb.from('ingesta_runs')
    .update({ finished_at: new Date().toISOString(), ...patch })
    .eq('id', id);
}

/** Cursor de la última corrida exitosa de un dataset (para sync incremental). */
export async function lastOkCursor(dataset) {
  const { data } = await sb
    .from('ingesta_runs')
    .select('cursor')
    .eq('dataset', dataset)
    .eq('status', 'success')
    .order('started_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  return data?.cursor ?? null;
}

/**
 * Reintenta una operación de BD ante errores transitorios:
 * 57014 = statement timeout (instancia chica bajo carga) · errores de red.
 * op() debe devolver { error } estilo supabase-js.
 */
export async function withRetry(op, { retries = 4, label = 'db' } = {}) {
  for (let attempt = 1; ; attempt++) {
    try {
      const { error } = await op();
      if (!error) return;
      const transitorio = error.code === '57014' || /timeout|fetch failed/i.test(error.message ?? '');
      if (!transitorio || attempt > retries) throw error;
      const waitMs = attempt * 5000;
      console.warn(`[${label}] ${error.code ?? ''} ${error.message} · retry ${attempt}/${retries} en ${waitMs / 1000}s`);
      await new Promise((r) => setTimeout(r, waitMs));
    } catch (e) {
      if (attempt > retries) throw e;
      const transitorio = e?.code === '57014' || /timeout|fetch failed/i.test(e?.message ?? '');
      if (!transitorio) throw e;
      await new Promise((r) => setTimeout(r, attempt * 5000));
    }
  }
}

/** UPSERT en lotes con reintentos ante timeouts. Devuelve filas afectadas. */
export async function upsertChunked(table, rows, conflict, chunk = 500) {
  let total = 0;
  for (let i = 0; i < rows.length; i += chunk) {
    const slice = rows.slice(i, i + chunk);
    await withRetry(() => sb.from(table).upsert(slice, { onConflict: conflict }), { label: table });
    total += slice.length;
  }
  return total;
}
