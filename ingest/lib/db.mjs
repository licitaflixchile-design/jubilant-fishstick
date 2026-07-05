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

/** UPSERT en lotes (evita payloads gigantes). Devuelve filas afectadas. */
export async function upsertChunked(table, rows, conflict, chunk = 500) {
  let total = 0;
  for (let i = 0; i < rows.length; i += chunk) {
    const slice = rows.slice(i, i + chunk);
    const { error } = await sb.from(table).upsert(slice, { onConflict: conflict });
    if (error) throw error;
    total += slice.length;
  }
  return total;
}
