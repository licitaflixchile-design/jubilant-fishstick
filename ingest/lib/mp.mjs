// Cliente HTTP para las APIs de Mercado Público.
//  - v1 (licitaciones / órdenes de compra): api.mercadopublico.cl · throttle ~2s/req
//  - v2 (compra ágil):                       api2.mercadopublico.cl · header ticket + cuota diaria (429)

const V1_BASE = 'https://api.mercadopublico.cl/servicios/v1/publico';
const V2_BASE = 'https://api2.mercadopublico.cl';

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Se lanza cuando la API v2 devuelve 429 (cuota diaria agotada). */
export class QuotaExceededError extends Error {
  constructor(msg) { super(msg); this.name = 'QuotaExceededError'; }
}

/**
 * GET a la API v1 (JSON). Respeta el throttle de ~1 request cada 2 s del ticket
 * durmiendo DESPUÉS de cada llamada, y reintenta ante 5xx / errores de red.
 */
export async function fetchV1(endpoint, params, ticket, { throttleMs = 2100, retries = 3 } = {}) {
  const qs = new URLSearchParams({ ...params, ticket }).toString();
  const url = `${V1_BASE}/${endpoint}?${qs}`;
  let json = null;
  for (let attempt = 1; ; attempt++) {
    try {
      const res = await fetch(url);
      if (res.status >= 500) throw new Error(`HTTP ${res.status}`);
      const text = await res.text();
      await sleep(throttleMs);
      json = text ? JSON.parse(text) : null;
      // Codigo 10500 = "peticiones simultáneas" (rate limit del ticket): reintentable.
      if (json && String(json.Codigo) === '10500') throw new Error('rate-limit v1 (Codigo 10500)');
      break;
    } catch (err) {
      if (attempt > retries) throw err;
      await sleep(attempt * 5000); // backoff 5s, 10s, 15s... (red/5xx/rate-limit)
    }
  }
  // Error lógico de la API (p.ej. ticket inválido): la respuesta trae {Codigo, Mensaje}
  // sin Listado. Lo lanzamos para que quede visible en ingesta_runs (no reintentable).
  if (json && json.Mensaje && json.Listado === undefined) {
    throw new Error(`API v1: ${json.Mensaje}${json.Codigo ? ` (Codigo ${json.Codigo})` : ''}`);
  }
  return json;
}

/**
 * GET a la API v2 (compra ágil). Lanza QuotaExceededError ante 429 y reintenta 5xx.
 */
export async function fetchV2(path, params, ticket, { retries = 2 } = {}) {
  const qs = params ? '?' + new URLSearchParams(params).toString() : '';
  const url = `${V2_BASE}${path}${qs}`;
  for (let attempt = 1; ; attempt++) {
    const res = await fetch(url, { headers: { ticket } });
    if (res.status === 429) throw new QuotaExceededError('Cuota diaria v2 agotada (HTTP 429)');
    if (res.status >= 500 && attempt <= retries) { await sleep(attempt * 2000); continue; }
    const json = await res.json();
    if (json?.success === 'NOK') {
      const e = json.errors?.[0];
      if (String(e?.codigo) === '429') throw new QuotaExceededError(e?.mensaje ?? 'Cuota agotada');
      throw new Error(e?.mensaje ?? 'Error API v2');
    }
    return json;
  }
}

/** Fecha -> 'ddmmyyyy' (formato que usa la API v1). */
export function ddmmyyyy(date) {
  const d = String(date.getDate()).padStart(2, '0');
  const m = String(date.getMonth() + 1).padStart(2, '0');
  return `${d}${m}${date.getFullYear()}`;
}
