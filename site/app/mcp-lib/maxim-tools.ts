/* eslint-disable @typescript-eslint/no-explicit-any */
// Zusatz-Tools für Maxims Tagesmix-Automatik
//
//   tidal_mix_candidates       Empfehlungen zu Seed-Songs sammeln, prüfen und ranken
//   tidal_library_check        Track-IDs gegen alle eigenen Playlists, Favoriten und den Verlauf prüfen
//   tidal_favorite_tracks      Favoriten ("Meine Titel") auflisten, optional nur noch nicht einsortierte
//   tidal_playlist_add_tracks  mehrere Songs auf einmal hinzufügen und das Ergebnis nachprüfen
//   tidal_playlist_tracks      Inhalt einer Playlist mit Titel, Interpreten und ISRC
//   tidal_mix_history_record   Songs eines Tagesmixes im Verlauf vermerken (danach 365 Tage gesperrt)
//
// "Gleicher Song" heißt: gleiche Track-ID, gleiche ISRC oder gleicher Titel + gleicher Interpret.
// Beim Titel zählen Klammerzusätze, "feat."-Angaben und Versionshinweise ("- 2011 Remaster") nicht.
// Zwischenspeicher und Verlauf liegen in Upstash Redis unter dem Präfix "mx:".

import { z } from 'zod';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { createAPIClient } from '@tidal-music/api';
import { Redis } from '@upstash/redis';

import { getTidalTokens, getAccessTokenUserId, saveTidalTokens } from './redis';
import { refreshTidalToken } from './tidal-oauth';

// ---------- Einstellungen ----------

const MIX_PREFIX = 'Tagesmix ·';
const PLAYLIST_ITEMS = '/playlists/{id}/relationships/items';
const FAVORITE_TRACKS = '/userCollectionTracks/{id}/relationships/items';
const SIMILAR_TRACKS = '/tracks/{id}/relationships/similarTracks';

const DAY_MS = 86400000;
const HISTORY_DAYS = 365;
const HISTORY_TTL_S = 400 * 86400;
const TRACK_TTL_S = 60 * 86400;
const TRACK_MISSING_TTL_S = 60 * 86400;
const LIST_TTL_S = 45 * 86400;
const FAVORITES_REFRESH_DAYS = 7;
const CALL_BUDGET_MS = 54000; // Vercel-Funktion läuft max. 60 s (siehe route.ts)
const TRACK_BATCH = 20;
const MAX_ENTRY_CHARS = 900000;

// ---------- kleine Helfer ----------

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function text(data: unknown): { content: Array<{ type: 'text'; text: string }> } {
  return { content: [{ type: 'text', text: JSON.stringify(data) }] };
}

function extractToken(extra: any): string {
  const token = extra?.authInfo?.token;
  if (!token) throw new Error('Unauthorized');
  return token;
}

function errMsg(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === 'string') return err;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}

function dedupe(values: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of values) {
    const v = String(raw).trim();
    if (v && !seen.has(v)) {
      seen.add(v);
      out.push(v);
    }
  }
  return out;
}

function chunk<T>(values: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < values.length; i += size) out.push(values.slice(i, i + size));
  return out;
}

function shuffled<T>(values: T[]): T[] {
  const out = values.slice();
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    const tmp = out[i];
    out[i] = out[j];
    out[j] = tmp;
  }
  return out;
}

function isoDate(ts: number): string {
  return new Date(ts).toISOString().slice(0, 10);
}

async function mapLimit<T>(items: T[], limit: number, fn: (item: T, index: number) => Promise<void>): Promise<void> {
  let next = 0;
  const workers: Array<Promise<void>> = [];
  const count = Math.min(Math.max(limit, 1), items.length);
  for (let w = 0; w < count; w++) {
    workers.push(
      (async () => {
        for (;;) {
          const i = next++;
          if (i >= items.length) return;
          await fn(items[i], i);
        }
      })(),
    );
  }
  await Promise.all(workers);
}

function isMixName(name: string | undefined): boolean {
  return (name ?? '').trim().startsWith(MIX_PREFIX);
}

// ---------- Redis ----------

let redisClient: Redis | null = null;

function kv(): Redis {
  if (!redisClient) {
    redisClient = new Redis({
      url: process.env.KV_REST_API_URL!,
      token: process.env.KV_REST_API_TOKEN!,
    });
  }
  return redisClient;
}

async function mgetChunked<T>(keys: string[], size = 100): Promise<Array<T | null>> {
  const out: Array<T | null> = [];
  for (let i = 0; i < keys.length; i += size) {
    const part = keys.slice(i, i + size);
    const values = await kv().mget<Array<T | null>>(part);
    for (let j = 0; j < part.length; j++) out.push(values?.[j] ?? null);
  }
  return out;
}

// ---------- Tidal-Zugang ----------

interface Ctx {
  client: any;
  countryCode: string;
  userId: string;
  deadline: number; // harte Zeitgrenze für diesen Tool-Aufruf
  requests: number;
  rateLimited: number;
  lookups: { batches: number; single: number; missing: number; errors: number; samples: string[] };
}

async function getContext(bearerToken: string, started: number): Promise<Ctx> {
  const userId = await getAccessTokenUserId(bearerToken);
  if (!userId) throw new Error('Unauthorized: invalid token');

  let tokens = await getTidalTokens(userId);
  if (!tokens) throw new Error('Unauthorized: no Tidal session');

  if (tokens.expiresAt < Date.now() + 60000) {
    const refreshed = await refreshTidalToken(tokens.refreshToken);
    tokens = {
      accessToken: refreshed.access_token,
      refreshToken: refreshed.refresh_token ?? tokens.refreshToken,
      expiresAt: Date.now() + refreshed.expires_in * 1000,
      countryCode: tokens.countryCode,
      userId: tokens.userId,
    };
    await saveTidalTokens(userId, tokens);
  }

  const current = tokens;
  const credentialsProvider = {
    bus: new EventTarget(),
    getCredentials: async () => ({
      clientId: 'PYVtmSHMTGI9oBUs',
      requestedScopes: [],
      grantedScopes: [],
      token: current.accessToken,
      expires: current.expiresAt,
      userId: current.userId,
    }),
  };

  const client: any = createAPIClient(credentialsProvider as any);
  return { client, countryCode: current.countryCode || 'US', userId, deadline: started + CALL_BUDGET_MS, requests: 0, rateLimited: 0, lookups: { batches: 0, single: 0, missing: 0, errors: 0, samples: [] } };
}

// Drosselung: höchstens eine Anfrage je `spacing` ms; nach einem 429 pausieren alle Anfragen
// gemeinsam (auch parallele) und der Abstand wird größer, bei Erfolg langsam wieder kleiner.
const SPACING_MIN_MS = 120;
const SPACING_MAX_MS = 1500;
let spacing = SPACING_MIN_MS;
let nextSlot = 0;
let pauseUntil = 0;

async function gate(ctx: Ctx): Promise<void> {
  for (;;) {
    const now = Date.now();
    const wait = Math.max(pauseUntil - now, nextSlot - now);
    if (wait <= 0) break;
    if (now + wait > ctx.deadline - 1500) throw new Error('Zeitbudget des Aufrufs erschöpft (Tidal-Rate-Limit)');
    await sleep(Math.min(wait, 2000));
  }
  nextSlot = Math.max(Date.now(), nextSlot) + spacing;
  ctx.requests++;
}

function retryAfterMs(res: any, attempt: number): number {
  const header = Number(res?.response?.headers?.get?.('retry-after'));
  const ms = Number.isFinite(header) && header > 0 ? header * 1000 : 2000 * attempt;
  return Math.min(Math.max(ms, 1000), 15000);
}

class ApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

// 400/404/410/422: Anfrage selbst ungültig (z. B. Track gibt es nicht mehr) – Wiederholen bringt nichts.
// 401/403 (Anmeldung) zählen bewusst nicht dazu, sonst würden bei einem Token-Problem alle Tracks als "fehlt" gemerkt.
function isPermanent(err: unknown): boolean {
  return err instanceof ApiError && [400, 404, 410, 422].includes(err.status);
}

// GET/POST mit Wiederholung bei 429 (Rate-Limit) und vorübergehenden Fehlern
async function apiCall(ctx: Ctx, method: 'GET' | 'POST', path: string, params: Record<string, unknown>, body?: unknown): Promise<any> {
  const attempts = method === 'GET' ? 5 : 4;
  let lastError: unknown = 'kein Versuch';
  let lastStatus = 0;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    await gate(ctx);
    const remaining = ctx.deadline - Date.now();
    if (remaining < 2000) {
      lastError = 'Zeitbudget des Aufrufs erschöpft';
      break;
    }
    let status = 0;
    let res: any = null;
    try {
      const init: Record<string, unknown> = { params, signal: AbortSignal.timeout(Math.min(15000, remaining - 1000)) };
      if (body !== undefined) init.body = body;
      res = method === 'GET' ? await ctx.client.GET(path, init) : await ctx.client.POST(path, init);
      status = Number(res?.response?.status ?? 0);
      lastStatus = status;
      if (!res?.error && status !== 429 && (method === 'POST' || res?.data)) {
        spacing = Math.max(SPACING_MIN_MS, spacing - 15);
        return res?.data ?? null;
      }
      lastError = res?.error ?? `HTTP ${status}`;
    } catch (err) {
      lastError = errMsg(err);
      if (method === 'POST') break; // Ausgang unklar → nicht blind wiederholen (sonst evtl. doppelt)
    }
    if (status === 429) {
      ctx.rateLimited++;
      spacing = Math.min(SPACING_MAX_MS, Math.round(spacing * 1.5) + 50);
      pauseUntil = Math.max(pauseUntil, Date.now() + retryAfterMs(res, attempt));
      lastError = 'HTTP 429 (Rate-Limit)';
      continue;
    }
    if (method === 'POST') break;
    if (status >= 400 && status < 500 && status !== 408) break;
    if (attempt < attempts) await sleep(400 * attempt);
  }
  throw new ApiError(`Tidal ${method} ${path} fehlgeschlagen (HTTP ${lastStatus || '-'}): ${errMsg(lastError)}`, lastStatus);
}

interface Page {
  data: any[];
  included: any[];
  next: string | null;
}

function extractCursor(next: unknown): string | null {
  if (typeof next !== 'string' || next.length === 0) return null;
  const q = next.indexOf('?');
  return new URLSearchParams(q >= 0 ? next.slice(q + 1) : next).get('page[cursor]');
}

function nextCursorOf(body: any): string | null {
  const metaCursor = body?.links?.meta?.nextCursor;
  if (typeof metaCursor === 'string' && metaCursor.length > 0) return metaCursor;
  return extractCursor(body?.links?.next);
}

async function getPage(ctx: Ctx, path: string, id: string, query: Record<string, unknown>, cursor: string | null): Promise<Page> {
  const q: Record<string, unknown> = { ...query };
  if (cursor) q['page[cursor]'] = cursor;
  const params: Record<string, unknown> = { path: { id } };
  if (Object.keys(q).length > 0) params.query = q;
  let page: Page = { data: [], included: [], next: null };
  for (let attempt = 1; attempt <= 3; attempt++) {
    const body = await apiCall(ctx, 'GET', path, params);
    page = {
      data: Array.isArray(body?.data) ? body.data : [],
      included: Array.isArray(body?.included) ? body.included : [],
      next: nextCursorOf(body),
    };
    // Tidal liefert gelegentlich kurzzeitig eine leere Seite – kurz warten und erneut fragen
    if (page.data.length > 0 || page.next) return page;
    if (attempt < 3) await sleep(250);
  }
  return page;
}

async function collectItemIds(ctx: Ctx, path: string, id: string): Promise<{ ids: string[]; other: number; complete: boolean }> {
  const ids: string[] = [];
  let other = 0;
  let cursor: string | null = null;
  const seen = new Set<string>();
  for (let n = 0; n < 300; n++) {
    if (Date.now() > ctx.deadline - 5000) return { ids, other, complete: false };
    const page: Page = await getPage(ctx, path, id, {}, cursor);
    for (const item of page.data) {
      if (item?.type === 'tracks' && item.id != null) ids.push(String(item.id));
      else other++;
    }
    if (!page.next || seen.has(page.next)) return { ids, other, complete: true };
    seen.add(page.next);
    cursor = page.next;
  }
  return { ids, other, complete: true };
}

// ---------- Track-Details (Titel, Interpreten, ISRC) mit Zwischenspeicher ----------

interface TrackMeta {
  id: string;
  title: string;
  artists: string[];
  isrc?: string;
}

interface CachedTrack {
  t?: string;
  a?: string[];
  i?: string;
  x?: number;
}

interface Details {
  map: Map<string, TrackMeta>;
  missing: Set<string>;
  complete: boolean;
}

async function fetchTrackBatch(ctx: Ctx, ids: string[]): Promise<Map<string, TrackMeta>> {
  const out = new Map<string, TrackMeta>();
  const names = new Map<string, string>();
  const items: any[] = [];
  let cursor: string | null = null;
  for (let pageNo = 0; pageNo < 5; pageNo++) {
    const query: Record<string, unknown> = { countryCode: ctx.countryCode, 'filter[id]': ids, include: ['artists'] };
    if (cursor) query['page[cursor]'] = cursor;
    let body: any = null;
    for (let attempt = 1; attempt <= 2; attempt++) {
      body = await apiCall(ctx, 'GET', '/tracks', { query });
      if (Array.isArray(body?.data) && body.data.length > 0) break;
      if (attempt < 2) await sleep(300);
    }
    const data: any[] = Array.isArray(body?.data) ? body.data : [];
    const included: any[] = Array.isArray(body?.included) ? body.included : [];
    for (const inc of included) {
      if (inc?.type === 'artists' && inc.attributes?.name) names.set(String(inc.id), String(inc.attributes.name));
    }
    items.push(...data);
    const next = nextCursorOf(body);
    if (!next || next === cursor || data.length === 0) break;
    cursor = next;
  }
  for (const item of items) {
    if (item?.type !== 'tracks' || item.id == null) continue;
    const attrs = item.attributes ?? {};
    const rels: any[] = Array.isArray(item.relationships?.artists?.data) ? item.relationships.artists.data : [];
    const artists = rels.map((rel: any) => names.get(String(rel?.id)) ?? '').filter((n: string) => n.length > 0);
    const id = String(item.id);
    out.set(id, {
      id,
      title: typeof attrs.title === 'string' ? attrs.title : '',
      artists,
      isrc: typeof attrs.isrc === 'string' && attrs.isrc ? attrs.isrc.toUpperCase() : undefined,
    });
  }
  return out;
}

// wird true, sobald eine Sammelabfrage mehrere Tracks geliefert hat (Tidal beachtet also mehrere IDs)
let multiIdOk = false;

async function getTrackDetails(ctx: Ctx, ids: string[], until: number): Promise<Details> {
  const unique = dedupe(ids);
  const map = new Map<string, TrackMeta>();
  const missing = new Set<string>();
  const cached = await mgetChunked<CachedTrack>(unique.map((id) => `mx:t:${id}`));
  const toFetch: string[] = [];
  unique.forEach((id, i) => {
    const c = cached[i];
    if (c && typeof c === 'object' && c.x) missing.add(id);
    else if (c && typeof c === 'object' && typeof c.t === 'string') {
      map.set(id, { id, title: c.t, artists: Array.isArray(c.a) ? c.a.map(String) : [], isrc: c.i ? String(c.i) : undefined });
    } else toFetch.push(id);
  });

  let complete = true;
  let errors = 0;
  await mapLimit(chunk(toFetch, TRACK_BATCH), 3, async (batch) => {
    if (Date.now() > until) {
      complete = false;
      return;
    }
    const fetched = new Map<string, TrackMeta>();
    const confirmedMissing = new Set<string>();
    const noteError = (err: unknown) => {
      errors++;
      ctx.lookups.errors++;
      if (ctx.lookups.samples.length < 3) ctx.lookups.samples.push(errMsg(err).slice(0, 160));
    };
    // eine Gruppe abfragen; bei "ungültige ID" (400 o. ä.) halbieren, bis die schuldige ID gefunden ist
    const lookup = async (group: string[]): Promise<void> => {
      if (Date.now() > until) {
        complete = false;
        return;
      }
      if (group.length === 1) ctx.lookups.single++;
      else ctx.lookups.batches++;
      let found: Map<string, TrackMeta>;
      try {
        found = await fetchTrackBatch(ctx, group);
      } catch (err) {
        if (!isPermanent(err)) {
          noteError(err);
          return;
        }
        if (group.length === 1) {
          confirmedMissing.add(group[0]);
          return;
        }
        const half = Math.ceil(group.length / 2);
        await lookup(group.slice(0, half));
        await lookup(group.slice(half));
        return;
      }
      found.forEach((meta, key) => fetched.set(key, meta));
      if (found.size >= 2) multiIdOk = true;
      const rest = group.filter((id) => !found.has(id));
      if (rest.length === 0) return;
      if (group.length === 1) {
        confirmedMissing.add(group[0]);
        return;
      }
      if (multiIdOk && found.size <= 1) {
        // auffällig wenig geliefert: Stichprobe einzeln prüfen, ob Tidal wirklich mehrere IDs beachtet
        const probe = rest.slice(0, 2);
        for (const id of probe) await lookup([id]);
        if (probe.some((id) => fetched.has(id))) multiIdOk = false;
        const remaining = rest.slice(2);
        if (multiIdOk) remaining.forEach((id) => confirmedMissing.add(id));
        else
          await mapLimit(remaining, 2, async (id) => {
            await lookup([id]);
          });
        return;
      }
      if (multiIdOk) {
        // Tidal liefert nicht verfügbare Tracks einfach nicht mit
        rest.forEach((id) => confirmedMissing.add(id));
        return;
      }
      // noch unklar, ob Tidal mehrere IDs beachtet: Rest einzeln nachfragen
      await mapLimit(rest, 2, async (id) => {
        await lookup([id]);
      });
    };
    await lookup(batch);

    const pipe = kv().pipeline();
    let writes = 0;
    for (const id of batch) {
      const meta = fetched.get(id);
      if (meta) {
        map.set(id, meta);
        pipe.set(`mx:t:${id}`, { t: meta.title, a: meta.artists, i: meta.isrc ?? '' }, { ex: TRACK_TTL_S });
        writes++;
      } else {
        missing.add(id);
        if (confirmedMissing.has(id)) {
          ctx.lookups.missing++;
          // in diesem Land nicht verfügbar → eine Weile merken
          pipe.set(`mx:t:${id}`, { x: 1 }, { ex: TRACK_MISSING_TTL_S });
          writes++;
        }
      }
    }
    if (writes > 0) {
      try {
        await pipe.exec();
      } catch {
        // nur Zwischenspeicher – Fehler hier sind unkritisch
      }
    }
  });
  // viele Abruffehler (z. B. Störung bei Tidal) → nicht als vollständig werten
  if (errors > Math.max(3, Math.ceil(toFetch.length * 0.05))) complete = false;
  return { map, missing, complete };
}

// ---------- "Gleicher Song?" ----------

const NON_WORD = new RegExp('[^\\p{L}\\p{N}]+', 'gu');
const BRACKETS = /\s*[([{][^)\]}]*[)\]}]/g;
const FEAT_TAIL = /\s+(?:feat\.?|ft\.?|featuring)\s.*$/;
const VERSION_TAIL =
  /\s+[-–—]\s+.*\b(?:remaster(?:ed)?|version|edit|mix|remix|live|mono|stereo|demo|acoustic|instrumental|single|radio|extended|original|bonus|deluxe|explicit|clean|session|recorded|anniversary|unplugged|reprise|from|soundtrack)\b.*$/;

function fold(value: string): string {
  return value.normalize('NFKD').replace(/[̀-ͯ]/g, '').toLowerCase();
}

function squash(value: string): string {
  return value.replace(/&/g, ' and ').replace(NON_WORD, ' ').trim().replace(/\s+/g, ' ');
}

function normTitle(title: string): string {
  let t = fold(title);
  t = t.replace(BRACKETS, ' ');
  t = t.replace(FEAT_TAIL, '');
  t = t.replace(VERSION_TAIL, '');
  return squash(t);
}

function normArtist(name: string): string {
  return squash(fold(name)).replace(/^the /, '');
}

function songKeys(meta: { title: string; artists: string[] }): string[] {
  const title = normTitle(meta.title);
  if (!title) return [];
  const keys = new Set<string>();
  for (const artist of meta.artists) {
    const a = normArtist(artist);
    if (a) keys.add(`${title}|${a}`);
  }
  return Array.from(keys);
}

// ---------- Verlauf der Tagesmixe ----------

function historyKeys(id: string, meta: TrackMeta | undefined, isrcHint?: string): string[] {
  const keys = new Set<string>([`id:${id}`]);
  const isrc = meta?.isrc ?? isrcHint;
  if (isrc) keys.add(`isrc:${isrc.toUpperCase()}`);
  if (meta) for (const k of songKeys(meta)) keys.add(`song:${k}`);
  return Array.from(keys);
}

// liefert pro Eintrag den Zeitpunkt der letzten Empfehlung (0 = nie)
async function historyLookup(userId: string, keyLists: string[][]): Promise<number[]> {
  const flat: string[] = [];
  const spans: Array<[number, number]> = [];
  for (const list of keyLists) {
    spans.push([flat.length, list.length]);
    for (const k of list) flat.push(`mx:h:${userId}:${k}`);
  }
  const values = await mgetChunked<number | string>(flat, 200);
  return spans.map(([start, len]) => {
    let latest = 0;
    for (let i = start; i < start + len; i++) {
      const v = Number(values[i] ?? 0);
      if (Number.isFinite(v) && v > latest) latest = v;
    }
    return latest;
  });
}

async function historyRecord(userId: string, keyLists: string[][], when: number): Promise<number> {
  const keys = dedupe(keyLists.reduce<string[]>((acc, list) => acc.concat(list), []));
  for (const part of chunk(keys, 200)) {
    const pipe = kv().pipeline();
    for (const k of part) pipe.set(`mx:h:${userId}:${k}`, when, { ex: HISTORY_TTL_S });
    await pipe.exec();
  }
  return keys.length;
}

// ---------- Index der eigenen Bibliothek (alle Playlists + Favoriten) ----------
// Pro Playlist wird ein Eintrag gespeichert und nur neu gelesen, wenn sich die Playlist geändert hat.
// Große Bibliotheken werden über mehrere Aufrufe hinweg eingelesen (Fortschritt wird gespeichert).

interface WorkState {
  s: string;
  ids: string[];
  c: string | null;
  done: boolean;
}

interface ListEntry {
  s?: string;
  ids?: string[];
  isrcs?: string[];
  keys?: string[];
  w?: WorkState;
}

interface ListSource {
  cacheKey: string;
  path: string;
  id: string;
  name: string;
  stamp: string;
  size: number;
  favorites: boolean;
}

interface LibraryIndex {
  ids: Map<string, string>;
  isrcs: Map<string, string>;
  keys: Map<string, string>;
  listIds: Map<string, string[]>;
  playlists: number;
  favorites: number | null;
  favoritesError?: string;
  pending: string[];
  failed: string[];
  progress: string[]; // Stand der noch offenen Playlists (Diagnose)
  errors: string[]; // Fehlermeldungen der fehlgeschlagenen Playlists (Diagnose)
  complete: boolean;
}

function stringList(value: unknown): string[] | undefined {
  return Array.isArray(value) ? value.map((v) => String(v)) : undefined;
}

function readEntry(raw: unknown): ListEntry {
  if (!raw || typeof raw !== 'object') return {};
  const e = raw as Record<string, any>;
  const out: ListEntry = {
    s: typeof e.s === 'string' ? e.s : undefined,
    ids: stringList(e.ids),
    isrcs: stringList(e.isrcs),
    keys: stringList(e.keys),
  };
  if (e.w && typeof e.w === 'object' && typeof e.w.s === 'string') {
    out.w = { s: e.w.s, ids: stringList(e.w.ids) ?? [], c: typeof e.w.c === 'string' ? e.w.c : null, done: e.w.done === true };
  }
  return out;
}

function fitEntry(entry: ListEntry): ListEntry {
  if (JSON.stringify(entry).length <= MAX_ENTRY_CHARS) return entry;
  return { ...entry, keys: [] };
}

function freshWork(stamp: string): WorkState {
  return { s: stamp, ids: [], c: null, done: false };
}

async function advanceList(ctx: Ctx, src: ListSource, entry: ListEntry, until: number): Promise<ListEntry> {
  const result: ListEntry = { ...entry };
  let work: WorkState =
    entry.w && entry.w.s === src.stamp
      ? { s: entry.w.s, ids: entry.w.ids.slice(), c: entry.w.c, done: entry.w.done }
      : freshWork(src.stamp);
  const seen = new Set<string>();
  let restarted = false;

  while (!work.done) {
    if (Date.now() > until) {
      result.w = work;
      return result;
    }
    let page: Page;
    try {
      page = await getPage(ctx, src.path, src.id, {}, work.c);
    } catch (err) {
      if (work.c && !restarted) {
        // gespeicherter Cursor evtl. abgelaufen → von vorn beginnen
        restarted = true;
        work = freshWork(src.stamp);
        continue;
      }
      throw err;
    }
    for (const item of page.data) {
      if (item?.type === 'tracks' && item.id != null) work.ids.push(String(item.id));
    }
    if (!page.next || seen.has(page.next) || page.next === work.c) {
      work.done = true;
      work.c = null;
    } else {
      seen.add(page.next);
      work.c = page.next;
    }
  }

  const ids = dedupe(work.ids);
  const details = await getTrackDetails(ctx, ids, until);
  if (!details.complete) {
    result.w = { s: work.s, ids, c: null, done: true };
    return result;
  }
  const isrcs = new Set<string>();
  const keys = new Set<string>();
  for (const id of ids) {
    const meta = details.map.get(id);
    if (!meta) continue;
    if (meta.isrc) isrcs.add(meta.isrc);
    for (const k of songKeys(meta)) keys.add(k);
  }
  return { s: src.stamp, ids, isrcs: Array.from(isrcs), keys: Array.from(keys) };
}

interface OwnPlaylist {
  id: string;
  name: string;
  numberOfItems?: number;
  lastModifiedAt?: string;
}

// eigene Playlists (alle Seiten), mit Rate-Limit-Behandlung über apiCall
async function listOwnPlaylists(ctx: Ctx): Promise<OwnPlaylist[]> {
  const out: OwnPlaylist[] = [];
  const seen = new Set<string>();
  let cursor: string | null = null;
  for (let n = 0; n < 50; n++) {
    const query: Record<string, unknown> = { 'filter[owners.id]': ['me'], countryCode: ctx.countryCode };
    if (cursor) query['page[cursor]'] = cursor;
    let body: any = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
      body = await apiCall(ctx, 'GET', '/playlists', { query });
      if (Array.isArray(body?.data) && body.data.length > 0) break;
      if (attempt < 3) await sleep(300);
    }
    const data: any[] = Array.isArray(body?.data) ? body.data : [];
    for (const p of data) {
      if (p?.id == null) continue;
      out.push({
        id: String(p.id),
        name: typeof p.attributes?.name === 'string' ? p.attributes.name : 'Untitled',
        numberOfItems: typeof p.attributes?.numberOfItems === 'number' ? p.attributes.numberOfItems : undefined,
        lastModifiedAt: typeof p.attributes?.lastModifiedAt === 'string' ? p.attributes.lastModifiedAt : undefined,
      });
    }
    const next = nextCursorOf(body);
    if (!next || seen.has(next) || data.length === 0) break;
    seen.add(next);
    cursor = next;
  }
  if (out.length === 0) throw new Error('Tidal hat keine Playlists geliefert – bitte erneut versuchen');
  return out;
}

async function buildLibraryIndex(ctx: Ctx, until: number, includeFavorites: boolean): Promise<LibraryIndex> {
  const playlists = await listOwnPlaylists(ctx);
  const sources: ListSource[] = playlists
    .filter((p) => !isMixName(p.name))
    .map((p) => ({
      cacheKey: `mx:pl:${ctx.userId}:${p.id}`,
      path: PLAYLIST_ITEMS,
      id: p.id,
      name: p.name,
      stamp: `${p.lastModifiedAt ?? ''}|${p.numberOfItems ?? ''}`,
      size: p.numberOfItems ?? 0,
      favorites: false,
    }))
    .sort((a, b) => a.size - b.size);
  if (includeFavorites) {
    sources.push({
      cacheKey: `mx:fav:${ctx.userId}`,
      path: FAVORITE_TRACKS,
      id: 'me',
      name: 'Favoriten',
      stamp: `w${Math.floor(Date.now() / (FAVORITES_REFRESH_DAYS * DAY_MS))}`,
      size: Number.MAX_SAFE_INTEGER,
      favorites: true,
    });
  }

  const stored = await mgetChunked<unknown>(
    sources.map((s) => s.cacheKey),
    5,
  );
  const state = sources.map((src, i) => ({ src, entry: readEntry(stored[i]), error: '' }));
  // Favoriten zuerst, damit sie nicht hinter großen Playlists warten
  const todo = state
    .filter((st) => !(st.entry.s === st.src.stamp && !st.entry.w))
    .sort((a, b) => Number(b.src.favorites) - Number(a.src.favorites));

  await mapLimit(todo, 3, async (st) => {
    if (Date.now() > until) return;
    try {
      st.entry = fitEntry(await advanceList(ctx, st.src, st.entry, until));
      await kv().set(st.src.cacheKey, st.entry, { ex: LIST_TTL_S });
    } catch (err) {
      st.error = errMsg(err);
    }
  });

  const index: LibraryIndex = {
    ids: new Map(),
    isrcs: new Map(),
    keys: new Map(),
    listIds: new Map(),
    playlists: 0,
    favorites: null,
    pending: [],
    failed: [],
    progress: [],
    errors: [],
    complete: true,
  };
  const addAll = (target: Map<string, string>, values: string[] | undefined, label: string) => {
    if (!values) return;
    for (const v of values) if (!target.has(v)) target.set(v, label);
  };
  for (const { src, entry, error } of state) {
    const label = src.favorites ? 'Favoriten' : `Playlist "${src.name}"`;
    const workIds = entry.w && entry.w.s === src.stamp ? entry.w.ids : [];
    addAll(index.ids, entry.ids, label);
    addAll(index.ids, workIds, label);
    addAll(index.isrcs, entry.isrcs, label);
    addAll(index.keys, entry.keys, label);
    if (src.favorites) {
      index.favorites = entry.ids ? entry.ids.length : null;
      if (error) index.favoritesError = error;
    } else {
      index.playlists++;
      index.listIds.set(src.id, dedupe((entry.ids ?? []).concat(workIds)));
    }
    const current = entry.s === src.stamp && !entry.w;
    if (!current) {
      if (error) {
        index.failed.push(src.name);
        index.errors.push(`${src.name}: ${error.slice(0, 160)}`);
      } else {
        index.pending.push(src.name);
        const w = entry.w && entry.w.s === src.stamp ? entry.w : null;
        index.progress.push(`${src.name}: ${w ? (w.done ? `Details für ${w.ids.length} Songs` : `${w.ids.length} Songs gelesen`) : 'noch nicht begonnen'}`);
      }
    }
  }
  index.complete = index.pending.length === 0;
  return index;
}

function ownedWhere(index: LibraryIndex, id: string, meta?: TrackMeta, isrcHint?: string): string | null {
  const byId = index.ids.get(id);
  if (byId) return byId;
  const isrc = meta?.isrc ?? isrcHint;
  if (isrc) {
    const byIsrc = index.isrcs.get(isrc);
    if (byIsrc) return `${byIsrc} (gleiche ISRC)`;
  }
  if (meta) {
    for (const k of songKeys(meta)) {
      const byKey = index.keys.get(k);
      if (byKey) return `${byKey} (gleicher Titel + Interpret)`;
    }
  }
  return null;
}

function librarySummary(index: LibraryIndex) {
  return {
    playlists: index.playlists,
    tracks: index.ids.size,
    favorites: index.favorites,
    pending: index.pending,
    failed: index.failed,
    favoritesError: index.favoritesError,
    progress: index.progress.length > 0 ? index.progress.slice(0, 12) : undefined,
    errors: index.errors.length > 0 ? index.errors.slice(0, 6) : undefined,
  };
}

// ---------- Ähnliche Songs zu einem Seed ----------

interface SimilarHit {
  id: string;
  title: string;
  isrc?: string;
  pop: number;
}

async function similarFor(ctx: Ctx, seedId: string, pages: number, until: number): Promise<SimilarHit[]> {
  const hits: SimilarHit[] = [];
  const seenIds = new Set<string>();
  let cursor: string | null = null;
  for (let p = 0; p < pages; p++) {
    if (p > 0 && Date.now() > until) break;
    const page: Page = await getPage(ctx, SIMILAR_TRACKS, seedId, { countryCode: ctx.countryCode, include: ['similarTracks'] }, cursor);
    const attrs = new Map<string, any>();
    for (const inc of page.included) {
      if (inc?.type === 'tracks' && inc.id != null) attrs.set(String(inc.id), inc.attributes ?? {});
    }
    const order: any[] = page.data.length > 0 ? page.data : page.included;
    for (const item of order) {
      if (item?.type !== 'tracks' || item.id == null) continue;
      const id = String(item.id);
      if (seenIds.has(id)) continue;
      seenIds.add(id);
      const a = attrs.get(id) ?? {};
      hits.push({
        id,
        title: typeof a.title === 'string' ? a.title : '',
        isrc: typeof a.isrc === 'string' && a.isrc ? a.isrc.toUpperCase() : undefined,
        pop: typeof a.popularity === 'number' ? a.popularity : 0,
      });
    }
    if (!page.next || page.next === cursor) break;
    cursor = page.next;
  }
  return hits;
}

interface Cand {
  id: string;
  title: string;
  isrc?: string;
  pop: number;
  seeds: Set<string>;
  rank: number;
}

interface OkCand {
  c: Cand;
  meta: TrackMeta;
  keys: string[];
}

// ---------- Tools ----------

export function registerMaximTools(server: McpServer) {
  server.tool(
    'tidal_mix_candidates',
    'Collect candidates for a Tagesmix: similar tracks of seed songs (random seeds from a playlist and/or given seed IDs), filtered against ALL own playlists (except "Tagesmix ·" ones), the favorites and the Tagesmix history (default 365 days). Other versions of a song count as the same song (same ISRC or same title + artist). Returns a ranked list (songs recommended by several seeds first, then alternating between seeds) that already respects maxPerArtist and contains no song twice. If libraryComplete is false, call the tool again with the same arguments.',
    {
      seedPlaylistId: z.string().optional().describe('Playlist to draw random seed songs from (e.g. the category playlist)'),
      seedCount: z.number().int().min(1).max(30).optional().describe('Number of random seeds from seedPlaylistId (default 15)'),
      seedTrackIds: z.array(z.string()).max(40).optional().describe('Additional fixed seed track IDs'),
      excludeTrackIds: z.array(z.string()).max(500).optional().describe('Track IDs to leave out, incl. other versions of the same songs'),
      limit: z.number().int().min(1).max(150).optional().describe('Max number of candidates returned (default 40)'),
      maxPerArtist: z.number().int().min(0).max(20).optional().describe('Max songs per artist in the returned list, 0 = no limit (default 2)'),
      pagesPerSeed: z.number().int().min(1).max(5).optional().describe('Pages of similar tracks per seed, about 20 each (default 1)'),
      windowDays: z.number().int().min(0).max(3650).optional().describe('History window in days (default 365)'),
      includeFavorites: z.boolean().optional().describe('Treat favorited tracks as owned (default true)'),
      force: z.boolean().optional().describe('Return candidates even if the library index is still incomplete'),
    },
    { readOnlyHint: true, destructiveHint: false, openWorldHint: true, title: 'Tagesmix Candidates' },
    async ({ seedPlaylistId, seedCount, seedTrackIds, excludeTrackIds, limit, maxPerArtist, pagesPerSeed, windowDays, includeFavorites, force }, extra) => {
      const started = Date.now();
      const ctx = await getContext(extractToken(extra), started);
      const max = limit ?? 40;
      const perArtist = maxPerArtist ?? 2;
      const pages = pagesPerSeed ?? 1;
      const windowMs = (windowDays ?? HISTORY_DAYS) * DAY_MS;

      const index = await buildLibraryIndex(ctx, started + 25000, includeFavorites ?? true);
      if (!index.complete && !force) {
        return text({
          libraryComplete: false,
          library: librarySummary(index),
          api: { requests: ctx.requests, rateLimited: ctx.rateLimited, lookups: ctx.lookups },
          message:
            'Die Bibliothek wird noch eingelesen (beim ersten Mal und nach Änderungen an Playlists). Bitte dieses Tool gleich noch einmal mit denselben Angaben aufrufen.',
        });
      }

      // Seeds bestimmen
      const seeds: string[] = [];
      if (seedPlaylistId) {
        let pool = index.listIds.get(seedPlaylistId) ?? [];
        if (pool.length === 0) pool = (await collectItemIds(ctx, PLAYLIST_ITEMS, seedPlaylistId)).ids;
        seeds.push(...shuffled(dedupe(pool)).slice(0, seedCount ?? 15));
      }
      for (const id of dedupe(seedTrackIds ?? [])) if (!seeds.includes(id)) seeds.push(id);
      if (seeds.length === 0) {
        return text({
          libraryComplete: index.complete,
          error: 'Keine Seed-Songs gefunden (seedPlaylistId leer oder unbekannt und keine seedTrackIds).',
        });
      }

      // Ähnliche Songs pro Seed
      const hitsBySeed = new Map<string, SimilarHit[]>();
      let seedErrors = 0;
      await mapLimit(seeds, 4, async (seed) => {
        if (Date.now() > started + 38000) return;
        try {
          hitsBySeed.set(seed, await similarFor(ctx, seed, pages, started + 38000));
        } catch {
          seedErrors++;
        }
      });

      const excludeIds = dedupe(excludeTrackIds ?? []);
      const blockedIds = new Set<string>(seeds.concat(excludeIds));
      const cands = new Map<string, Cand>();
      for (const seed of seeds) {
        (hitsBySeed.get(seed) ?? []).forEach((hit, rank) => {
          if (blockedIds.has(hit.id)) return;
          let c = cands.get(hit.id);
          if (!c) {
            c = { id: hit.id, title: hit.title, isrc: hit.isrc, pop: hit.pop, seeds: new Set<string>(), rank };
            cands.set(hit.id, c);
          }
          c.seeds.add(seed);
          if (rank < c.rank) c.rank = rank;
        });
      }
      const candIds = Array.from(cands.keys());
      const details = await getTrackDetails(ctx, candIds.concat(excludeIds), started + 48000);

      // andere Fassungen der ausgeschlossenen Songs ebenfalls sperren
      const blockIsrcs = new Set<string>();
      const blockKeys = new Set<string>();
      for (const id of excludeIds) {
        const meta = details.map.get(id);
        if (!meta) continue;
        if (meta.isrc) blockIsrcs.add(meta.isrc);
        for (const k of songKeys(meta)) blockKeys.add(k);
      }

      const lastSeen = await historyLookup(
        ctx.userId,
        candIds.map((id) => historyKeys(id, details.map.get(id), cands.get(id)?.isrc)),
      );
      const stats = { owned: 0, recent: 0, excluded: 0, unknown: 0, sameSongMerged: 0, artistLimit: 0 };
      const ok: OkCand[] = [];
      candIds.forEach((id, i) => {
        const c = cands.get(id);
        if (!c) return;
        const meta = details.map.get(id);
        if (!meta) {
          stats.unknown++;
          return;
        }
        if (ownedWhere(index, id, meta, c.isrc)) {
          stats.owned++;
          return;
        }
        const keys = songKeys(meta);
        if ((meta.isrc && blockIsrcs.has(meta.isrc)) || keys.some((k) => blockKeys.has(k))) {
          stats.excluded++;
          return;
        }
        if (lastSeen[i] > 0 && started - lastSeen[i] < windowMs) {
          stats.recent++;
          return;
        }
        ok.push({ c, meta, keys });
      });

      // verschiedene Fassungen desselben Songs zusammenfassen
      const byStrength = (a: OkCand, b: OkCand) => b.c.seeds.size - a.c.seeds.size || b.c.pop - a.c.pop || a.c.rank - b.c.rank;
      ok.sort(byStrength);
      const owner = new Map<string, OkCand>();
      const winners: OkCand[] = [];
      for (const item of ok) {
        const ident = item.keys.map((k) => `k:${k}`);
        if (item.meta.isrc) ident.push(`i:${item.meta.isrc}`);
        let target: OkCand | undefined;
        for (const k of ident) {
          const hit = owner.get(k);
          if (hit) {
            target = hit;
            break;
          }
        }
        if (target) {
          const winner = target;
          item.c.seeds.forEach((s) => winner.c.seeds.add(s));
          stats.sameSongMerged++;
        } else {
          winners.push(item);
          target = item;
        }
        for (const k of ident) if (!owner.has(k)) owner.set(k, target);
      }

      // Reihenfolge: von mehreren Seeds empfohlen zuerst, danach abwechselnd über die Seeds
      const tier1 = winners.filter((w) => w.c.seeds.size >= 2).sort(byStrength);
      const bySeed = new Map<string, OkCand[]>();
      for (const w of winners) {
        if (w.c.seeds.size >= 2) continue;
        const seed = Array.from(w.c.seeds)[0];
        const list = bySeed.get(seed) ?? [];
        list.push(w);
        bySeed.set(seed, list);
      }
      bySeed.forEach((list) => list.sort((a, b) => a.c.rank - b.c.rank || b.c.pop - a.c.pop));
      const tier2: OkCand[] = [];
      for (let round = 0; ; round++) {
        let progressed = false;
        for (const seed of seeds) {
          const list = bySeed.get(seed);
          if (list && round < list.length) {
            tier2.push(list[round]);
            progressed = true;
          }
        }
        if (!progressed) break;
      }

      // Interpreten-Limit und Länge
      const artistCount = new Map<string, number>();
      const out: Array<{ id: string; title: string; artists: string[]; isrc: string | null; seeds: number; pop: number }> = [];
      for (const w of tier1.concat(tier2)) {
        if (out.length >= max) break;
        const names = dedupe(w.meta.artists.map(normArtist));
        if (perArtist > 0 && names.some((n) => (artistCount.get(n) ?? 0) >= perArtist)) {
          stats.artistLimit++;
          continue;
        }
        for (const n of names) artistCount.set(n, (artistCount.get(n) ?? 0) + 1);
        out.push({
          id: w.c.id,
          title: w.meta.title || w.c.title,
          artists: w.meta.artists,
          isrc: w.meta.isrc ?? null,
          seeds: w.c.seeds.size,
          pop: Math.round(w.c.pop * 100) / 100,
        });
      }

      return text({
        libraryComplete: index.complete,
        library: librarySummary(index),
        api: { requests: ctx.requests, rateLimited: ctx.rateLimited, lookups: ctx.lookups },
        seeds: { used: seeds.length, answered: hitsBySeed.size, errors: seedErrors },
        filtered: { raw: cands.size, ...stats, available: winners.length },
        note: out.length < 25 ? 'Wenige Kandidaten: erneut mit höherem seedCount (bis 30) und pagesPerSeed (bis 5) aufrufen.' : undefined,
        candidates: out,
      });
    },
  );

  server.tool(
    'tidal_library_check',
    'Check track IDs against ALL own playlists (except "Tagesmix ·" ones), the favorites and the Tagesmix history (default 365 days). Other versions of the same song count as owned (same ISRC or same title + artist). Returns ok and blocked tracks with the reason. If libraryComplete is false, simply call again.',
    {
      trackIds: z.array(z.string()).min(1).max(300).describe('Tidal track IDs to check'),
      windowDays: z.number().int().min(0).max(3650).optional().describe('History window in days (default 365)'),
      includeFavorites: z.boolean().optional().describe('Treat favorited tracks as owned (default true)'),
    },
    { readOnlyHint: true, destructiveHint: false, openWorldHint: true, title: 'Library & History Check' },
    async ({ trackIds, windowDays, includeFavorites }, extra) => {
      const started = Date.now();
      const ctx = await getContext(extractToken(extra), started);
      const ids = dedupe(trackIds);
      const index = await buildLibraryIndex(ctx, started + 30000, includeFavorites ?? true);
      const details = await getTrackDetails(ctx, ids, started + 48000);
      const lastSeen = await historyLookup(
        ctx.userId,
        ids.map((id) => historyKeys(id, details.map.get(id))),
      );
      const windowMs = (windowDays ?? HISTORY_DAYS) * DAY_MS;
      const ok: Array<Record<string, unknown>> = [];
      const blocked: Array<Record<string, unknown>> = [];
      ids.forEach((id, i) => {
        const meta = details.map.get(id);
        const base = { id, title: meta?.title ?? null, artists: meta?.artists ?? [] };
        const where = ownedWhere(index, id, meta);
        if (where) blocked.push({ ...base, why: 'owned', where });
        else if (lastSeen[i] > 0 && started - lastSeen[i] < windowMs) blocked.push({ ...base, why: 'recent', last: isoDate(lastSeen[i]) });
        else if (!meta) blocked.push({ ...base, why: 'unknown' });
        else ok.push({ ...base, isrc: meta.isrc ?? null });
      });
      return text({
        libraryComplete: index.complete,
        library: librarySummary(index),
        api: { requests: ctx.requests, rateLimited: ctx.rateLimited, lookups: ctx.lookups },
        windowDays: windowDays ?? HISTORY_DAYS,
        ok,
        blocked,
      });
    },
  );

  server.tool(
    'tidal_favorite_tracks',
    'List the favorited tracks ("Meine Titel") with title, artists and ISRC. With notInPlaylistIds only favorites are returned whose song (same track ID, same ISRC or same title + artist) is in none of these playlists – e.g. new favorites that are not yet sorted into the category playlists. Favorites are re-read from Tidal at most once a week unless refresh is true. Unavailable tracks are left out. If complete is false, simply call again with the same arguments.',
    {
      notInPlaylistIds: z.array(z.string()).max(100).optional().describe('Only return favorites whose song is in none of these playlists'),
      refresh: z.boolean().optional().describe('Re-read the favorites from Tidal today instead of using the copy from the last 7 days (default false)'),
      limit: z.number().int().min(1).max(500).optional().describe('Max number of tracks returned (default 200)'),
      offset: z.number().int().min(0).optional().describe('Skip this many matching tracks, for paging (default 0)'),
    },
    { readOnlyHint: true, destructiveHint: false, openWorldHint: true, title: 'Favorite Tracks' },
    async ({ notInPlaylistIds, refresh, limit, offset }, extra) => {
      const started = Date.now();
      const ctx = await getContext(extractToken(extra), started);
      const api = () => ({ requests: ctx.requests, rateLimited: ctx.rateLimited, lookups: ctx.lookups });

      // 1) Favoriten lesen – gleicher Zwischenspeicher wie im Bibliotheks-Index
      const favSource: ListSource = {
        cacheKey: `mx:fav:${ctx.userId}`,
        path: FAVORITE_TRACKS,
        id: 'me',
        name: 'Favoriten',
        stamp: refresh ? `d${isoDate(started)}` : `w${Math.floor(started / (FAVORITES_REFRESH_DAYS * DAY_MS))}`,
        size: Number.MAX_SAFE_INTEGER,
        favorites: true,
      };
      let favEntry = readEntry(await kv().get(favSource.cacheKey));
      if (!(favEntry.s === favSource.stamp && !favEntry.w)) {
        favEntry = fitEntry(await advanceList(ctx, favSource, favEntry, started + 25000));
        await kv().set(favSource.cacheKey, favEntry, { ex: LIST_TTL_S });
      }
      if (favEntry.w || !favEntry.ids) {
        return text({
          complete: false,
          note: 'Favoriten werden noch eingelesen – einfach mit denselben Angaben erneut aufrufen.',
          read: favEntry.w ? favEntry.w.ids.length : 0,
          api: api(),
        });
      }
      const favIds = favEntry.ids;

      // 2) Songs der angegebenen Playlists (gleiche ID, ISRC oder Titel + Interpret)
      const wanted = dedupe(notInPlaylistIds ?? []);
      const exclIds = new Set<string>();
      const exclIsrcs = new Set<string>();
      const exclKeys = new Set<string>();
      const notFound: string[] = [];
      if (wanted.length > 0) {
        const index = await buildLibraryIndex(ctx, started + 40000, false);
        if (!index.complete) {
          return text({
            complete: false,
            note: 'Playlists werden noch eingelesen – einfach mit denselben Angaben erneut aufrufen.',
            library: librarySummary(index),
            api: api(),
          });
        }
        const raw = await mgetChunked<unknown>(
          wanted.map((id) => `mx:pl:${ctx.userId}:${id}`),
          50,
        );
        raw.forEach((value, i) => {
          const e = readEntry(value);
          if (!e.ids) {
            notFound.push(wanted[i]);
            return;
          }
          for (const v of e.ids) exclIds.add(v);
          for (const v of e.isrcs ?? []) exclIsrcs.add(v);
          for (const v of e.keys ?? []) exclKeys.add(v);
        });
      }

      // 3) Details der Favoriten und Filter
      const details = await getTrackDetails(ctx, favIds, started + 48000);
      if (!details.complete) {
        return text({
          complete: false,
          note: 'Details der Favoriten werden noch geladen – einfach mit denselben Angaben erneut aufrufen.',
          api: api(),
        });
      }
      const matched: Array<Record<string, unknown>> = [];
      let unavailable = 0;
      for (const id of favIds) {
        const meta = details.map.get(id);
        if (!meta) {
          unavailable++;
          continue;
        }
        if (exclIds.has(id)) continue;
        if (meta.isrc && exclIsrcs.has(meta.isrc)) continue;
        if (songKeys(meta).some((k) => exclKeys.has(k))) continue;
        matched.push({ id, title: meta.title, artists: meta.artists, isrc: meta.isrc ?? null });
      }

      const from = offset ?? 0;
      const page = matched.slice(from, from + (limit ?? 200));
      return text({
        complete: true,
        favorites: favIds.length,
        unavailable,
        checkedPlaylists: wanted.length - notFound.length,
        notFoundPlaylists: notFound,
        matched: matched.length,
        offset: from,
        returned: page.length,
        more: from + page.length < matched.length,
        tracks: page,
        api: api(),
      });
    },
  );

  server.tool(
    'tidal_playlist_add_tracks',
    'Add several tracks to a playlist in one go (tracks already in the playlist are skipped), then read the playlist back to verify. Returns total, missing (not in the playlist afterwards) and duplicates.',
    {
      playlistId: z.string().describe('Tidal playlist ID'),
      trackIds: z.array(z.string()).min(1).max(100).describe('Track IDs in the desired order'),
      shuffle: z.boolean().optional().describe('Shuffle the order before adding (default false)'),
    },
    { readOnlyHint: false, destructiveHint: false, openWorldHint: true, title: 'Add Tracks (batch)' },
    async ({ playlistId, trackIds, shuffle }, extra) => {
      const started = Date.now();
      const ctx = await getContext(extractToken(extra), started);
      const before = await collectItemIds(ctx, PLAYLIST_ITEMS, playlistId);
      const present = new Set(before.ids);
      const wanted = shuffle ? shuffled(dedupe(trackIds)) : dedupe(trackIds);
      const toAdd = wanted.filter((id) => !present.has(id));
      const errors: string[] = [];
      const post = (ids: string[]) =>
        apiCall(ctx, 'POST', PLAYLIST_ITEMS, { path: { id: playlistId } }, { data: ids.map((id) => ({ id, type: 'tracks' })) });

      for (const part of chunk(toAdd, 20)) {
        try {
          await post(part);
        } catch (err) {
          errors.push(errMsg(err));
        }
      }

      const readBack = async (waitMs: number) => {
        if (waitMs > 0) await sleep(waitMs);
        const ids = (await collectItemIds(ctx, PLAYLIST_ITEMS, playlistId)).ids;
        return { ids, set: new Set(ids) };
      };
      let after = await readBack(toAdd.length > 0 ? 800 : 0);
      let missing = toAdd.filter((id) => !after.set.has(id));
      if (missing.length > 0) {
        after = await readBack(1500);
        missing = toAdd.filter((id) => !after.set.has(id));
      }
      if (missing.length > 0 && Date.now() < ctx.deadline - 15000) {
        for (const id of missing) {
          try {
            await post([id]);
          } catch (err) {
            errors.push(`${id}: ${errMsg(err)}`);
          }
        }
        after = await readBack(1000);
        missing = toAdd.filter((id) => !after.set.has(id));
      }

      const counts = new Map<string, number>();
      for (const id of after.ids) counts.set(id, (counts.get(id) ?? 0) + 1);
      const duplicates = Array.from(counts.entries())
        .filter(([, n]) => n > 1)
        .map(([id, n]) => ({ id, count: n }));
      return text({
        playlistId,
        requested: wanted.length,
        alreadyPresent: wanted.length - toAdd.length,
        added: toAdd.length - missing.length,
        missing,
        total: after.ids.length,
        duplicates,
        errors,
      });
    },
  );

  server.tool(
    'tidal_playlist_tracks',
    'List all tracks of a playlist in order, with title, artists and ISRC (also to verify how many songs a playlist really contains).',
    {
      playlistId: z.string().describe('Tidal playlist ID'),
    },
    { readOnlyHint: true, destructiveHint: false, openWorldHint: true, title: 'Playlist Tracks' },
    async ({ playlistId }, extra) => {
      const started = Date.now();
      const ctx = await getContext(extractToken(extra), started);
      const items = await collectItemIds(ctx, PLAYLIST_ITEMS, playlistId);
      const details = await getTrackDetails(ctx, items.ids, started + 48000);
      return text({
        playlistId,
        count: items.ids.length,
        otherItems: items.other,
        complete: items.complete,
        tracks: items.ids.map((id, i) => {
          const meta = details.map.get(id);
          return { pos: i + 1, id, title: meta?.title ?? null, artists: meta?.artists ?? [], isrc: meta?.isrc ?? null };
        }),
      });
    },
  );

  server.tool(
    'tidal_mix_history_record',
    'Record tracks in the Tagesmix history so that tidal_mix_candidates and tidal_library_check block them (incl. other versions of the same songs) for 365 days. Pass the playlistId of a finished Tagesmix and/or trackIds. Recording the same tracks again only updates the date.',
    {
      playlistId: z.string().optional().describe('Playlist whose tracks are recorded'),
      trackIds: z.array(z.string()).max(500).optional().describe('Additional track IDs to record'),
    },
    { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: true, title: 'Record Tagesmix History' },
    async ({ playlistId, trackIds }, extra) => {
      const started = Date.now();
      const ctx = await getContext(extractToken(extra), started);
      let ids = dedupe(trackIds ?? []);
      if (playlistId) ids = dedupe(ids.concat((await collectItemIds(ctx, PLAYLIST_ITEMS, playlistId)).ids));
      if (ids.length === 0) return text({ recorded: 0, note: 'Keine Tracks angegeben bzw. Playlist leer.' });
      const details = await getTrackDetails(ctx, ids, started + 45000);
      const keys = await historyRecord(
        ctx.userId,
        ids.map((id) => historyKeys(id, details.map.get(id))),
        started,
      );
      return text({
        recorded: ids.length,
        keys,
        date: isoDate(started),
        withoutDetails: ids.filter((id) => !details.map.has(id)).length,
      });
    },
  );
}
