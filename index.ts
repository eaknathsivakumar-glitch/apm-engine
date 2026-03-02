/**
 * APM Engine Server
 *
 * The query layer between the UI and storage backends.
 * UI never talks to ClickHouse or Tempo directly — everything goes here.
 *
 * Endpoints map 1:1 to UI screens:
 *
 *  GET /api/services                     → service list (from ClickHouse)
 *
 *  Performance flow:
 *  GET /api/routes?service&from&to       → route list (graph hover → view details)
 *  GET /api/routes/samples?service&route&from&to  → sample list
 *  GET /api/trace/:traceId               → trace waterfall (Tempo)
 *  GET /api/sample/:traceId              → trace + exceptions combined
 *
 *  Error flow:
 *  GET /api/errors?service&from&to       → error list (fingerprinted groups)
 *  GET /api/errors/:fingerprint          → error type detail (summary + routes + timeline)
 *  GET /api/errors/:fingerprint/routes   → affected routes for this error type
 *  GET /api/errors/:fingerprint/samples?route  → route+error samples (JOIN)
 *
 *  Raw:
 *  GET /api/exceptions?service&route&traceId&limit  → raw exception rows
 *  GET /api/requests?service&route&from&to&limit    → raw request rows
 */

import express, { Request, Response, NextFunction } from "express";
import axios from "axios";

const app = express();
app.use(express.json());

// ── Config ────────────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || "4000");
const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL || "http://localhost:8123";
const CLICKHOUSE_DB = process.env.CLICKHOUSE_DB || "apm";
const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER || "default";
const CLICKHOUSE_PASS = process.env.CLICKHOUSE_PASSWORD || "apm_pass";
const TEMPO_URL = process.env.TEMPO_URL || "http://localhost:3200";
const MIMIR_URL = process.env.MIMIR_URL || "http://localhost:9009";
const DEFAULT_TENANT = process.env.DEFAULT_TENANT || "anonymous";

// ── CORS ──────────────────────────────────────────────────────────────────────
app.use((req: Request, res: Response, next: NextFunction) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type,X-Tenant-ID");
    if (req.method === "OPTIONS") { res.sendStatus(204); return; }
    next();
});

// ── ClickHouse query helper ───────────────────────────────────────────────────
async function chQuery<T = any>(sql: string, tenant: string): Promise<T[]> {
    // Inject tenant into every query for multi-tenancy safety
    const safeSql = sql.trim();
    try {
        const response = await axios.post(
            `${CLICKHOUSE_URL}/?database=${CLICKHOUSE_DB}&default_format=JSONEachRow&user=${CLICKHOUSE_USER}&password=${CLICKHOUSE_PASS}`,
            safeSql,
            { headers: { "Content-Type": "text/plain" }, timeout: 30_000 }
        );
        const data = response.data;
        if (!data) return [];
        if (typeof data === "string") {
            return data.trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) as T[];
        }
        return (Array.isArray(data) ? data : [data]) as T[];
    } catch (err: any) {
        console.error("[APM Server] ClickHouse error:", err.message, "\nSQL:", safeSql);
        throw new Error(`ClickHouse query failed: ${err.message}`);
    }
}

// ── Tempo query helper ────────────────────────────────────────────────────────
async function tempoTrace(traceId: string): Promise<any> {
    try {
        const response = await axios.get(`${TEMPO_URL}/api/traces/${traceId}`, {
            timeout: 10_000,
            validateStatus: (s: any) => s < 500,
        });
        if (response.status === 404) return null;
        return response.data;
    } catch (err: any) {
        console.error("[APM Server] Tempo error for trace", traceId, ":", err.message);
        return null; // Graceful fallback
    }
}

// ── Time helpers ──────────────────────────────────────────────────────────────
function toChTimestamp(iso: string): string {
    // ClickHouse DateTime expects 'YYYY-MM-DD HH:MM:SS'
    return iso.replace("T", " ").replace("Z", "").split(".")[0];
}

function defaultFrom(): string {
    // Default: last 1 hour
    return toChTimestamp(new Date(Date.now() - 60 * 60 * 1000).toISOString());
}

function defaultTo(): string {
    return toChTimestamp(new Date().toISOString());
}

// ── Error wrapper ─────────────────────────────────────────────────────────────
function handle(fn: (req: Request, res: Response) => Promise<void>) {
    return async (req: Request, res: Response) => {
        try {
            await fn(req, res);
        } catch (err: any) {
            console.error("[APM Server] Error:", err.message);
            res.status(500).json({ error: err.message });
        }
    };
}

function tenant(req: Request): string {
    return (req.headers["x-tenant-id"] as string) || DEFAULT_TENANT;
}

/* =============================================================
   ENDPOINTS
============================================================ */

// ── Health ────────────────────────────────────────────────────────────────────
app.get("/health", (_req, res) => {
    res.json({ status: "ok", ts: new Date().toISOString() });
});

// ── Service list ──────────────────────────────────────────────────────────────
// Used by UI to populate service filter dropdowns
app.get("/api/services", handle(async (req, res) => {
    const rows = await chQuery<{ service: string }>(
        `SELECT DISTINCT service FROM (
            SELECT service FROM requests
            UNION ALL
            SELECT service FROM spans
        ) ORDER BY service ASC LIMIT 100`,
        "anonymous"
    );
    res.json([...new Set(rows.map(r => r.service))]);
}));

// ── Route list ────────────────────────────────────────────────────────────────
// Triggered by: graph hover → "View Details"
// Returns per-route aggregates for a service in a time window
app.get("/api/routes", handle(async (req, res) => {
    const t = tenant(req);
    const service = req.query.service as string || "";
    const from = req.query.from as string || defaultFrom();
    const to = req.query.to as string || defaultTo();

    const whereService = service ? `AND service = '${service}'` : "";

    const rows = await chQuery(
        `SELECT
       route,
       service,
       count()                                         AS throughput,
       avg(duration_ms)                                AS mean_ms,
       quantile(0.50)(duration_ms)                    AS p50_ms,
       quantile(0.95)(duration_ms)                    AS p95_ms,
       quantile(0.99)(duration_ms)                    AS p99_ms,
       max(duration_ms)                                AS max_ms,
       sum(duration_ms)                                AS impact_ms,
       countIf(has_error = 1)                          AS error_count,
       countIf(has_error = 1) / count()               AS error_rate
     FROM requests
     WHERE tenant_id = '${t}'
       AND timestamp BETWEEN '${from}' AND '${to}'
       ${whereService}
     GROUP BY route, service
     ORDER BY impact_ms DESC
     LIMIT 100`,
        t
    );
    res.json(rows);
}));

// ── Sample list for a route ───────────────────────────────────────────────────
// Triggered by: clicking a route in the route list
// Returns individual request rows, slowest first
app.get("/api/routes/samples", handle(async (req, res) => {
    const t = tenant(req);
    const service = req.query.service as string || "";
    const route = req.query.route as string || "";
    const from = req.query.from as string || defaultFrom();
    const to = req.query.to as string || defaultTo();
    const limit = parseInt(req.query.limit as string || "50");

    let query = `SELECT 
       trace_id, span_id, timestamp, duration_ms, status_code, has_error
     FROM requests WHERE route = '${route}'`;

    if (service) {
        query += ` AND service = '${service}'`;
    } else {
        query += ` AND tenant_id = '${t}'`;
    }

    query += ` AND timestamp BETWEEN '${from}' AND '${to}'
      ORDER BY duration_ms DESC LIMIT ${limit}`;

    const rows = await chQuery(query, t);
    res.json(rows);
}));

// ── Trace from Tempo ──────────────────────────────────────────────────────────
// Triggered by: clicking a sample row
// Returns raw Tempo OTLP response — UI renders the waterfall
app.get("/api/trace/:traceId", handle(async (req, res) => {
    const data = await tempoTrace(req.params.traceId);
    if (!data) return void res.status(404).json({ error: "Trace not found in Tempo" });
    res.json(data);
}));

// ── Sample detail — trace + exceptions combined ───────────────────────────────
// Triggered by: clicking a sample (combines Tempo + ClickHouse in one call)
app.get("/api/sample/:traceId", handle(async (req, res) => {
    const traceId = req.params.traceId;

    const [trace, exceptions, request, spans] = await Promise.all([
        tempoTrace(traceId),

        chQuery(
            `SELECT
          type, message, stack_trace, fingerprint, timestamp, duration_ms, route
        FROM exceptions
        WHERE trace_id  = '${traceId}'
        ORDER BY timestamp ASC`,
            "anonymous"
        ),

        chQuery(
            `SELECT
          trace_id, service, route, method, status_code, duration_ms, timestamp
        FROM requests
        WHERE trace_id  = '${traceId}'
        LIMIT 1`,
            "anonymous"
        ),

        chQuery(
            `SELECT
          trace_id, span_id, parent_span_id, name, type, duration_ms, timestamp, metadata
        FROM spans
        WHERE trace_id = '${traceId}'
        ORDER BY timestamp ASC`,
            "anonymous"
        ),
    ]);

    res.json({
        trace,                       // Tempo span waterfall (legacy)
        exceptions,                  // ClickHouse exception rows for this trace
        request: request[0] || null, // Summary of the root request
        spans,                       // Granular hierarchical spans
    });
}));

// ── Error list ────────────────────────────────────────────────────────────────
// Triggered by: clicking the error rate graph
// Returns fingerprinted error groups — the error list screen
app.get("/api/errors", handle(async (req, res) => {
    const t = tenant(req);
    const service = req.query.service as string || "";
    const from = req.query.from as string || defaultFrom();
    const to = req.query.to as string || defaultTo();

    const whereService = service ? `AND service = '${service}'` : "";

    const rows = await chQuery(
        `SELECT
       fingerprint,
       type,
       any(message)             AS sample_message,
       any(service)             AS service_name,
       count()                  AS occurrences,
       countDistinct(route)     AS affected_routes,
       countDistinct(trace_id)  AS affected_requests,
       min(timestamp)           AS first_seen,
       max(timestamp)           AS last_seen,
       any(trace_id)            AS sample_trace_id
     FROM exceptions
     WHERE tenant_id = '${t}'
       AND timestamp BETWEEN '${from}' AND '${to}'
       ${whereService}
     GROUP BY fingerprint, type
     ORDER BY occurrences DESC
     LIMIT 50`,
        t
    );
    res.json(rows);
}));

// ── Error type detail ─────────────────────────────────────────────────────────
// Triggered by: clicking an error group
// Returns summary + affected routes + occurrence timeline (3 queries parallel)
app.get("/api/errors/:fingerprint", handle(async (req, res) => {
    const t = tenant(req);
    const fp = req.params.fingerprint;
    const from = req.query.from as string || defaultFrom();
    const to = req.query.to as string || defaultTo();

    const [summary, routes, timeline] = await Promise.all([

        // 1. Summary stats
        chQuery(
            `SELECT
          any(type)                AS type,
          any(message)             AS sample_message,
          count()                  AS total_occurrences,
          countDistinct(service)   AS services_affected,
          countDistinct(route)     AS routes_affected,
          min(timestamp)           AS first_seen,
          max(timestamp)           AS last_seen,
          any(trace_id)            AS sample_trace_id
        FROM exceptions
        WHERE tenant_id  = '${t}'
          AND fingerprint = '${fp}'`,
            t
        ),

        // 2. Affected routes breakdown
        chQuery(
            `SELECT
          route,
          any(service)     AS service,
          count()          AS occurrences,
          avg(duration_ms) AS mean_ms,
          max(duration_ms) AS max_ms,
          any(trace_id)    AS sample_trace_id,
          max(timestamp)   AS last_seen
        FROM exceptions
        WHERE tenant_id   = '${t}'
          AND fingerprint  = '${fp}'
          AND timestamp   BETWEEN '${from}' AND '${to}'
        GROUP BY route
        ORDER BY occurrences DESC`,
            t
        ),

        // 3. Occurrence timeline (per minute) for the mini sparkline
        chQuery(
            `SELECT
          toStartOfMinute(timestamp) AS minute,
          count()                    AS count
        FROM exceptions
        WHERE tenant_id   = '${t}'
          AND fingerprint  = '${fp}'
          AND timestamp   BETWEEN '${from}' AND '${to}'
        GROUP BY minute
        ORDER BY minute ASC`,
            t
        ),
    ]);

    res.json({
        summary: summary[0] || null,
        routes,
        timeline,
    });
}));

// ── Route+error samples ───────────────────────────────────────────────────────
// Triggered by: clicking a route in the error type detail screen
// Returns individual requests where this error happened on this route
app.get("/api/errors/:fingerprint/samples", handle(async (req, res) => {
    const t = tenant(req);
    const fp = req.params.fingerprint;
    const route = req.query.route as string || "";
    const from = req.query.from as string || defaultFrom();
    const to = req.query.to as string || defaultTo();
    const limit = parseInt(req.query.limit as string || "50");

    const whereRoute = route ? `AND r.route = '${route}'` : "";

    // JOIN exceptions → requests to get both exception detail and request timing
    const rows = await chQuery(
        `SELECT
       e.trace_id,
       e.timestamp,
       e.type,
       e.message,
       e.stack_trace,
       r.route,
       r.duration_ms,
       r.status_code,
       r.method
     FROM exceptions e
     INNER JOIN requests r
       ON e.trace_id  = r.trace_id
      AND e.tenant_id = r.tenant_id
     WHERE e.tenant_id   = '${t}'
       AND e.fingerprint  = '${fp}'
       AND e.timestamp   BETWEEN '${from}' AND '${to}'
       ${whereRoute}
     ORDER BY e.timestamp DESC
     LIMIT ${limit}`,
        t
    );
    res.json(rows);
}));

// ── Raw exceptions ────────────────────────────────────────────────────────────
// Generic query endpoint — for exception detail panels, trace-scoped lookups
app.get("/api/exceptions", handle(async (req, res) => {
    const t = tenant(req);
    const service = req.query.service as string || "";
    const limit = parseInt(req.query.limit as string || "50");

    let query = `SELECT 
       trace_id, span_id, service, route, type, message,
       stack_trace, fingerprint, duration_ms, timestamp
     FROM exceptions WHERE 1=1`;

    if (service) {
        query += ` AND service = '${service}'`;
    } else {
        query += ` AND tenant_id = '${t}'`;
    }

    query += ` ORDER BY timestamp DESC LIMIT ${limit}`;

    const rows = await chQuery(query, t);
    res.json(rows);
}));

// ── Raw requests ──────────────────────────────────────────────────────────────
// Generic query endpoint for request rows
app.get("/api/requests", handle(async (req, res) => {
    const t = tenant(req);
    const service = req.query.service as string || "";
    const limit = parseInt(req.query.limit as string || "50");

    let query = `SELECT 
       trace_id, service, route, method,
       status_code, duration_ms, has_error, timestamp
     FROM requests WHERE 1=1`;

    if (service) {
        query += ` AND service = '${service}'`;
    } else {
        query += ` AND tenant_id = '${t}'`;
    }

    query += ` ORDER BY timestamp DESC LIMIT ${limit}`;

    const rows = await chQuery(query, t);
    res.json(rows);
}));

// ── Metrics (Mimir) ──────────────────────────────────────────────────────────
// Returns time-series data for throughput and error rate
app.get("/api/metrics", handle(async (req, res) => {
    const t = tenant(req);
    const service = req.query.service as string || "";
    const step = req.query.step as string || "15s";

    // Last 30 minutes by default
    const now = Math.floor(Date.now() / 1000);
    const start = parseInt(req.query.start as string) || (now - 30 * 60);
    const end = parseInt(req.query.end as string) || now;

    // In this unified model: service = tenant = application name
    const filters = [`service="${service || t}"`];
    const filterStr = `{${filters.join(",")}}`;

    // 1. Throughput (Req/s)
    // 2. Errors (Req/s)
    // 3. Request Count (Total in the interval)
    const queries = {
        throughput: `sum(rate(http_requests_total${filterStr}[2m]))`,
        errors: `sum(rate(http_errors_total${filterStr}[2m]))`,
        count: `sum(increase(http_requests_total${filterStr}[1m]))`,
    };

    const results: any = {};
    await Promise.all(Object.entries(queries).map(async ([key, promql]) => {
        try {
            const response = await axios.get(`${MIMIR_URL}/prometheus/api/v1/query_range`, {
                params: {
                    query: promql,
                    start,
                    end,
                    step
                },
                headers: { "X-Scope-OrgID": t }
            });
            results[key] = response.data.data.result[0]?.values || [];
        } catch (err: any) {
            console.error(`[APM Server] Mimir query failed for ${key}:`, err.message);
            results[key] = [];
        }
    }));

    // Format for FE: [{ ts: number, throughput: number, errors: number, errorRate: number, count: number }]
    const formatted: any[] = [];
    const tMap: Record<number, any> = {};

    results.throughput.forEach(([ts, val]: [number, string]) => {
        const time = ts;
        tMap[time] = { ts: time, throughput: parseFloat(val), errors: 0, count: 0 };
    });

    results.errors.forEach(([ts, val]: [number, string]) => {
        const time = ts;
        if (!tMap[time]) tMap[time] = { ts: time, throughput: 0, errors: 0, count: 0 };
        tMap[time].errors = parseFloat(val);
    });

    results.count.forEach(([ts, val]: [number, string]) => {
        const time = ts;
        if (!tMap[time]) tMap[time] = { ts: time, throughput: 0, errors: 0, count: 0 };
        tMap[time].count = Math.round(parseFloat(val));
    });

    Object.values(tMap).forEach((d: any) => {
        d.errorRate = d.throughput > 0 ? (d.errors / d.throughput) * 100 : 0;
        formatted.push(d);
    });

    res.json(formatted.sort((a, b) => a.ts - b.ts));
}));

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
    console.log(`[APM Server] Listening on port ${PORT}`);
    console.log(`[APM Server] ClickHouse: ${CLICKHOUSE_URL}/${CLICKHOUSE_DB}`);
    console.log(`[APM Server] Tempo:      ${TEMPO_URL}`);
});