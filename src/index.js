require("dotenv").config();

const express = require("express");
const Database = require("better-sqlite3");
const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT || 3200);
const POLL_INTERVAL_MS = 10_000;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "..", "mission-control.sqlite");
const AGENT_API_TOKEN = process.env.AGENT_API_TOKEN || "";
const PM2_BIN = "/home/hermes/.local/node_modules/.bin/pm2";
const CRON_JOBS_PATH = "/home/hermes/.hermes/cron/jobs.json";
const AGENT_EVENT_TYPES = new Set([
  "terminal",
  "codex",
  "food",
  "deploy",
  "subagent_start",
  "subagent_end",
  "error",
  "cron",
  "backup",
]);
const AGENT_EVENT_SOURCES = new Set(["wex", "subagent"]);

const repos = [
  { name: "debt-tracker", path: "/home/hermes/debt-tracker" },
  { name: "food-tracker", path: "/home/hermes/food-tracker" },
];

const backupDirs = [
  { app: "food-tracker", path: "/home/hermes/backups/food-tracker/" },
  { app: "debt-tracker", path: "/home/hermes/backups/debt-tracker/" },
];

const apps = [
  { app: "debt-tracker", url: "http://localhost:3000/api/state", port: 3000 },
  { app: "food-tracker", url: "http://localhost:3100/api/state", port: 3100 },
];

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE IF NOT EXISTS processes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    pid INTEGER,
    status TEXT,
    uptime INTEGER,
    restarts INTEGER,
    memory_mb REAL,
    cpu REAL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS git_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo TEXT NOT NULL,
    commit_hash TEXT NOT NULL,
    message TEXT,
    date TEXT,
    author TEXT,
    captured_at TEXT NOT NULL,
    UNIQUE(repo, commit_hash)
  );

  CREATE TABLE IF NOT EXISTS backups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app TEXT NOT NULL,
    filepath TEXT NOT NULL UNIQUE,
    filename TEXT,
    size_bytes INTEGER,
    modified_at TEXT,
    age_hours REAL,
    checked_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS cron_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL UNIQUE,
    name TEXT,
    schedule TEXT,
    last_run_at TEXT,
    last_status TEXT,
    next_run_at TEXT,
    enabled INTEGER,
    checked_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS app_health (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    status TEXT NOT NULL,
    response_time_ms INTEGER,
    checked_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS food_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    calories REAL,
    protein REAL,
    foods_count INTEGER,
    weight REAL,
    captured_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS debt_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    total_debt REAL,
    available_spend REAL,
    active_debts INTEGER,
    captured_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS agent_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
  );
`);

const statements = {
  upsertProcess: db.prepare(`
    INSERT INTO processes (name, pid, status, uptime, restarts, memory_mb, cpu, updated_at)
    VALUES (@name, @pid, @status, @uptime, @restarts, @memory_mb, @cpu, @updated_at)
    ON CONFLICT(name) DO UPDATE SET
      pid=excluded.pid,
      status=excluded.status,
      uptime=excluded.uptime,
      restarts=excluded.restarts,
      memory_mb=excluded.memory_mb,
      cpu=excluded.cpu,
      updated_at=excluded.updated_at
  `),
  deleteGitRepo: db.prepare("DELETE FROM git_activity WHERE repo = ?"),
  insertGit: db.prepare(`
    INSERT OR REPLACE INTO git_activity (repo, commit_hash, message, date, author, captured_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `),
  deleteBackupsByApp: db.prepare("DELETE FROM backups WHERE app = ?"),
  upsertBackup: db.prepare(`
    INSERT INTO backups (app, filepath, filename, size_bytes, modified_at, age_hours, checked_at)
    VALUES (@app, @filepath, @filename, @size_bytes, @modified_at, @age_hours, @checked_at)
    ON CONFLICT(filepath) DO UPDATE SET
      app=excluded.app,
      filename=excluded.filename,
      size_bytes=excluded.size_bytes,
      modified_at=excluded.modified_at,
      age_hours=excluded.age_hours,
      checked_at=excluded.checked_at
  `),
  deleteCronMissing: db.prepare("DELETE FROM cron_jobs WHERE checked_at != ?"),
  upsertCron: db.prepare(`
    INSERT INTO cron_jobs (job_id, name, schedule, last_run_at, last_status, next_run_at, enabled, checked_at)
    VALUES (@job_id, @name, @schedule, @last_run_at, @last_status, @next_run_at, @enabled, @checked_at)
    ON CONFLICT(job_id) DO UPDATE SET
      name=excluded.name,
      schedule=excluded.schedule,
      last_run_at=excluded.last_run_at,
      last_status=excluded.last_status,
      next_run_at=excluded.next_run_at,
      enabled=excluded.enabled,
      checked_at=excluded.checked_at
  `),
  upsertHealth: db.prepare(`
    INSERT INTO app_health (app, url, status, response_time_ms, checked_at)
    VALUES (@app, @url, @status, @response_time_ms, @checked_at)
    ON CONFLICT(app) DO UPDATE SET
      url=excluded.url,
      status=excluded.status,
      response_time_ms=excluded.response_time_ms,
      checked_at=excluded.checked_at
  `),
  insertFood: db.prepare(`
    INSERT INTO food_activity (calories, protein, foods_count, weight, captured_at)
    VALUES (?, ?, ?, ?, ?)
  `),
  trimFood: db.prepare("DELETE FROM food_activity WHERE id NOT IN (SELECT id FROM food_activity ORDER BY captured_at DESC, id DESC LIMIT 100)"),
  insertDebt: db.prepare(`
    INSERT INTO debt_activity (total_debt, available_spend, active_debts, captured_at)
    VALUES (?, ?, ?, ?)
  `),
  trimDebt: db.prepare("DELETE FROM debt_activity WHERE id NOT IN (SELECT id FROM debt_activity ORDER BY captured_at DESC, id DESC LIMIT 100)"),
  insertAgentEvent: db.prepare(`
    INSERT INTO agent_events (type, description, source, created_at)
    VALUES (@type, @description, @source, @created_at)
  `),
  trimAgentEvents: db.prepare("DELETE FROM agent_events WHERE id NOT IN (SELECT id FROM agent_events ORDER BY created_at DESC, id DESC LIMIT 200)"),
};

function nowIso() {
  return new Date().toISOString();
}

function numberOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function textOrNull(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") return String(value);
  return JSON.stringify(value);
}

function cleanText(value, maxLength) {
  if (typeof value !== "string") return "";
  return value.trim().slice(0, maxLength);
}

function requireAgentToken(req, res, next) {
  if (!AGENT_API_TOKEN) {
    res.status(503).json({ error: "AGENT_API_TOKEN is not configured" });
    return;
  }

  const header = req.get("authorization") || "";
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match || match[1] !== AGENT_API_TOKEN) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  next();
}

function cronScheduleText(value) {
  if (value === undefined || value === null || value === "") return "";
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === "object") return textOrNull(parsed.expr || parsed.display) || value;
    } catch {
      return value;
    }
    return value;
  }
  if (typeof value === "object") return textOrNull(value.expr || value.display) || textOrNull(value) || "";
  return textOrNull(value) || "";
}

function findNumber(source, keys) {
  for (const key of keys) {
    const value = getDeepValue(source, key);
    if (value !== undefined && value !== null && value !== "") return numberOrNull(value);
  }
  return null;
}

function getDeepValue(source, key) {
  if (!source || typeof source !== "object") return undefined;
  if (Object.prototype.hasOwnProperty.call(source, key)) return source[key];

  const parts = key.split(".");
  let current = source;
  for (const part of parts) {
    if (!current || typeof current !== "object") return undefined;
    current = current[part];
  }
  return current;
}

function countActiveDebts(state) {
  const direct = findNumber(state, ["active_debts", "activeDebts", "summary.active_debts", "summary.activeDebts"]);
  if (direct !== null) return direct;

  const debts = state && (state.summary?.debts || state.debts || state.data?.debts);
  if (!Array.isArray(debts)) return null;
  return debts.filter((debt) => String(debt.status || "").toLowerCase() === "active").length;
}

function countFoods(state) {
  const direct = findNumber(state, ["foods_count", "foodsCount", "today.foods_count", "today.foodsCount"]);
  if (direct !== null) return direct;

  const foods = state && (
    state.summary?.today_foods ||
    state.today_foods ||
    state.foods ||
    state.entries ||
    state.logs ||
    state.today?.foods ||
    state.today?.entries
  );
  return Array.isArray(foods) ? foods.length : null;
}

function pollPm2(timestamp) {
  try {
    const output = execSync(`${PM2_BIN} jlist`, { encoding: "utf8", timeout: 5000 });
    const processes = JSON.parse(output);
    for (const processInfo of processes) {
      statements.upsertProcess.run({
        name: processInfo.name || String(processInfo.pm_id),
        pid: processInfo.pid || null,
        status: processInfo.pm2_env?.status || "unknown",
        uptime: processInfo.pm2_env?.pm_uptime || null,
        restarts: processInfo.pm2_env?.restart_time || 0,
        memory_mb: processInfo.monit?.memory ? processInfo.monit.memory / 1024 / 1024 : null,
        cpu: numberOrNull(processInfo.monit?.cpu),
        updated_at: timestamp,
      });
    }
  } catch (error) {
    statements.upsertProcess.run({
      name: "pm2",
      pid: null,
      status: "unknown",
      uptime: null,
      restarts: null,
      memory_mb: null,
      cpu: null,
      updated_at: timestamp,
    });
    console.error("PM2 poll failed:", error.message);
  }
}

function pollGit(timestamp) {
  for (const repo of repos) {
    try {
      const output = execSync("git log --oneline -5 --format='%h|%s|%ci|%an'", {
        cwd: repo.path,
        encoding: "utf8",
        timeout: 5000,
      }).trim();

      const rows = output ? output.split("\n") : [];
      const replaceRepoRows = db.transaction(() => {
        statements.deleteGitRepo.run(repo.name);
        for (const row of rows) {
          const [hash, message, date, author] = row.split("|");
          statements.insertGit.run(repo.name, hash || "", message || "", date || null, author || "", timestamp);
        }
      });
      replaceRepoRows();
    } catch (error) {
      console.error(`Git poll failed for ${repo.name}:`, error.message);
    }
  }
}

function pollBackups(timestamp) {
  for (const backupDir of backupDirs) {
    const replaceBackupRows = db.transaction(() => {
      statements.deleteBackupsByApp.run(backupDir.app);

      if (!fs.existsSync(backupDir.path)) {
        statements.upsertBackup.run({
          app: backupDir.app,
          filepath: backupDir.path,
          filename: "(missing directory)",
          size_bytes: null,
          modified_at: null,
          age_hours: null,
          checked_at: timestamp,
        });
        return;
      }

      const files = fs.readdirSync(backupDir.path, { withFileTypes: true }).filter((entry) => entry.isFile());
      if (files.length === 0) {
        statements.upsertBackup.run({
          app: backupDir.app,
          filepath: backupDir.path,
          filename: "(no backups found)",
          size_bytes: null,
          modified_at: null,
          age_hours: null,
          checked_at: timestamp,
        });
        return;
      }

      for (const file of files) {
        const filepath = path.join(backupDir.path, file.name);
        const stat = fs.statSync(filepath);
        statements.upsertBackup.run({
          app: backupDir.app,
          filepath,
          filename: file.name,
          size_bytes: stat.size,
          modified_at: stat.mtime.toISOString(),
          age_hours: (Date.now() - stat.mtime.getTime()) / 36e5,
          checked_at: timestamp,
        });
      }
    });
    replaceBackupRows();
  }
}

function normalizeCronJobs(json) {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json.jobs)) return json.jobs;
  if (json.jobs && typeof json.jobs === "object") return Object.entries(json.jobs).map(([id, job]) => ({ job_id: id, ...job }));
  if (json && typeof json === "object") return Object.entries(json).map(([id, job]) => ({ job_id: id, ...job }));
  return [];
}

function pollCron(timestamp) {
  try {
    if (!fs.existsSync(CRON_JOBS_PATH)) return;
    const jobs = normalizeCronJobs(JSON.parse(fs.readFileSync(CRON_JOBS_PATH, "utf8")));
    for (const job of jobs) {
      statements.upsertCron.run({
        job_id: textOrNull(job.job_id || job.id || job.name) || "unknown",
        name: textOrNull(job.name || job.title || job.job_id || job.id) || "Unnamed job",
        schedule: cronScheduleText(job.schedule || job.cron),
        last_run_at: textOrNull(job.last_run_at || job.lastRunAt),
        last_status: textOrNull(job.last_status || job.lastStatus),
        next_run_at: textOrNull(job.next_run_at || job.nextRunAt),
        enabled: job.enabled === false ? 0 : 1,
        checked_at: timestamp,
      });
    }
    statements.deleteCronMissing.run(timestamp);
  } catch (error) {
    console.error("Cron poll failed:", error.message);
  }
}

async function fetchJsonHealth(appConfig, timestamp) {
  const started = Date.now();
  try {
    const response = await fetch(appConfig.url, { signal: AbortSignal.timeout(5000) });
    const text = await response.text();
    const responseTime = Date.now() - started;
    const json = JSON.parse(text);
    const status = response.ok && json && typeof json === "object" ? "up" : "down";

    statements.upsertHealth.run({
      app: appConfig.app,
      url: appConfig.url,
      status,
      response_time_ms: responseTime,
      checked_at: timestamp,
    });

    return status === "up" ? json : null;
  } catch (error) {
    statements.upsertHealth.run({
      app: appConfig.app,
      url: appConfig.url,
      status: "down",
      response_time_ms: Date.now() - started,
      checked_at: timestamp,
    });
    return null;
  }
}

function recordFoodActivity(state, timestamp) {
  if (!state) return;
  statements.insertFood.run(
    findNumber(state, ["summary.totals.calories", "today.calories", "calories", "daily.calories", "summary.calories", "totals.calories"]),
    findNumber(state, ["summary.totals.protein", "today.protein", "protein", "daily.protein", "summary.protein", "totals.protein"]),
    countFoods(state),
    findNumber(state, ["summary.weight.latest.weight", "weight", "current_weight", "currentWeight", "profile.weight", "today.weight"]),
    timestamp,
  );
  statements.trimFood.run();
}

function recordDebtActivity(state, timestamp) {
  if (!state) return;
  statements.insertDebt.run(
    findNumber(state, ["total_debt", "totalDebt", "summary.total_debt", "summary.totalDebt"]),
    findNumber(state, ["available_spend", "availableSpend", "summary.available_spend", "summary.availableSpend"]),
    countActiveDebts(state),
    timestamp,
  );
  statements.trimDebt.run();
}

let polling = false;
let lastUpdated = null;

async function pollAll() {
  if (polling) return;
  polling = true;
  const timestamp = nowIso();
  try {
    pollPm2(timestamp);
    pollGit(timestamp);
    pollBackups(timestamp);
    pollCron(timestamp);

    const healthResults = await Promise.all(apps.map((appConfig) => fetchJsonHealth(appConfig, timestamp)));
    recordDebtActivity(healthResults[0], timestamp);
    recordFoodActivity(healthResults[1], timestamp);
    lastUpdated = timestamp;
  } catch (error) {
    console.error("Poll failed:", error.message);
  } finally {
    polling = false;
  }
}

function rows(sql, ...params) {
  return db.prepare(sql).all(...params);
}

function row(sql, ...params) {
  return db.prepare(sql).get(...params);
}

function getState() {
  const cronJobs = rows("SELECT * FROM cron_jobs ORDER BY name").map((job) => ({
    ...job,
    schedule: cronScheduleText(job.schedule),
  }));

  return {
    server_time: nowIso(),
    processes: rows("SELECT * FROM processes ORDER BY name"),
    git_activity: rows("SELECT * FROM git_activity ORDER BY repo, date DESC"),
    backups: rows("SELECT * FROM backups ORDER BY app, modified_at DESC"),
    cron_jobs: cronJobs,
    app_health: rows("SELECT *, CAST(substr(url, instr(url, ':') + 1) AS INTEGER) AS port FROM app_health ORDER BY app"),
    food_activity: row(`
      SELECT * FROM food_activity
      ORDER BY
        (calories IS NULL AND protein IS NULL AND foods_count IS NULL AND weight IS NULL),
        captured_at DESC,
        id DESC
      LIMIT 1
    `) || null,
    debt_activity: row(`
      SELECT * FROM debt_activity
      ORDER BY
        (active_debts IS NULL),
        captured_at DESC,
        id DESC
      LIMIT 1
    `) || null,
    last_updated: lastUpdated,
  };
}

function getEvents() {
  const events = [
    ...rows(`
      SELECT created_at AS timestamp, source, type, description
      FROM agent_events
      ORDER BY created_at DESC, id DESC
      LIMIT 200
    `),
    ...rows(`
      SELECT updated_at AS timestamp, 'pm2' AS source,
        name || ' is ' || COALESCE(status, 'unknown') || ' (restarts: ' || COALESCE(restarts, 0) || ')' AS description
      FROM processes
    `),
    ...rows(`
      SELECT captured_at AS timestamp, repo AS source,
        commit_hash || ' ' || message AS description
      FROM git_activity
    `),
    ...rows(`
      SELECT checked_at AS timestamp, app || ' backups' AS source,
        filename || CASE
          WHEN age_hours IS NULL THEN ' missing'
          ELSE ' age ' || ROUND(age_hours, 1) || 'h'
        END AS description
      FROM backups
    `),
    ...rows(`
      SELECT checked_at AS timestamp, 'cron' AS source,
        name || ' last status ' || COALESCE(last_status, 'unknown') AS description
      FROM cron_jobs
    `),
    ...rows(`
      SELECT checked_at AS timestamp, app AS source,
        'health ' || status || ' in ' || COALESCE(response_time_ms, 0) || 'ms' AS description
      FROM app_health
    `),
    ...rows(`
      SELECT captured_at AS timestamp, 'food-tracker' AS source,
        'calories ' || COALESCE(calories, 0) || ', protein ' || COALESCE(protein, 0) || ', foods ' || COALESCE(foods_count, 0) AS description
      FROM food_activity
      ORDER BY captured_at DESC, id DESC
      LIMIT 20
    `),
    ...rows(`
      SELECT captured_at AS timestamp, 'debt-tracker' AS source,
        'debt ' || COALESCE(total_debt, 0) || ', available ' || COALESCE(available_spend, 0) || ', active debts ' || COALESCE(active_debts, 0) AS description
      FROM debt_activity
      ORDER BY captured_at DESC, id DESC
      LIMIT 20
    `),
  ];

  return events
    .filter((event) => event.timestamp)
    .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
    .slice(0, 50);
}

function getAgentEvents(limit = 50) {
  return rows(`
    SELECT id, type, description, source, created_at
    FROM agent_events
    ORDER BY created_at DESC, id DESC
    LIMIT ?
  `, limit);
}

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "16kb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

app.get("/api/state", (req, res) => {
  res.json(getState());
});

app.get("/api/events", (req, res) => {
  res.json({ server_time: nowIso(), events: getEvents() });
});

app.get("/api/agent-events", (req, res) => {
  res.json({ server_time: nowIso(), events: getAgentEvents(50) });
});

app.post("/api/events", requireAgentToken, (req, res) => {
  const type = cleanText(req.body?.type, 40);
  const description = cleanText(req.body?.description, 500);
  const source = cleanText(req.body?.source, 40);

  if (!AGENT_EVENT_TYPES.has(type)) {
    res.status(400).json({ error: "Invalid event type" });
    return;
  }
  if (!AGENT_EVENT_SOURCES.has(source)) {
    res.status(400).json({ error: "Invalid event source" });
    return;
  }
  if (!description) {
    res.status(400).json({ error: "description is required" });
    return;
  }

  const createdAt = nowIso();
  const saveEvent = db.transaction(() => {
    const result = statements.insertAgentEvent.run({ type, description, source, created_at: createdAt });
    statements.trimAgentEvents.run();
    return result.lastInsertRowid;
  });
  const id = saveEvent();

  res.status(201).json({
    event: { id, type, description, source, created_at: createdAt },
  });
});

app.listen(PORT, () => {
  console.log(`Mission Control listening on http://localhost:${PORT}`);
});

pollAll();
setInterval(pollAll, POLL_INTERVAL_MS);
