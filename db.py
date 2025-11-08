import sqlite3
import json
from datetime import datetime, timezone
import os

DB_PATH = os.environ.get("QUEUECTL_DB", "queuectl.db")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

class Database:
    def __init__(self, path=DB_PATH):
        self.path = path
        self._init_db()

    def conn(self):
        c = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL;")
        return c

    def _init_db(self):
        con = self.conn()
        cur = con.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            created_at TEXT,
            updated_at TEXT,
            next_attempt_at TEXT,
            output TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_state_next ON jobs (state, next_attempt_at);
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            val TEXT
        );
        """)
        
        cur.execute("INSERT OR IGNORE INTO config (key,val) VALUES ('backoff_base','2'), ('poll_interval','1')")
        con.commit()
        con.close()

    
    def enqueue_job(self, job):
        con = self.conn(); cur = con.cursor()
        created = now_iso()
        cur.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?)
        """, (job["id"], job["command"], "pending", job.get("attempts", 0), job.get("max_retries", 3), created, created))
        con.commit(); con.close()
        print(job["id"])

    
    def claim_job(self):
        con = self.conn(); cur = con.cursor()
        now = now_iso()
        
        cur.execute("BEGIN;")
        cur.execute("""
            SELECT id FROM jobs
            WHERE state='pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY created_at LIMIT 1
        """, (now,))
        row = cur.fetchone()
        if not row:
            cur.execute("COMMIT;"); con.close(); return None
        job_id = row["id"]
        updated = now_iso()
        cur.execute("UPDATE jobs SET state='processing', updated_at=? WHERE id=? AND state='pending'", (updated, job_id))
        if cur.rowcount != 1:
            cur.execute("ROLLBACK;"); con.close(); return None
        cur.execute("COMMIT;")
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = cur.fetchone()
        con.close()
        return dict(job)

    def finish_success(self, job_id, output):
        con = self.conn(); cur = con.cursor()
        cur.execute("UPDATE jobs SET state='completed', updated_at=?, output=? WHERE id=?", (now_iso(), output, job_id))
        con.commit(); con.close()

    def finish_failure(self, job_row, output, backoff_base=2.0):
        con = self.conn(); cur = con.cursor()
        attempts = job_row["attempts"] + 1
        maxr = job_row["max_retries"]
        if attempts >= maxr:
            cur.execute("UPDATE jobs SET state='dead', attempts=?, updated_at=?, output=? WHERE id=?", (attempts, now_iso(), output, job_row["id"]))
        else:
            delay = (float(backoff_base) ** attempts)
            next_ts = (datetime.now(timezone.utc) + timedelta_seconds(delay)).isoformat()
            cur.execute("UPDATE jobs SET state='pending', attempts=?, next_attempt_at=?, updated_at=?, output=? WHERE id=?", (attempts, next_ts, now_iso(), output, job_row["id"]))
        con.commit(); con.close()

    def get_config(self, key, default=None):
        con = self.conn(); cur = con.cursor()
        cur.execute("SELECT val FROM config WHERE key=?", (key,))
        r = cur.fetchone(); con.close()
        return r["val"] if r else default

    def set_config(self, key, val):
        con = self.conn(); cur = con.cursor()
        cur.execute("INSERT OR REPLACE INTO config (key,val) VALUES (?,?)", (key, str(val)))
        con.commit(); con.close()
        print("OK")

    
    def list_jobs(self, state=None):
        con = self.conn(); cur = con.cursor()
        if state:
            cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at", (state,))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY created_at")
        rows = cur.fetchall(); con.close()
        for r in rows:
            print(json.dumps(dict(r), default=str))

    def list_dlq(self):
        con = self.conn(); cur = con.cursor()
        cur.execute("SELECT * FROM jobs WHERE state='dead' ORDER BY updated_at")
        rows = cur.fetchall(); con.close()
        for r in rows:
            print(json.dumps(dict(r), default=str))

    def retry_dlq(self, job_id):
        con = self.conn(); cur = con.cursor()
        cur.execute("SELECT state FROM jobs WHERE id=?", (job_id,))
        r = cur.fetchone()
        if not r:
            print("JOB NOT FOUND"); con.close(); return
        if r["state"] != "dead":
            print("JOB NOT IN DLQ"); con.close(); return
        cur.execute("UPDATE jobs SET state='pending', attempts=0, next_attempt_at=NULL, updated_at=? WHERE id=?", (now_iso(), job_id))
        con.commit(); con.close()
        print("OK")

    def print_status(self):
        con = self.conn(); cur = con.cursor()
        stats = {}
        for s in ("pending","processing","completed","failed","dead"):
            cur.execute("SELECT COUNT(1) as c FROM jobs WHERE state=?", (s,))
            stats[s] = cur.fetchone()["c"]
       
        cur.execute("SELECT COUNT(DISTINCT id) as w FROM jobs WHERE state='processing'")
        stats["active_workers"] = cur.fetchone()["w"]
        con.close()
        print(json.dumps(stats, indent=2))


from datetime import datetime, timezone, timedelta
def timedelta_seconds(s):
    return timedelta(seconds=s)
