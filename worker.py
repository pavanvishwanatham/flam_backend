import subprocess
import time
import os
import signal
from multiprocessing import Process
from db import Database
from datetime import datetime, timezone
import json

PID_FILE = "queuectl_workers.pid"

class WorkerManager:
    def __init__(self, db: Database):
        self.db = db

    def start_workers(self, count=1):
        procs = []
        for i in range(count):
            p = Process(target=self._worker_loop, args=(i,))
            p.start()
            procs.append(p)
        with open(PID_FILE, "w") as f:
            f.write(",".join(str(p.pid) for p in procs))
        print(f"Started {len(procs)} worker(s)")
        for p in procs:
            p.join()

    def stop_workers(self):
        if not os.path.exists(PID_FILE):
            print("No workers running")
            return
        with open(PID_FILE) as f:
            pids = [int(x) for x in f.read().split(",") if x.strip()]
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        os.remove(PID_FILE)
        print("Stopped workers")

    def _worker_loop(self, idx):
        
        def _handler(sig, frame):
            raise SystemExit()
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

        db = Database()
        while True:
            job = db.claim_job()
            if not job:
                time.sleep(float(db.get_config("poll_interval", "1")))
                continue
            job_id = job["id"]
            cmd = job["command"]
            
            try:
                proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                output = (proc.stdout or "") + (("\nERR:\n"+proc.stderr) if proc.stderr else "")
                if proc.returncode == 0:
                    db.finish_success(job_id, output.strip())
                else:
                    
                    backoff = float(db.get_config("backoff_base", "2"))
                    db.finish_failure(job, output.strip(), backoff_base=backoff)
            except SystemExit:
                
                break
            except Exception as e:
                db.finish_failure(job, str(e)[:200], backoff_base=float(db.get_config("backoff_base", "2")))
