import argparse
import json
import sys
from db import Database
from worker import WorkerManager

db = Database()
wm = WorkerManager(db)

def normalize_input_json(s: str) -> str:
    
    s = s.strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1]
    return s

def main_cli():
    parser = argparse.ArgumentParser(prog="queuectl", description="QueueCTL - Background Job Queue CLI")
    sub = parser.add_subparsers(dest="cmd")

    
    p_enq = sub.add_parser("enqueue")
    p_enq.add_argument("job_json", help="Job JSON as a single argument, e.g. '{\"id\":\"job1\",\"command\":\"sleep 2\"}'")

    
    p_worker = sub.add_parser("worker")
    wsub = p_worker.add_subparsers(dest="action")
    p_ws = wsub.add_parser("start")
    p_ws.add_argument("--count", type=int, default=1)
    wsub.add_parser("stop")

   
    sub.add_parser("status")

    
    p_list = sub.add_parser("list")
    p_list.add_argument("--state", default=None)

    
    p_dlq = sub.add_parser("dlq")
    dlq_sub = p_dlq.add_subparsers(dest="action")
    dlq_sub.add_parser("list")
    pdlq_retry = dlq_sub.add_parser("retry")
    pdlq_retry.add_argument("job_id")

    
    p_cfg = sub.add_parser("config")
    cfg_sub = p_cfg.add_subparsers(dest="action")
    p_set = cfg_sub.add_parser("set")
    p_set.add_argument("key")
    p_set.add_argument("val")
    p_get = cfg_sub.add_parser("get")
    p_get.add_argument("key")

    args = parser.parse_args()

    if args.cmd == "enqueue":
        raw = normalize_input_json(args.job_json)
        try:
            job = json.loads(raw)
        except Exception as e:
            print("Invalid JSON:", e)
            sys.exit(1)
        
        if "id" not in job or "command" not in job:
            print("Invalid job: 'id' and 'command' are required.")
            sys.exit(1)
        db.enqueue_job(job)
    elif args.cmd == "worker":
        if args.action == "start":
            wm.start_workers(args.count)
        elif args.action == "stop":
            wm.stop_workers()
        else:
            parser.print_help()
    elif args.cmd == "status":
        db.print_status()
    elif args.cmd == "list":
        db.list_jobs(args.state)
    elif args.cmd == "dlq":
        if args.action == "list":
            db.list_dlq()
        elif args.action == "retry":
            db.retry_dlq(args.job_id)
        else:
            parser.print_help()
    elif args.cmd == "config":
        if args.action == "set":
            db.set_config(args.key, args.val)
        elif args.action == "get":
            db.get_config(args.key)
        else:
            parser.print_help()
    else:
        parser.print_help()
