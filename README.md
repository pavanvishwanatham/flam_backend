# flam_backend
🧩 QueueCTL — CLI-Based Background Job Queue System
🚀 Tech Stack

Language: Python 3.9+
Database: SQLite (built-in, no setup required)
Interface: Command-Line Interface (CLI)


🎯 Objective

QueueCTL is a CLI-based background job queue system that can enqueue tasks, execute them with workers, handle retries with exponential backoff, and move failed jobs into a Dead Letter Queue (DLQ) after exhausting retries.



🧱 Project Structure

queuectl/
├── main.py              # Entry point

├── cli.py               # CLI command handling

├── db.py                # SQLite storage logic

├── worker.py            # Worker & retry handling

├── utils.py             # Timestamp helpers

└── README.md            # Documentation


🧩 Job Specification

Each job follows the structure below:

{
  "id": "unique-job-id",
  "command": "echo 'Hello World'",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-04T10:30:00Z",
  "updated_at": "2025-11-04T10:30:00Z"
}



🧠 Job Lifecycle
State	Description
pending	Waiting to be picked up by a worker
processing	Currently being executed
completed	Successfully executed
failed	Failed, retryable
dead	Permanently failed → moved to DLQ


⚙️ Installation & Setup


1️⃣ Clone the repository

git clone https://github.com/<your-username>/queuectl.git
cd queuectl


2️⃣ Ensure Python 3.9+ is installed

SQLite comes pre-installed with Python, so no database setup is needed.


3️⃣ Run the CLI
python main.py


💻 Usage Examples
🟢 Enqueue jobs

python main.py enqueue "{\"id\":\"job1\",\"command\":\"echo Hello World\"}"
python main.py enqueue "{\"id\":\"job2\",\"command\":\"sleep 2\"}"


⚙️ Start workers
python main.py worker start --count 2


🛑 Stop workers
python main.py worker stop


📊 Check status
python main.py status


Example output
{
  "pending": 0,
  "processing": 0,
  "completed": 3,
  "failed": 0,
  "dead": 1,
  "active_workers": 0
}


📋 List jobs
python main.py list
python main.py list --state completed


💀 Dead Letter Queue
python main.py dlq list
python main.py dlq retry bad1


⚙️ Manage configuration
python main.py config set max-retries 3
python main.py config get max-retries
python main.py config set backoff_base 2
python main.py config get backoff_base


🧪 Expected Test Scenarios
#	Scenario	Expected Behavior
1	Job executes successfully	State → completed
2	Job fails repeatedly	Retries (exponential delay) → DLQ
3	Multiple workers	Parallel job execution, no duplicates
4	Restart system	Jobs persist from SQLite
5	Retry from DLQ	Moved back to pending, reprocessed


🧮 Exponential Backoff Formula
delay = base ^ attempts


Example: for base = 2

1st retry → 2 s

2nd retry → 4 s

3rd retry → 8 s


| Command                                 | Sample Output                             |
| --------------------------------------- | ----------------------------------------- |
| `python main.py enqueue ...`            | `job1`                                    |
| `python main.py worker start --count 1` | `[Worker-0] Processing job1 → echo Hello` |
| `python main.py dlq list`               | Shows job with `"state": "dead"`          |
| `python main.py status`                 | JSON summary of job states                |


