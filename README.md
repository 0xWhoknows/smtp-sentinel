# SMTP Sentinel

A fast, async SMTP credential tester with a real-time web dashboard. Test hundreds of SMTP servers concurrently and instantly see which credentials are valid.

---

## Features

- **High concurrency** — test up to 200 SMTP servers simultaneously using async I/O
- **Real-time dashboard** — live results streamed via WebSocket with charts and progress tracking
- **Multi-port support** — handles port 465 (SSL), 587 (STARTTLS), and plain 25
- **Streaming architecture** — memory-efficient processing, no storing all results in RAM
- **CSV export** — download results after each test run
- **Stop/resume** — cancel a running test at any time
- **Graceful TLS handling** — auto-detects STARTTLS support per server

---

## Requirements

- Python 3.9+
- [FastAPI](https://fastapi.tiangolo.com/)
- [aiosmtplib](https://aiosmtplib.readthedocs.io/)
- [uvicorn](https://www.uvicorn.org/)

---

## Installation

```bash
git clone https://github.com/0xWhoknows/smtp-sentinel.git
cd smtp-sentinel
pip install fastapi aiosmtplib uvicorn
```

---

## Usage

### Start the server

```bash
python app.py
```

Then open your browser at `http://localhost:5000`.

### Input format

Paste your SMTP credentials into the dashboard using pipe-delimited format, one per line:

```
host|port|user|pass
mail.example.com|587|user@example.com|password123
smtp.provider.net|465|admin@provider.net|s3cr3t
```

Lines starting with `#` and blank lines are ignored.

### Configuration

| Setting | Default | Max |
|---|---|---|
| Concurrency | 100 | 200 |
| Timeout (seconds) | 5 | 30 |

---

## How It Works

1. Credentials are parsed and dispatched as async tasks via `aiosmtplib`
2. A `BoundedSemaphore` caps the number of active connections
3. Results are streamed back to the browser over a WebSocket as each test completes
4. Successful credentials are displayed in a separate table and can be copied or downloaded as CSV

### Result statuses

| Status | Meaning |
|---|---|
| `success` | Login and test email sent successfully |
| `auth_failed` | Server reachable but credentials rejected |
| `connection_failed` | Could not connect to the server |
| `timeout` | Server did not respond within the timeout |
| `error` | Unexpected error (shown with details) |

---

## Project Structure

```
smtp-sentinel/
├── app.py           # FastAPI app, WebSocket handling, HTML frontend
├── smtp_tester.py   # Async SMTP testing engine
└── results/         # Auto-created folder for CSV exports
```

---

