# app.py - Main FastAPI application for SMTP Sentinel


from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
import asyncio
import os
import uuid
from datetime import datetime
from typing import List, Dict, Optional
import json
import logging

from smtp_tester import SMTPTester

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="SMTP Sentinel")

os.makedirs('results', exist_ok=True)


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._lock = asyncio.Lock() 
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
    
    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
    
    async def send_message(self, websocket: WebSocket, message: dict):
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.warning(f"Failed to send message: {e}")
            await self.disconnect(websocket)
    
    async def broadcast(self, message: dict):
        disconnected = []
        async with self._lock:
            connections = self.active_connections.copy()
        
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        async with self._lock:
            for conn in disconnected:
                if conn in self.active_connections:
                    self.active_connections.remove(conn)


class TestState:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._current_tester: Optional[SMTPTester] = None
        self._test_task: Optional[asyncio.Task] = None
        self._is_running = False
    
    @property
    async def is_running(self) -> bool:
        async with self._lock:
            return self._is_running
    
    async def set_running(self, value: bool):
        async with self._lock:
            self._is_running = value
    
    @property
    async def current_tester(self) -> Optional[SMTPTester]:
        async with self._lock:
            return self._current_tester
    
    async def set_current_tester(self, tester: Optional[SMTPTester]):
        async with self._lock:
            self._current_tester = tester
    
    @property
    async def test_task(self) -> Optional[asyncio.Task]:
        async with self._lock:
            return self._test_task
    
    async def set_test_task(self, task: Optional[asyncio.Task]):
        async with self._lock:
            self._test_task = task
    
    async def stop_test(self):
        """Thread-safe test stopping"""
        async with self._lock:
            if self._current_tester:
                self._current_tester.stop()
            if self._test_task and not self._test_task.done():
                self._test_task.cancel()
            self._is_running = False


manager = ConnectionManager()
state = TestState()


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(content=HTML_CONTENT)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await manager.send_message(websocket, {"type": "connected", "data": "Connected"})
        
        while True:
            try:
                data = await websocket.receive_json()
                await handle_message(websocket, data)
            except WebSocketDisconnect:
                raise
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON received: {e}")
                await manager.send_message(websocket, {
                    "type": "error",
                    "error": "Invalid JSON format"
                })
            except Exception as e:
                logger.error(f"Error handling message: {e}")
                await manager.send_message(websocket, {
                    "type": "error",
                    "error": "Internal server error"
                })
                
    except WebSocketDisconnect:
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await manager.disconnect(websocket)


async def handle_message(websocket: WebSocket, data: dict):
    msg_type = data.get("type")
    
    if msg_type == "start_test":
        await handle_start_test(websocket, data)
    elif msg_type == "stop_test":
        await handle_stop_test(websocket)
    else:
        await manager.send_message(websocket, {
            "type": "error",
            "error": f"Unknown message type: {msg_type}"
        })


async def handle_start_test(websocket: WebSocket, data: dict):
    if await state.is_running:
        await manager.send_message(websocket, {
            "type": "test_error",
            "error": "Test already running"
        })
        return
    
    test_id = str(uuid.uuid4())[:8]
    
    smtp_content = data.get("smtp_list", "").strip()
    target_email = data.get("target_email", "").strip()
    concurrency = min(int(data.get("concurrency", 100)), 200)
    timeout = min(int(data.get("timeout", 5)), 30)
    
    if not smtp_content or not target_email:
        await manager.send_message(websocket, {
            "type": "test_error",
            "error": "Missing SMTP list or target email"
        })
        return
    
    temp_tester = SMTPTester()
    credentials = temp_tester.parse_content(smtp_content)
    
    if not credentials:
        await manager.send_message(websocket, {
            "type": "test_error",
            "error": "No valid SMTP credentials found"
        })
        return
    
    await manager.send_message(websocket, {
        "type": "test_started",
        "test_id": test_id,
        "total": len(credentials)
    })
    
    await state.set_running(True)
    
    task = asyncio.create_task(
        run_test_worker(credentials, target_email, test_id, concurrency, timeout),
        name=f"test_{test_id}"
    )
    await state.set_test_task(task)
    
    task.add_done_callback(lambda t: asyncio.create_task(cleanup_after_test(t)))


async def cleanup_after_test(task: asyncio.Task):
    """Cleanup function called when test task completes"""
    try:

        exc = task.exception()
        if exc and not isinstance(exc, asyncio.CancelledError):
            logger.error(f"Test task failed with exception: {exc}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")
    finally:
        await state.set_running(False)
        await state.set_current_tester(None)
        await state.set_test_task(None)


async def run_test_worker(credentials, target_email, test_id, concurrency, timeout):
    tester = SMTPTester(max_concurrent=concurrency, timeout=timeout)
    await state.set_current_tester(tester)
    
    csv_filename = f"results/smtp_results_{test_id}.csv"
    file_handle = None
    results_count = {"success": 0, "auth_failed": 0, "connection_failed": 0, "timeout": 0, "error": 0}
    
    try:
        file_handle = open(csv_filename, 'w', encoding='utf-8', newline='')
        file_handle.write("host,port,user,password,status,error,response_time,timestamp\n")
        
        async def write_result(result: dict):
            """Write result to CSV immediately to save memory"""
            row = f"{result['host']},{result['port']},{result['user']},{result['password']},{result['status']},\"{result.get('error', '')}\",{result.get('response_time', '')},{result['timestamp']}\n"
            file_handle.write(row)
            file_handle.flush()  
        
        async def progress_callback(result: dict):
            """Handle progress updates"""

            status = result['status']
            if status in results_count:
                results_count[status] += 1
            else:
                results_count['error'] += 1
            
            await write_result(result)
            
            await broadcast_progress(result)
        

        await tester.run_tests_streaming(
            credentials, 
            target_email, 
            progress_callback
        )
        
        file_handle.close()
        file_handle = None
        
        total = sum(results_count.values())
        success_count = results_count['success']
        
        successful_results = []
        try:
            with open(csv_filename, 'r', encoding='utf-8') as f:
                next(f)  
                for line in f:
                    if len(successful_results) >= 1000:  
                        break
                    parts = line.strip().split(',')
                    if len(parts) >= 5 and parts[4] == 'success':
                        successful_results.append({
                            'host': parts[0],
                            'port': parts[1],
                            'user': parts[2],
                            'password': parts[3],
                            'status': parts[4],
                            'response_time': parts[6] if len(parts) > 6 else '',
                            'timestamp': parts[7] if len(parts) > 7 else ''
                        })
        except Exception as e:
            logger.error(f"Error reading results: {e}")
        
        await manager.broadcast({
            "type": "test_complete",
            "test_id": test_id,
            "total": total,
            "success": success_count,
            "auth_failed": results_count['auth_failed'],
            "connection_failed": results_count['connection_failed'],
            "timeout": results_count['timeout'],
            "other_errors": total - success_count,
            "success_rate": round(success_count / total * 100, 1) if total else 0,
            "download_url": f"/download/{test_id}",
            "successful_results": successful_results
        })
        
    except asyncio.CancelledError:
        logger.info("Test cancelled by user")
        await manager.broadcast({
            "type": "test_stopped",
            "message": "Test stopped by user"
        })
        raise  # Re-raise to propagate cancellation
    except Exception as e:
        logger.error(f"Test error: {e}")
        await manager.broadcast({
            "type": "test_error",
            "test_id": test_id,
            "error": str(e)
        })
    finally:
        # Ensure file is closed
        if file_handle:
            try:
                file_handle.close()
            except Exception:
                pass


async def broadcast_progress(result: dict):
    await manager.broadcast({
        "type": "test_progress",
        "result": result
    })


async def handle_stop_test(websocket: WebSocket):
    await state.stop_test()
    await manager.send_message(websocket, {
        "type": "test_stopped",
        "message": "Test stopping..."
    })


@app.get("/download/{test_id}")
async def download_results(test_id: str):
    filename = f"results/smtp_results_{test_id}.csv"
    if os.path.exists(filename):
        return FileResponse(
            filename, 
            filename=f"smtp_results_{test_id}.csv",
            media_type="text/csv"
        )
    raise HTTPException(status_code=404, detail="File not found")


# HTML Content (unchanged from original)
HTML_CONTENT = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SMTP Sentinel | High-Performance Tester</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0a0f;
            --bg-secondary: #12121a;
            --bg-tertiary: #1a1a25;
            --bg-card: #161622;
            --bg-success: #064e3b;
            --accent-primary: #6366f1;
            --accent-secondary: #8b5cf6;
            --accent-success: #10b981;
            --accent-success-light: #34d399;
            --accent-warning: #f59e0b;
            --accent-danger: #ef4444;
            --accent-info: #3b82f6;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            --border-color: #27273a;
            --border-success: #059669;
            --gradient-primary: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
            --gradient-success: linear-gradient(135deg, #10b981 0%, #059669 100%);
            --gradient-danger: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
            --shadow-glow: 0 0 20px rgba(99, 102, 241, 0.3);
            --shadow-success: 0 0 20px rgba(16, 185, 129, 0.3);
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Inter', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            overflow-x: hidden;
        }

        .bg-grid {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-image: 
                linear-gradient(rgba(99, 102, 241, 0.03) 1px, transparent 1px),
                linear-gradient(90deg, rgba(99, 102, 241, 0.03) 1px, transparent 1px);
            background-size: 50px 50px;
            pointer-events: none;
            z-index: 0;
        }

        .bg-glow {
            position: fixed;
            width: 600px;
            height: 600px;
            background: radial-gradient(circle, rgba(99, 102, 241, 0.15) 0%, transparent 70%);
            top: -300px;
            right: -300px;
            pointer-events: none;
            z-index: 0;
            animation: pulse 4s ease-in-out infinite;
        }

        @keyframes pulse {
            0%, 100% { transform: scale(1); opacity: 0.5; }
            50% { transform: scale(1.2); opacity: 0.8; }
        }

        .container {
            position: relative;
            z-index: 1;
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 0;
            margin-bottom: 30px;
            border-bottom: 1px solid var(--border-color);
        }

        .logo {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .logo-icon {
            width: 45px;
            height: 45px;
            background: var(--gradient-primary);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 24px;
            box-shadow: var(--shadow-glow);
            animation: float 3s ease-in-out infinite;
        }

        @keyframes float {
            0%, 100% { transform: translateY(0); }
            50% { transform: translateY(-5px); }
        }

        .logo-text h1 {
            font-size: 24px;
            font-weight: 700;
            background: var(--gradient-primary);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        .logo-text p {
            font-size: 12px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 2px;
        }

        .connection-status {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            background: var(--bg-tertiary);
            border-radius: 20px;
            border: 1px solid var(--border-color);
            font-size: 13px;
            font-weight: 500;
            transition: all 0.3s;
        }

        .connection-status.connected {
            border-color: var(--accent-success);
            color: var(--accent-success);
            box-shadow: 0 0 10px rgba(16, 185, 129, 0.2);
        }

        .connection-status.disconnected {
            border-color: var(--accent-danger);
            color: var(--accent-danger);
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            animation: blink 2s infinite;
        }

        .connected .status-dot { background: var(--accent-success); }
        .disconnected .status-dot { background: var(--accent-danger); }

        @keyframes blink {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .main-grid {
            display: grid;
            grid-template-columns: 380px 1fr;
            gap: 24px;
            margin-bottom: 24px;
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 24px;
            backdrop-filter: blur(10px);
            transition: transform 0.3s, box-shadow 0.3s;
        }

        .card:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
        }

        .card-header {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 20px;
            font-weight: 600;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
        }

        .form-group {
            margin-bottom: 20px;
        }

        label {
            display: block;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
            margin-bottom: 8px;
        }

        textarea, input[type="email"], input[type="number"] {
            width: 100%;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            padding: 12px 16px;
            color: var(--text-primary);
            font-family: 'JetBrains Mono', monospace;
            font-size: 13px;
            transition: all 0.3s;
            outline: none;
        }

        textarea {
            min-height: 180px;
            resize: vertical;
            line-height: 1.6;
        }

        textarea:focus, input:focus {
            border-color: var(--accent-primary);
            box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
        }

        .input-row {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
        }

        .btn {
            width: 100%;
            padding: 14px 24px;
            border: none;
            border-radius: 10px;
            font-size: 14px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            cursor: pointer;
            transition: all 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }

        .btn-primary {
            background: var(--gradient-primary);
            color: white;
            box-shadow: 0 4px 15px rgba(99, 102, 241, 0.4);
        }

        .btn-primary:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: 0 6px 25px rgba(99, 102, 241, 0.6);
        }

        .btn-danger {
            background: var(--gradient-danger);
            color: white;
            box-shadow: 0 4px 15px rgba(239, 68, 68, 0.4);
        }

        .btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none !important;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 16px;
            margin-bottom: 24px;
        }

        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 20px;
            text-align: center;
            position: relative;
            overflow: hidden;
            transition: all 0.3s;
        }

        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 3px;
            background: var(--accent-color);
            opacity: 0;
            transition: opacity 0.3s;
        }

        .stat-card:hover::before {
            opacity: 1;
        }

        .stat-card:hover {
            transform: translateY(-3px);
            border-color: var(--accent-color);
        }

        .stat-card.pending { --accent-color: var(--accent-info); }
        .stat-card.success { --accent-color: var(--accent-success); }
        .stat-card.auth { --accent-color: var(--accent-danger); }
        .stat-card.conn { --accent-color: var(--accent-warning); }
        .stat-card.timeout { --accent-color: #8b5cf6; }

        .stat-value {
            font-size: 32px;
            font-weight: 700;
            color: var(--text-primary);
            margin-bottom: 4px;
            font-family: 'JetBrains Mono', monospace;
        }

        .stat-label {
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
        }

        .progress-section {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 24px;
        }

        .progress-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }

        .progress-title {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-secondary);
        }

        .progress-percent {
            font-size: 24px;
            font-weight: 700;
            color: var(--accent-primary);
            font-family: 'JetBrains Mono', monospace;
        }

        .progress-bar-bg {
            height: 8px;
            background: var(--bg-tertiary);
            border-radius: 4px;
            overflow: hidden;
            position: relative;
        }

        .progress-bar-fill {
            height: 100%;
            background: var(--gradient-primary);
            border-radius: 4px;
            transition: width 0.3s ease;
            position: relative;
            overflow: hidden;
        }

        .progress-bar-fill::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
            animation: shimmer 2s infinite;
        }

        @keyframes shimmer {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(100%); }
        }

        .progress-stats {
            display: flex;
            justify-content: space-between;
            margin-top: 12px;
            font-size: 12px;
            color: var(--text-muted);
        }

        .chart-container {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 24px;
            height: 300px;
            position: relative;
        }

        /* SUCCESS BOX - GUARANTEED VISIBLE */
        .success-container {
            background: linear-gradient(135deg, #064e3b 0%, #065f46 50%, #047857 100%);
            border: 3px solid var(--accent-success);
            border-radius: 16px;
            overflow: hidden;
            margin-bottom: 24px;
            box-shadow: 0 0 40px rgba(16, 185, 129, 0.3), inset 0 0 20px rgba(16, 185, 129, 0.1);
            display: none;
        }

        .success-container.show {
            display: block !important;
            animation: successSlideIn 0.5s ease-out;
        }

        @keyframes successSlideIn {
            from {
                opacity: 0;
                transform: translateY(-50px) scale(0.95);
            }
            to {
                opacity: 1;
                transform: translateY(0) scale(1);
            }
        }

        .success-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 24px;
            background: rgba(16, 185, 129, 0.2);
            border-bottom: 2px solid var(--accent-success);
        }

        .success-title {
            font-size: 20px;
            font-weight: 700;
            color: #6ee7b7;
            display: flex;
            align-items: center;
            gap: 12px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.3);
        }

        .success-counter {
            background: var(--accent-success);
            color: white;
            padding: 6px 16px;
            border-radius: 20px;
            font-size: 16px;
            font-family: 'JetBrains Mono', monospace;
            font-weight: 700;
            box-shadow: 0 4px 15px rgba(16, 185, 129, 0.5);
            border: 2px solid rgba(255,255,255,0.2);
        }

        .success-table-wrap {
            max-height: 500px;
            overflow-y: auto;
            overflow-x: auto;
        }

        .success-table-wrap::-webkit-scrollbar {
            width: 12px;
            height: 12px;
        }

        .success-table-wrap::-webkit-scrollbar-track {
            background: rgba(6, 78, 59, 0.8);
        }

        .success-table-wrap::-webkit-scrollbar-thumb {
            background: var(--accent-success);
            border-radius: 6px;
            border: 2px solid rgba(6, 78, 59, 0.8);
        }

        .success-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 13px;
        }

        .success-table th {
            background: rgba(5, 150, 105, 0.4);
            padding: 16px;
            text-align: left;
            font-weight: 700;
            color: #a7f3d0;
            text-transform: uppercase;
            font-size: 11px;
            letter-spacing: 1.5px;
            position: sticky;
            top: 0;
            z-index: 10;
            border-bottom: 3px solid var(--accent-success);
        }

        .success-table td {
            padding: 14px 16px;
            border-bottom: 1px solid rgba(16, 185, 129, 0.3);
            color: #ecfdf5;
            font-family: 'JetBrains Mono', monospace;
            font-weight: 500;
        }

        .success-table tr:hover td {
            background: rgba(16, 185, 129, 0.15);
        }

        .success-table tr:last-child td {
            border-bottom: none;
        }

        .pwd-cell {
            background: rgba(0,0,0,0.3);
            border-radius: 6px;
            padding: 4px 8px;
            filter: blur(6px);
            transition: all 0.3s;
            cursor: pointer;
            display: inline-block;
            border: 1px solid transparent;
        }

        .pwd-cell:hover {
            filter: blur(0);
            background: rgba(0,0,0,0.5);
            border-color: var(--accent-success);
        }

        .time-cell {
            color: #6ee7b7;
            font-weight: 600;
        }

        /* Regular Results Section */
        .results-section {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            overflow: hidden;
            margin-bottom: 24px;
        }

        .results-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 24px;
            border-bottom: 1px solid var(--border-color);
        }

        .results-title {
            font-size: 16px;
            font-weight: 600;
        }

        .download-btn {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 10px 20px;
            background: var(--gradient-success);
            color: white;
            text-decoration: none;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 600;
            transition: all 0.3s;
        }

        .download-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(16, 185, 129, 0.4);
        }

        .copy-btn {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 10px 20px;
            background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
            font-family: 'Inter', sans-serif;
        }

        .copy-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(59, 130, 246, 0.4);
        }

        .copy-btn.copied {
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            box-shadow: 0 4px 15px rgba(16, 185, 129, 0.4);
        }

        .copy-btn.copied span {
            animation: checkPop 0.3s ease;
        }

        @keyframes checkPop {
            0% { transform: scale(1); }
            50% { transform: scale(1.3); }
            100% { transform: scale(1); }
        }

        .table-container {
            max-height: 400px;
            overflow-y: auto;
            overflow-x: auto;
        }

        .table-container::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }

        .table-container::-webkit-scrollbar-track {
            background: var(--bg-tertiary);
        }

        .table-container::-webkit-scrollbar-thumb {
            background: var(--border-color);
            border-radius: 4px;
        }

        .results-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }

        .results-table th {
            background: var(--bg-tertiary);
            padding: 14px 16px;
            text-align: left;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            font-size: 11px;
            letter-spacing: 1px;
            position: sticky;
            top: 0;
            z-index: 10;
        }

        .results-table td {
            padding: 12px 16px;
            border-bottom: 1px solid var(--border-color);
            color: var(--text-primary);
            font-family: 'JetBrains Mono', monospace;
        }

        .results-table tr:hover td {
            background: rgba(99, 102, 241, 0.05);
        }

        .results-table tr:last-child td {
            border-bottom: none;
        }

        .badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .badge::before {
            content: '';
            width: 6px;
            height: 6px;
            border-radius: 50%;
        }

        .badge-success {
            background: rgba(16, 185, 129, 0.15);
            color: var(--accent-success);
            border: 1px solid rgba(16, 185, 129, 0.3);
        }
        .badge-success::before { background: var(--accent-success); }

        .badge-auth {
            background: rgba(239, 68, 68, 0.1);
            color: var(--accent-danger);
            border: 1px solid rgba(239, 68, 68, 0.2);
        }
        .badge-auth::before { background: var(--accent-danger); }

        .badge-conn {
            background: rgba(245, 158, 11, 0.1);
            color: var(--accent-warning);
            border: 1px solid rgba(245, 158, 11, 0.2);
        }
        .badge-conn::before { background: var(--accent-warning); }

        .badge-timeout {
            background: rgba(139, 92, 246, 0.1);
            color: #8b5cf6;
            border: 1px solid rgba(139, 92, 246, 0.2);
        }
        .badge-timeout::before { background: #8b5cf6; }

        .badge-error {
            background: rgba(239, 68, 68, 0.1);
            color: var(--accent-danger);
            border: 1px solid rgba(239, 68, 68, 0.2);
        }
        .badge-error::before { background: var(--accent-danger); }

        .badge-pending {
            background: rgba(59, 130, 246, 0.1);
            color: var(--accent-info);
            border: 1px solid rgba(59, 130, 246, 0.2);
        }
        .badge-pending::before {
            background: var(--accent-info);
            animation: pulse 2s infinite;
        }

        .speed-indicator {
            position: fixed;
            bottom: 24px;
            right: 24px;
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 16px 20px;
            display: flex;
            align-items: center;
            gap: 12px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.4);
            transform: translateY(100px);
            opacity: 0;
            transition: all 0.3s;
        }

        .speed-indicator.active {
            transform: translateY(0);
            opacity: 1;
        }

        .speed-value {
            font-size: 24px;
            font-weight: 700;
            color: var(--accent-primary);
            font-family: 'JetBrains Mono', monospace;
        }

        .speed-label {
            font-size: 11px;
            color: var(--text-secondary);
            text-transform: uppercase;
        }

        @keyframes slideIn {
            from {
                opacity: 0;
                transform: translateY(-10px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .animate-in {
            animation: slideIn 0.3s ease;
        }

        @media (max-width: 1200px) {
            .main-grid {
                grid-template-columns: 1fr;
            }
            .stats-grid {
                grid-template-columns: repeat(3, 1fr);
            }
        }

        @media (max-width: 768px) {
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
            .input-row {
                grid-template-columns: 1fr;
            }
        }

        .hidden { display: none !important; }
    </style>
</head>
<body>
    <div class="bg-grid"></div>
    <div class="bg-glow"></div>

    <div class="container">
        <header>
            <div class="logo">
                <div class="logo-icon">⚡</div>
                <div class="logo-text">
                    <h1>SMTP Sentinel</h1>
                    <p>High-Performance Testing Suite</p>
                </div>
            </div>
            <div class="connection-status disconnected" id="connStatus">
                <span class="status-dot"></span>
                <span id="connText">Disconnected</span>
            </div>
        </header>

        <div class="main-grid">
            <!-- Left Panel - Controls -->
            <div class="card">
                <div class="card-header">
                    <span>⚙️</span>
                    Configuration
                </div>
                
                <div class="form-group">
                    <label>SMTP Credentials</label>
                    <textarea id="smtpList" placeholder="smtp.example.com|587|user@example.com|password&#10;smtp.gmail.com|465|user@gmail.com|pass123"></textarea>
                </div>

                <div class="form-group">
                    <label>Target Email</label>
                    <input type="email" id="targetEmail" placeholder="recipient@example.com">
                </div>

                <div class="input-row">
                    <div class="form-group">
                        <label>Concurrency</label>
                        <input type="number" id="concurrency" value="100" min="1" max="200">
                    </div>
                    <div class="form-group">
                        <label>Timeout (s)</label>
                        <input type="number" id="timeout" value="5" min="1" max="30">
                    </div>
                </div>

                <button class="btn btn-primary" id="startBtn" onclick="startTest()">
                    <span>▶</span> Start Test
                </button>
                <button class="btn btn-danger hidden" id="stopBtn" onclick="stopTest()" style="margin-top: 12px;">
                    <span>⏹</span> Stop Test
                </button>
            </div>

            <!-- Right Panel - Stats & Visualization -->
            <div>
                <!-- Stats Grid -->
                <div class="stats-grid" id="statsGrid">
                    <div class="stat-card pending">
                        <div class="stat-value" id="statPending">0</div>
                        <div class="stat-label">Pending</div>
                    </div>
                    <div class="stat-card success">
                        <div class="stat-value" id="statSuccess">0</div>
                        <div class="stat-label">Success</div>
                    </div>
                    <div class="stat-card auth">
                        <div class="stat-value" id="statAuth">0</div>
                        <div class="stat-label">Auth Failed</div>
                    </div>
                    <div class="stat-card conn">
                        <div class="stat-value" id="statConn">0</div>
                        <div class="stat-label">Conn Failed</div>
                    </div>
                    <div class="stat-card timeout">
                        <div class="stat-value" id="statTimeout">0</div>
                        <div class="stat-label">Timeout</div>
                    </div>
                </div>

                <!-- Progress Bar -->
                <div class="progress-section hidden" id="progressSection">
                    <div class="progress-header">
                        <span class="progress-title">Test Progress</span>
                        <span class="progress-percent" id="progressPercent">0%</span>
                    </div>
                    <div class="progress-bar-bg">
                        <div class="progress-bar-fill" id="progressBar" style="width: 0%"></div>
                    </div>
                    <div class="progress-stats">
                        <span id="progressDetail">Ready to start</span>
                        <span id="speedStat">0 tests/sec</span>
                    </div>
                </div>

                <!-- Chart -->
                <div class="chart-container hidden" id="chartSection">
                    <canvas id="resultsChart"></canvas>
                </div>
            </div>
        </div>

        <!-- SUCCESS RESULTS BOX - FIXED -->
        <div class="success-container" id="successBox">
            <div class="success-header">
                <div class="success-title">
                    ✅ VALID SMTP ACCOUNTS
                    <span class="success-counter" id="successCounter">0</span>
                </div>
                <div style="display: flex; gap: 10px; align-items: center;">
                    <button class="copy-btn" id="copySuccessBtn" onclick="copySuccessSMTP()" style="display: none;">
                        <span>📋</span> Copy All
                    </button>
                    <div id="successDownloadBtn"></div>
                </div>
            </div>
            <div class="success-table-wrap">
                <table class="success-table">
                    <thead>
                        <tr>
                            <th>Host</th>
                            <th>Port</th>
                            <th>Username</th>
                            <th>Password</th>
                            <th>Response Time</th>
                            <th>Timestamp</th>
                        </tr>
                    </thead>
                    <tbody id="successBody"></tbody>
                </table>
            </div>
        </div>

        <!-- All Results Table -->
        <div class="results-section hidden" id="resultsSection">
            <div class="results-header">
                <span class="results-title">All Test Results</span>
                <div id="downloadContainer"></div>
            </div>
            <div class="table-container">
                <table class="results-table">
                    <thead>
                        <tr>
                            <th>Status</th>
                            <th>Host</th>
                            <th>Port</th>
                            <th>Username</th>
                            <th>Response Time</th>
                            <th>Error Details</th>
                        </tr>
                    </thead>
                    <tbody id="resultsBody"></tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Speed Indicator -->
    <div class="speed-indicator" id="speedIndicator">
        <div>
            <div class="speed-value" id="speedValue">0</div>
            <div class="speed-label">Tests/Second</div>
        </div>
    </div>

    <script>
        let ws;
        let chart = null;
        let testStartTime = null;
        let completedCount = 0;
        let totalCount = 0;
        let successCount = 0;
        
        let stats = {
            pending: 0,
            success: 0,
            auth_failed: 0,
            connection_failed: 0,
            timeout: 0,
            error: 0
        };
        
        let successList = []; // Store successful SMTPs for copying

        function connect() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => {
                document.getElementById('connStatus').className = 'connection-status connected';
                document.getElementById('connText').textContent = 'Connected';
            };
            
            ws.onclose = () => {
                document.getElementById('connStatus').className = 'connection-status disconnected';
                document.getElementById('connText').textContent = 'Disconnected';
                setTimeout(connect, 3000);
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                handleMessage(data);
            };
        }

        function handleMessage(data) {
            switch(data.type) {
                case 'connected':
                    console.log('Connected to server');
                    break;
                case 'test_started':
                    handleTestStarted(data);
                    break;
                case 'test_progress':
                    handleTestProgress(data);
                    break;
                case 'test_complete':
                    handleTestComplete(data);
                    break;
                case 'test_error':
                    handleTestError(data);
                    break;
                case 'test_stopped':
                    handleTestStopped(data);
                    break;
            }
        }

        function initChart() {
            const ctx = document.getElementById('resultsChart').getContext('2d');
            chart = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: ['Success', 'Auth Failed', 'Conn Failed', 'Timeout', 'Error'],
                    datasets: [{
                        data: [0, 0, 0, 0, 0],
                        backgroundColor: [
                            '#10b981',
                            '#ef4444',
                            '#f59e0b',
                            '#8b5cf6',
                            '#64748b'
                        ],
                        borderWidth: 0,
                        hoverOffset: 4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'right',
                            labels: {
                                color: '#94a3b8',
                                font: { family: 'Inter', size: 12 },
                                padding: 20,
                                usePointStyle: true
                            }
                        }
                    },
                    cutout: '70%'
                }
            });
        }

        function handleTestStarted(data) {
            totalCount = data.total;
            completedCount = 0;
            successCount = 0;
            successList = []; // Clear previous successful SMTPs
            testStartTime = Date.now();
            stats = { pending: data.total, success: 0, auth_failed: 0, connection_failed: 0, timeout: 0, error: 0 };
            
            // CRITICAL: Hide success box completely on start
            const successBox = document.getElementById('successBox');
            successBox.style.display = 'none';
            successBox.classList.remove('show');
            
            // Reset UI
            document.getElementById('progressSection').classList.remove('hidden');
            document.getElementById('chartSection').classList.remove('hidden');
            document.getElementById('resultsSection').classList.remove('hidden');
            document.getElementById('startBtn').classList.add('hidden');
            document.getElementById('stopBtn').classList.remove('hidden');
            document.getElementById('speedIndicator').classList.add('active');
            document.getElementById('resultsBody').innerHTML = '';
            document.getElementById('successBody').innerHTML = '';
            document.getElementById('downloadContainer').innerHTML = '';
            document.getElementById('successDownloadBtn').innerHTML = '';
            document.getElementById('successCounter').textContent = '0';
            document.getElementById('copySuccessBtn').style.display = 'none';
            
            if (!chart) initChart();
            updateChart();
            updateStats();
        }

        function handleTestProgress(data) {
            const r = data.result;
            completedCount++;
            
            stats.pending = totalCount - completedCount;
            if (r.status === 'success') {
                stats.success++;
                successCount++;
                // Store in format: host|port|user|pass
                const smtpString = `${r.host}|${r.port}|${r.user}|${r.password}`;
                successList.push(smtpString);
                addSuccessRow(r);
                showSuccessBox();
                updateSuccessCounter();
                showCopyButton();
            }
            else if (r.status === 'auth_failed') stats.auth_failed++;
            else if (r.status === 'connection_failed') stats.connection_failed++;
            else if (r.status === 'timeout') stats.timeout++;
            else stats.error++;
            
            const elapsed = (Date.now() - testStartTime) / 1000;
            const speed = Math.round(completedCount / elapsed);
            
            updateStats();
            updateChart();
            updateProgress(speed);
            addResultRow(r);
            
            document.getElementById('speedValue').textContent = speed;
        }

        function showSuccessBox() {
            const box = document.getElementById('successBox');
            if (box.style.display === 'none' || box.style.display === '') {
                box.style.display = 'block';
                // Trigger animation
                requestAnimationFrame(() => {
                    box.classList.add('show');
                });
            }
        }

        function updateSuccessCounter() {
            document.getElementById('successCounter').textContent = successCount;
        }

        function showCopyButton() {
            const btn = document.getElementById('copySuccessBtn');
            if (btn) {
                btn.style.display = 'inline-flex';
            }
        }

        async function copySuccessSMTP() {
            if (successList.length === 0) {
                alert('No successful SMTPs to copy');
                return;
            }
            
            const textToCopy = successList.join('\\n');
            
            try {
                await navigator.clipboard.writeText(textToCopy);
                
                const btn = document.getElementById('copySuccessBtn');
                const originalHTML = btn.innerHTML;
                btn.classList.add('copied');
                btn.innerHTML = '<span>✓</span> Copied!';
                
                setTimeout(() => {
                    btn.classList.remove('copied');
                    btn.innerHTML = originalHTML;
                }, 2000);
            } catch (err) {
                // Fallback for older browsers
                const textArea = document.createElement('textarea');
                textArea.value = textToCopy;
                document.body.appendChild(textArea);
                textArea.select();
                document.execCommand('copy');
                document.body.removeChild(textArea);
                
                const btn = document.getElementById('copySuccessBtn');
                const originalHTML = btn.innerHTML;
                btn.classList.add('copied');
                btn.innerHTML = '<span>✓</span> Copied!';
                
                setTimeout(() => {
                    btn.classList.remove('copied');
                    btn.innerHTML = originalHTML;
                }, 2000);
            }
        }

        function handleTestComplete(data) {
            resetUI();
            
            // Ensure success box is visible if there are results
            if (data.success > 0) {
                showSuccessBox();
                updateSuccessCounter();
                showCopyButton();
            }
            
            const downloadHtml = `
                <a href="${data.download_url}" class="download-btn" download>
                    <span>⬇</span> Download CSV
                </a>
            `;
            document.getElementById('downloadContainer').innerHTML = downloadHtml;
            document.getElementById('successDownloadBtn').innerHTML = downloadHtml;
            
            document.getElementById('progressDetail').textContent = 
                `Completed: ${data.success}/${data.total} successful (${data.success_rate}%)`;
        }

        function handleTestError(data) {
            alert('Error: ' + data.error);
            resetUI();
        }

        function handleTestStopped(data) {
            resetUI();
        }

        function startTest() {
            const smtpList = document.getElementById('smtpList').value.trim();
            const targetEmail = document.getElementById('targetEmail').value.trim();
            const concurrency = parseInt(document.getElementById('concurrency').value);
            const timeout = parseInt(document.getElementById('timeout').value);

            if (!smtpList || !targetEmail) {
                alert('Please fill in all fields');
                return;
            }

            ws.send(JSON.stringify({
                type: 'start_test',
                smtp_list: smtpList,
                target_email: targetEmail,
                concurrency: concurrency,
                timeout: timeout
            }));
        }

        function stopTest() {
            ws.send(JSON.stringify({ type: 'stop_test' }));
        }

        function resetUI() {
            document.getElementById('startBtn').classList.remove('hidden');
            document.getElementById('stopBtn').classList.add('hidden');
            document.getElementById('speedIndicator').classList.remove('active');
        }

        function updateStats() {
            document.getElementById('statPending').textContent = stats.pending;
            document.getElementById('statSuccess').textContent = stats.success;
            document.getElementById('statAuth').textContent = stats.auth_failed;
            document.getElementById('statConn').textContent = stats.connection_failed;
            document.getElementById('statTimeout').textContent = stats.timeout;
        }

        function updateChart() {
            if (!chart) return;
            chart.data.datasets[0].data = [
                stats.success,
                stats.auth_failed,
                stats.connection_failed,
                stats.timeout,
                stats.error
            ];
            chart.update('none');
        }

        function updateProgress(speed) {
            const pct = Math.round((completedCount / totalCount) * 100);
            document.getElementById('progressBar').style.width = pct + '%';
            document.getElementById('progressPercent').textContent = pct + '%';
            document.getElementById('progressDetail').textContent = 
                `${completedCount} of ${totalCount} completed`;
            document.getElementById('speedStat').textContent = `${speed} tests/sec`;
        }

        function addResultRow(r) {
            const tbody = document.getElementById('resultsBody');
            const row = document.createElement('tr');
            row.className = 'animate-in';
            
            const badges = {
                success: 'badge-success',
                auth_failed: 'badge-auth',
                connection_failed: 'badge-conn',
                timeout: 'badge-timeout',
                error: 'badge-error',
                pending: 'badge-pending'
            };
            
            const displayStatus = r.status.replace('_', ' ');
            
            row.innerHTML = `
                <td><span class="badge ${badges[r.status] || 'badge-error'}">${displayStatus}</span></td>
                <td>${r.host}</td>
                <td>${r.port}</td>
                <td>${r.user}</td>
                <td>${r.response_time ? r.response_time + 's' : '-'}</td>
                <td style="color: var(--accent-danger); font-size: 12px;">${r.error || '-'}</td>
            `;
            
            tbody.insertBefore(row, tbody.firstChild);
            
            if (tbody.children.length > 100) {
                tbody.removeChild(tbody.lastChild);
            }
        }

        function addSuccessRow(r) {
            const tbody = document.getElementById('successBody');
            const row = document.createElement('tr');
            row.className = 'animate-in';
            
            const timestamp = new Date(r.timestamp).toLocaleString();
            
            row.innerHTML = `
                <td>${r.host}</td>
                <td>${r.port}</td>
                <td>${r.user}</td>
                <td><span class="pwd-cell">${r.password}</span></td>
                <td class="time-cell">${r.response_time ? r.response_time + 's' : '-'}</td>
                <td>${timestamp}</td>
            `;
            
            tbody.insertBefore(row, tbody.firstChild);
        }

        connect();
    </script>
</body>
</html>"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
