# coded by WhoKnows | https://t.me/Moonlightcrow

import asyncio
import aiosmtplib
import ssl
import csv
import io
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Tuple, Dict, Callable, Optional

logger = logging.getLogger(__name__)


class SMTPTester:
    def __init__(self, max_concurrent: int = 50, timeout: int = 10):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.semaphore = asyncio.BoundedSemaphore(max_concurrent)
        self.is_running = False
        self.should_stop = False
        
    def parse_content(self, content: str) -> List[Tuple[str, int, str, str]]:
        """format: host|port|user|pass"""
        credentials = []
        lines = content.strip().split('\n')
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            parts = line.split('|')
            if len(parts) != 4:
                continue
                
            host, port, user, password = parts
            try:
                port = int(port)
            except ValueError:
                continue
                
            credentials.append((host, port, user, password))
                
        return credentials

    def _get_ssl_context(self) -> ssl.SSLContext:
        """Get SSL context"""
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE 
        return context

    async def test_smtp(
        self, 
        host: str, 
        port: int, 
        user: str, 
        password: str,
        target_email: str,
        progress_callback: Optional[Callable] = None
    ) -> Dict:
        async with self.semaphore:
            if self.should_stop:
                return {
                    'host': host, 'port': port, 'user': user, 'password': password,
                    'status': 'stopped', 'error': 'Test stopped by user',
                    'response_time': None, 'timestamp': datetime.now().isoformat()
                }
            
            result = {
                'host': host, 'port': port, 'user': user, 'password': password,
                'status': 'pending', 'error': None,
                'response_time': None, 'timestamp': datetime.now().isoformat()
            }
            
            smtp: Optional[aiosmtplib.SMTP] = None
            start_time = asyncio.get_event_loop().time()
            
            try:
                msg = MIMEMultipart('alternative')
                msg['Subject'] = f'SMTP Test - {host}'
                msg['From'] = user
                msg['To'] = target_email
                
                html_body = f"""
                <html>
                    <body style="font-family: Arial, sans-serif; padding: 20px;">
                        <h2 style="color: #2c3e50;">SMTP Test Email</h2>
                        <p><strong>Server:</strong> {host}:{port}</p>
                        <p><strong>From:</strong> {user}</p>
                        <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                        <p style="color: #27ae60; font-weight: bold;">✓ Connection successful!</p>
                    </body>
                </html>
                """
                
                text_body = f"SMTP Test\nServer: {host}:{port}\nFrom: {user}\nStatus: SUCCESS"
                
                msg.attach(MIMEText(text_body, 'plain'))
                msg.attach(MIMEText(html_body, 'html'))
                
                
                use_tls = port == 465 
                
                smtp = aiosmtplib.SMTP(
                    hostname=host,
                    port=port,
                    timeout=self.timeout,
                    use_tls=use_tls,  
                )
                
                if use_tls:

                    await smtp.connect()
                else:
                    await smtp.connect()
                    
                    if port == 587:
                        try:
                            if await smtp.supports_extension('STARTTLS'):
                                await smtp.starttls(self._get_ssl_context())
                        except Exception as e:
                            logger.debug(f"STARTTLS failed for {host}:{port}: {e}")
                            
                
                # Login and send
                await smtp.login(user, password)
                await smtp.send_message(msg)
                
                result['status'] = 'success'
                result['response_time'] = round(asyncio.get_event_loop().time() - start_time, 2)
                
            except aiosmtplib.errors.SMTPAuthenticationError as e:
                result['status'] = 'auth_failed'
                result['error'] = f"Authentication failed: {str(e)}"
                
            except aiosmtplib.errors.SMTPConnectError as e:
                result['status'] = 'connection_failed'
                result['error'] = f"Connection error: {str(e)}"
                
            except aiosmtplib.errors.SMTPTimeoutError as e:
                result['status'] = 'timeout'
                result['error'] = f"Timeout after {self.timeout}s"
                
            except asyncio.TimeoutError:
                result['status'] = 'timeout'
                result['error'] = f"Timeout after {self.timeout}s"
                
            except ssl.SSLError as e:
                result['status'] = 'connection_failed'
                result['error'] = f"SSL/TLS error: {str(e)}"
                
            except Exception as e:
                result['status'] = 'error'
                result['error'] = f"{type(e).__name__}: {str(e)}"
                
            finally:

                if smtp:
                    try:
                        await smtp.quit()
                    except Exception:
                        try:
                            await smtp.close()
                        except Exception:
                            pass
            
            if progress_callback:
                try:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(result)
                    else:
                        progress_callback(result)
                except Exception as e:
                    logger.error(f"Progress callback error: {e}")
                
            return result

    async def run_tests(
        self, 
        credentials: List[Tuple], 
        target_email: str,
        progress_callback: Optional[Callable] = None
    ) -> List[Dict]:
        """Run all tests concurrently - DEPRECATED: Use run_tests_streaming for memory efficiency"""
        self.is_running = True
        self.should_stop = False
        results = []
        
        tasks = [
            self.test_smtp(host, port, user, pwd, target_email, progress_callback)
            for host, port, user, pwd in credentials
        ]
        
        for coro in asyncio.as_completed(tasks):
            if self.should_stop:
                # Cancel remaining tasks
                for task in tasks:
                    if not task.done():
                        task.cancel()
                break
            try:
                result = await coro
                results.append(result)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Task error: {e}")
                
        self.is_running = False
        return results

    async def run_tests_streaming(
        self, 
        credentials: List[Tuple], 
        target_email: str,
        progress_callback: Optional[Callable] = None
    ) -> None:

        self.is_running = True
        self.should_stop = False
        
        # Create  tasks
        pending_tasks = {
            asyncio.create_task(
                self.test_smtp(host, port, user, pwd, target_email, None),
                name=f"smtp_test_{host}_{port}_{user}"
            ): (host, port, user, pwd)
            for host, port, user, pwd in credentials
        }
        
        completed_count = 0
        total_count = len(credentials)
        
        try:
            while pending_tasks and not self.should_stop:
               
                done, pending_tasks = await asyncio.wait(
                    pending_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=self.timeout + 5 
                )
                
                for task in done:
                    completed_count += 1
                    try:
                       
                        if task.cancelled():
                            continue
                            
                        result = await task
                        
                        
                        if progress_callback:
                            try:
                                if asyncio.iscoroutinefunction(progress_callback):
                                    await progress_callback(result)
                                else:
                                    progress_callback(result)
                            except Exception as e:
                                logger.error(f"Progress callback error: {e}")
                                
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.error(f"Task error: {e}")
                        
                        if progress_callback:
                            try:
                                host, port, user, pwd = pending_tasks.get(task, ('unknown', 0, 'unknown', ''))
                                error_result = {
                                    'host': host, 'port': port, 'user': user, 'password': pwd,
                                    'status': 'error', 'error': str(e),
                                    'response_time': None, 'timestamp': datetime.now().isoformat()
                                }
                                if asyncio.iscoroutinefunction(progress_callback):
                                    await progress_callback(error_result)
                                else:
                                    progress_callback(error_result)
                            except Exception:
                                pass
            
            
            if self.should_stop and pending_tasks:
                for task in pending_tasks:
                    task.cancel()
                
                
                if pending_tasks:
                    await asyncio.wait(pending_tasks, return_when=asyncio.ALL_COMPLETED)
                    
        except asyncio.CancelledError:
            
            for task in pending_tasks:
                task.cancel()
            if pending_tasks:
                await asyncio.wait(pending_tasks, return_when=asyncio.ALL_COMPLETED)
            raise
            
        finally:
            self.is_running = False

    def stop(self):
        self.should_stop = True

    def generate_csv(self, results: List[Dict]) -> str:
        output = io.StringIO()
        fieldnames = ['host', 'port', 'user', 'password', 'status', 'error', 'response_time', 'timestamp']
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
        return output.getvalue()