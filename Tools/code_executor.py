import asyncio
from jupyter_client.manager import KernelManager
from jupyter_client.kernelspec import KernelSpecManager
import nest_asyncio
import logging
import time
import uuid
import black
import psutil
import gc
import concurrent.futures
from functools import partial
from queue import Empty

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.progress import Progress

class CodeExecutor:
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, use_rich=True, max_kernels=5):
        if not hasattr(self, 'initialized'):
            self.kernels = {}
            self.max_kernels = max_kernels
            nest_asyncio.apply()
            self.use_rich = use_rich
            
            self.console = Console()
            logging.basicConfig(
                level=logging.INFO,
                format="%(message)s",
                datefmt="[%X]",
                handlers=[RichHandler(rich_tracebacks=True)]
            )
            
            self.logger = logging.getLogger("CodeExecutor")
            self.process = psutil.Process()
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_kernels)
            self.initialized = True

    def print_kernel_info(self, kernel_id, message):
        """打印内核信息"""
        if self.use_rich:
            self.console.print(f"[bold cyan]Kernel {kernel_id}:[/] {message}")
        else:
            print(f"Kernel {kernel_id}: {message}")

    async def start_kernel(self, kernel_id=None):
        if len(self.kernels) >= self.max_kernels:
            self.logger.warning(f"Maximum number of kernels ({self.max_kernels}) reached.")
            return None

        if kernel_id is None:
            kernel_id = str(uuid.uuid4())[:8]  # 使用更短的ID
        
        self.print_kernel_info(kernel_id, "Starting...")
        start_time = time.time()
        
        km = KernelManager(kernel_name='python3')
        await asyncio.get_event_loop().run_in_executor(self.executor, km.start_kernel)
        kc = km.client()
        kc.start_channels()
        
        self.kernels[kernel_id] = {'km': km, 'kc': kc, 'start_time': time.time()}
        
        elapsed = time.time() - start_time
        self.print_kernel_info(kernel_id, f"Started in {elapsed:.2f}s")
        return kernel_id

    async def stop_kernel(self, kernel_id):
        if kernel_id not in self.kernels:
            return

        self.print_kernel_info(kernel_id, "Stopping...")
        start_time = time.time()

        kc = self.kernels[kernel_id]['kc']
        km = self.kernels[kernel_id]['km']
        
        if kc:
            kc.stop_channels()
        if km:
            await asyncio.get_event_loop().run_in_executor(self.executor, km.shutdown_kernel)
        
        if km.has_kernel:
            km.kernel.kill()
        
        uptime = time.time() - self.kernels[kernel_id]['start_time']
        del self.kernels[kernel_id]
        gc.collect()

        elapsed = time.time() - start_time
        self.print_kernel_info(kernel_id, f"Stopped in {elapsed:.2f}s (uptime: {uptime:.2f}s)")

    async def execute_code(self, code, kernel_id=None, timeout=30, auto_format=True, show_code=False):
        """执行Python代码
        
        Args:
            code: 要执行的Python代码
            kernel_id: 内核ID，如果为None则创建新内核
            timeout: 执行超时时间（秒）
            auto_format: 是否自动格式化代码
            show_code: 是否显示执行的代码
            
        Returns:
            tuple: (输出结果, 执行时间, 内核ID)
        """
        if kernel_id not in self.kernels:
            kernel_id = await self.start_kernel(kernel_id)
            if kernel_id is None:
                return None, 0, None

        kc = self.kernels[kernel_id]['kc']

        if auto_format:
            code = self.format_code(code)
        
        if show_code:
            self.print_kernel_info(kernel_id, "Executing code:")
            self.console.print(Panel(Syntax(code, "python", theme="monokai"), 
                                  title="Code", border_style="blue"))

        start_time = time.time()
        msg_id = kc.execute(code)
        
        output = []
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            self.executor, 
                            partial(kc.get_iopub_msg, timeout=1)
                        ),
                        timeout=timeout
                    )
                    
                    if msg['parent_header'].get('msg_id') == msg_id:
                        if msg['msg_type'] == 'stream':
                            content = msg['content']['text']
                            output.append(content)
                            if show_code:
                                self.print_kernel_info(kernel_id, f"Output: {content.strip()}")
                        elif msg['msg_type'] == 'execute_result':
                            content = str(msg['content']['data'].get('text/plain', ''))
                            output.append(content)
                            if show_code:
                                self.print_kernel_info(kernel_id, f"Result: {content.strip()}")
                        elif msg['msg_type'] == 'error':
                            error_msg = f"Error: {msg['content']['ename']} - {msg['content']['evalue']}"
                            output.append(error_msg)
                            if show_code:
                                self.print_kernel_info(kernel_id, error_msg)
                        elif msg['msg_type'] == 'status' and msg['content']['execution_state'] == 'idle':
                            break
                except asyncio.TimeoutError:
                    msg = f"Execution timed out after {timeout} seconds"
                    output.append(msg)
                    if show_code:
                        self.print_kernel_info(kernel_id, msg)
                    break
                except Empty:
                    continue
                except Exception as e:
                    msg = f"Error during execution: {str(e)}"
                    output.append(msg)
                    if show_code:
                        self.print_kernel_info(kernel_id, msg)
                    break
        finally:
            execution_time = time.time() - start_time
            if show_code:
                self.print_kernel_info(kernel_id, f"Completed in {execution_time:.2f}s")
            
        return ''.join(output), execution_time, kernel_id

    def format_code(self, code):
        try:
            return black.format_str(code, mode=black.FileMode())
        except Exception as e:
            self.logger.warning(f"Code formatting failed: {str(e)}")
            return code

    def get_kernel_stats(self):
        """获取内核统计信息"""
        stats = []
        for kernel_id, info in self.kernels.items():
            uptime = time.time() - info['start_time']
            stats.append({
                'kernel_id': kernel_id,
                'uptime': uptime,
                'status': 'running' if info['km'].is_alive() else 'dead'
            })
        return stats

    def print_kernel_stats(self):
        """打印内核统计信息"""
        stats = self.get_kernel_stats()
        
        table = Table(title="Kernel Statistics")
        table.add_column("Kernel ID", style="cyan")
        table.add_column("Uptime", style="magenta")
        table.add_column("Status", style="green")
        
        for stat in stats:
            table.add_row(
                stat['kernel_id'],
                f"{stat['uptime']:.2f}s",
                stat['status']
            )
        
        self.console.print(table)

    async def cleanup(self):
        """清理所有内核资源"""
        for kernel_id in list(self.kernels.keys()):
            await self.stop_kernel(kernel_id)
        self.executor.shutdown()
