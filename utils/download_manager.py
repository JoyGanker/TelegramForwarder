"""
下载管理器（单例）

在 Bot 转发模式（use_bot=True，走 FilterChain）下统一管理「下载到本地」这一步骤：
- 有界队列（上限 250 个待处理任务）：队列满时阻塞生产者，形成背压，
  作为临时盘不超过约 8GB 的保障机制。
- 3 个 worker 协程同时运行下载任务。
- 每个任务内部最多 2 个文件并发下载（→ 全局峰值 3×2=6 并发文件下载）。

所有下载调用点（MediaFilter / SenderFilter / PushFilter）均通过本管理器入队，
message.download_media() 仍使用消息自带的 client 绑定，行为不变。
"""
import asyncio
import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    """从环境变量读取整数，非法值回退到默认值"""
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        logger.warning(f"环境变量 {name}={raw!r} 非正整数，使用默认值 {default}")
        return default


class _DownloadTask:
    """队列中的一个下载任务

    messages: list[Message]，单条下载为 [1 条]，媒体组为 [N 条]
    download_dir: 下载目录
    propagate: 单条下载时是否向上传播异常（保持调用点原有 try/except 行为）
    future: 调用方 await 此 future 获取结果（路径列表）
    """

    __slots__ = ("messages", "download_dir", "propagate", "future")

    def __init__(self, messages, download_dir, propagate=False):
        self.messages = messages
        self.download_dir = download_dir
        self.propagate = propagate
        self.future: Optional[asyncio.Future] = None


class DownloadManager:
    """下载队列管理器：有界队列 + 多 worker + 每任务内并发限制"""

    def __init__(self):
        self.max_queue = _env_int("DOWNLOAD_QUEUE_MAX_SIZE", 250)
        self.max_workers = _env_int("DOWNLOAD_MAX_CONCURRENT_TASKS", 3)
        self.max_concurrent_per_task = _env_int(
            "DOWNLOAD_MAX_CONCURRENT_PER_TASK", 2
        )

        self._queue: Optional[asyncio.Queue] = None
        self._workers: List[asyncio.Task] = []
        self._started = False
        self._start_lock = asyncio.Lock()

    async def start(self):
        """创建有界队列与 worker 协程（幂等，可重复调用）"""
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            loop = asyncio.get_running_loop()
            self._queue = asyncio.Queue(maxsize=self.max_queue)
            self._workers = [
                loop.create_task(self._worker(i)) for i in range(self.max_workers)
            ]
            self._started = True
            logger.info(
                f"下载队列已启动: 队列上限={self.max_queue}, "
                f"worker={self.max_workers}, 每任务并发={self.max_concurrent_per_task}"
            )

    async def stop(self):
        """优雅停止 worker（取消并等待）"""
        if not self._started:
            return
        for w in self._workers:
            w.cancel()
        for w in self._workers:
            try:
                await asyncio.wait_for(w, timeout=2)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
        self._workers.clear()
        self._started = False
        logger.info("下载队列已停止")

    async def _ensure(self):
        """确保队列已启动（懒启动）"""
        if not self._started:
            await self.start()

    async def download_single(self, message, download_dir) -> Optional[str]:
        """单文件下载：入队一条任务，await 结果

        异常会向上传播，保持各调用点原有 try/except 的错误处理行为。
        返回下载后的文件路径（无媒体或下载返回 None 时为 None）。
        """
        await self._ensure()
        task = _DownloadTask([message], download_dir, propagate=True)
        task.future = asyncio.get_running_loop().create_future()
        await self._queue.put(task)  # 队列满(250)时阻塞 → 背压
        paths = await task.future
        return paths[0] if paths else None

    async def download_group(self, messages, download_dir) -> List[str]:
        """媒体组下载：入队一条任务，内部 2 并发下载所有文件

        单个文件失败不影响其余文件（best-effort），返回成功下载的路径列表。
        """
        await self._ensure()
        task = _DownloadTask(list(messages), download_dir)
        task.future = asyncio.get_running_loop().create_future()
        await self._queue.put(task)
        return await task.future

    async def _worker(self, idx: int):
        """worker 主循环：从队列取任务执行"""
        while True:
            task: _DownloadTask = await self._queue.get()
            try:
                paths = await self._run_task(task)
                if not task.future.done():
                    task.future.set_result(paths)
            except Exception as e:
                logger.error(f"worker[{idx}] 下载任务出错: {e}")
                if not task.future.done():
                    task.future.set_exception(e)
            finally:
                self._queue.task_done()

    async def _run_task(self, task: _DownloadTask) -> List[str]:
        """执行单个任务：每任务最多 max_concurrent_per_task 个文件并发下载"""
        sem = asyncio.Semaphore(self.max_concurrent_per_task)

        async def _download_one(msg):
            async with sem:
                if not getattr(msg, "media", None):
                    return None
                return await msg.download_media(task.download_dir)

        # 并发下载本任务内的所有文件，单文件异常被收集而不中断其余
        results = await asyncio.gather(
            *[_download_one(m) for m in task.messages], return_exceptions=True
        )
        paths: List[str] = []
        first_exc: Optional[Exception] = None
        for r in results:
            if isinstance(r, BaseException):
                logger.error(f"下载单个文件失败: {r}")
                if first_exc is None:
                    first_exc = r
            elif r:
                paths.append(r)
        # 单条下载（propagate=True）：向上传播异常，保持调用点原有 try/except 行为
        # 媒体组下载：best-effort，返回成功下载的路径
        if task.propagate and first_exc is not None:
            raise first_exc
        return paths


# 模块级单例，供各过滤器直接导入使用
download_manager = DownloadManager()
