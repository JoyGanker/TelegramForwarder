"""
消息刷新机制（Bot 转发模式）

Telethon 依赖长连接推送，遇到 Telegram 服务器或网络故障时可能漏收消息。
本模块在 Bot 转发模式（use_bot=True，走过滤器链）下每 60 秒轮询一次所有被
监控的源聊天，按「上次处理到的消息ID」增量拉取漏收的消息，合成等价的事件
对象并复用同一套过滤器链处理，从而确保不漏消息。

设计要点：
- 增量拉取：user_client.iter_messages(peer, min_id=last_id, reverse=True)，
  只取 ID 大于上次处理位置的消息，按旧→新顺序处理，避免中间漏掉。
- 去重：与事件驱动路径共用 utils.group_cache，避免媒体组被重复转发。
- 持久化：上次处理位置写入 chats.last_processed_message_id，
  重启后能继续补漏停机期间的消息；首次运行以当前最新消息为基准，不重放历史。
- 限速：每轮每个聊天最多补 batch_limit 条，剩余下轮继续，避免补漏时刷屏。
"""
import asyncio
import logging
import os

from telethon import utils as telethon_utils

from filters.process import process_forward_rule
from models.models import Chat, ForwardRule, get_session
from utils.group_cache import mark_group_processed

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    """从环境变量读取正整数，非法值回退到默认值"""
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        logger.warning(f"环境变量 {name}={raw!r} 非正整数，使用默认值 {default}")
        return default


class _SyntheticEvent:
    """模拟 Telethon NewMessage 事件

    使刷新拉取到的消息能直接复用过滤器链。仅需提供过滤器用到的属性：
    message / chat_id / client / sender / get_chat()。
    """

    def __init__(self, client, message, chat_id):
        self.client = client
        self._message = message
        self.chat_id = chat_id          # Telethon 风格 peer id，如 -1001234567890
        self.chat = None
        self.sender = None              # 未预取，info_filter 会回退到 peer_id 逻辑
        self.sender_id = getattr(message, 'sender_id', None)

    @property
    def message(self):
        return self._message

    async def get_chat(self):
        if self.chat is None:
            self.chat = await self.client.get_entity(self.chat_id)
        return self.chat


class MessageRefresher:
    """定时刷新器：轮询补漏被 Telethon 漏收的消息"""

    def __init__(self, user_client, bot_client):
        self.user_client = user_client
        self.bot_client = bot_client

        self.interval = _env_int('REFRESH_INTERVAL', 60)
        self.batch_limit = _env_int('REFRESH_BATCH_LIMIT', 20)
        self.enabled = os.getenv('REFRESH_ENABLED', 'true').lower() == 'true'

        self._task = None
        self._running = False
        self._last_ids = {}        # telegram_chat_id(str) -> 上次处理的消息ID
        self._initialized = set()  # 已完成初始化的聊天
        self._peer_cache = {}      # telegram_chat_id(str) -> 解析出的实体
        self._bot_id = None

    def init(self, user_client, bot_client):
        """注入客户端实例（main.py 在客户端启动后调用）

        单例在各模块间共享，故采用注入而非构造传参，
        保证 message_listener 与主流程拿到的是同一个对象。
        """
        self.user_client = user_client
        self.bot_client = bot_client

    async def start(self):
        """启动刷新循环"""
        if not self.enabled:
            logger.info('消息刷新机制未启用（REFRESH_ENABLED=false）')
            return
        if self._running:
            return

        try:
            me = await self.bot_client.get_me()
            self._bot_id = me.id
        except Exception as e:
            logger.warning(f'获取机器人ID失败，刷新时将不会过滤机器人自身消息: {e}')

        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(
            f'消息刷新机制已启动: 间隔={self.interval}s, 每轮每聊天最多补 {self.batch_limit} 条'
        )

    async def stop(self):
        """停止刷新循环，并在退出前把处理位置落盘"""
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=3)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                logger.error(f'停止消息刷新任务时出错: {e}')
            self._task = None
        await self._persist_all()
        logger.info('消息刷新机制已停止')

    def mark_processed(self, chat_key: str, message_id: int):
        """事件驱动路径处理消息时调用，避免刷新重复处理同一条

        仅更新内存（高频调用不写库），由刷新循环周期性落盘。
        """
        if not chat_key or not message_id:
            return
        current = self._last_ids.get(chat_key)
        if current is None or message_id > current:
            self._last_ids[chat_key] = message_id

    async def _loop(self):
        """刷新主循环"""
        # 启动后先等待一个间隔，避免与登录初始化抢资源
        try:
            await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            raise

        while self._running:
            try:
                await self.refresh_all()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f'刷新消息时出错: {e}')
            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                raise

    async def refresh_all(self):
        """刷新所有被监控的源聊天"""
        session = get_session()
        try:
            rules = session.query(ForwardRule).filter(
                ForwardRule.enable_rule == True,
                ForwardRule.use_bot == True
            ).all()

            # 收集去重后的源聊天
            chats = {}
            for rule in rules:
                source_chat = rule.source_chat
                if source_chat and source_chat.telegram_chat_id:
                    chats[source_chat.telegram_chat_id] = source_chat

            if not chats:
                logger.debug('刷新: 没有需要监控的 Bot 模式源聊天')
                return

            for chat_key, chat in chats.items():
                try:
                    await self._refresh_chat(session, chat_key, chat)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f'刷新聊天 {chat.name}({chat_key}) 时出错: {e}')
                    await asyncio.sleep(1)  # 出错后稍作停顿，避免连续报错
        finally:
            session.close()

    async def _refresh_chat(self, session, chat_key: str, chat: Chat):
        """刷新单个聊天的漏收消息"""
        peer = await self._resolve_peer(chat_key)
        if peer is None:
            logger.warning(f'刷新: 无法解析聊天实体 {chat.name}({chat_key})，跳过')
            return

        # 首次处理该聊天时确定起始位置
        if chat_key not in self._initialized:
            await self._init_last_id(peer, chat_key, chat)
            self._initialized.add(chat_key)

        last_id = self._last_ids.get(chat_key) or 0

        # 增量拉取：min_id 表示排除 ID <= last_id 的消息；reverse=True 保证旧→新
        new_messages = []
        try:
            async for message in self.user_client.iter_messages(
                peer,
                min_id=last_id,
                limit=self.batch_limit,
                reverse=True
            ):
                if message.id <= last_id:
                    continue
                new_messages.append(message)
        except Exception as e:
            logger.error(f'刷新: 拉取聊天 {chat.name}({chat_key}) 消息失败: {e}')
            return

        if not new_messages:
            return

        logger.info(
            f'刷新: 聊天 {chat.name}({chat_key}) 发现 {len(new_messages)} 条漏收消息，'
            f'ID {new_messages[0].id} ~ {new_messages[-1].id}'
        )

        for message in new_messages:
            if self._running is False:
                break
            try:
                await self._process_message(session, chat_key, chat, message)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f'刷新: 处理消息 {message.id} 时出错: {e}')
            finally:
                # 无论成功失败都推进位置，避免同一条反复重试导致卡住
                self.mark_processed(chat_key, message.id)

        # 本轮结束后落盘
        await self._persist_chat(session, chat_key, chat)

    async def _process_message(self, session, chat_key: str, chat: Chat, message):
        """用过滤器链处理一条刷新到的消息"""
        # 跳过机器人自身消息，避免自我循环
        if self._bot_id and getattr(message, 'sender_id', None) == self._bot_id:
            logger.debug(f'刷新: 跳过机器人自身消息 {message.id}')
            return

        # 媒体组去重（与事件路径共用缓存）
        grouped_id = getattr(message, 'grouped_id', None)
        if not mark_group_processed(chat_key, grouped_id):
            return

        chat_id_str = chat_key

        # 媒体组消息需要等待同组其他消息到达，否则可能只收集到部分
        if grouped_id:
            await asyncio.sleep(1)

        # 查该聊天下的所有规则
        rules = session.query(ForwardRule).filter(
            ForwardRule.source_chat_id == chat.id
        ).all()

        if not rules:
            return

        event = _SyntheticEvent(
            self.user_client,
            message,
            telethon_utils.get_peer_id(message.peer_id)
        )

        for rule in rules:
            if not rule.enable_rule:
                continue
            if not rule.use_bot:
                # 刷新机制只覆盖 Bot 转发模式
                continue
            try:
                logger.info(
                    f'刷新: 处理规则 {rule.id} 的漏收消息 ID={message.id} '
                    f'(从 {chat.name} 到 {rule.target_chat.name})'
                )
                await process_forward_rule(self.bot_client, event, chat_id_str, rule)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f'刷新: 规则 {rule.id} 处理消息 {message.id} 失败: {e}')

    async def _init_last_id(self, peer, chat_key: str, chat: Chat):
        """确定聊天的起始处理位置

        优先取数据库中持久化的位置（可补漏停机期间的消息）；
        否则取内存值（事件路径已处理的）；
        两者都没有则取当前最新一条消息，避免首次部署重放全部历史。
        """
        stored = getattr(chat, 'last_processed_message_id', None)
        current = self._last_ids.get(chat_key)

        if stored is None and current is None:
            newest = None
            try:
                async for message in self.user_client.iter_messages(peer, limit=1):
                    newest = message.id
                    break
            except Exception as e:
                logger.error(f'刷新: 获取聊天 {chat.name} 最新消息失败: {e}')
            self._last_ids[chat_key] = newest or 0
            logger.info(
                f'刷新: 聊天 {chat.name}({chat_key}) 首次初始化基准消息ID={self._last_ids[chat_key]}'
            )
        else:
            self._last_ids[chat_key] = max(stored or 0, current or 0)
            logger.info(
                f'刷新: 聊天 {chat.name}({chat_key}) 从消息ID={self._last_ids[chat_key]} 继续'
            )

    async def _resolve_peer(self, chat_key: str):
        """解析聊天实体

        数据库中存的是 abs(id) 字符串，需还原为 Telethon 可用的 peer：
        频道为 -100xxx，普通群组为 -xxx，用户为正数。依次尝试并在成功后缓存。
        """
        if chat_key in self._peer_cache:
            return self._peer_cache[chat_key]

        try:
            raw = int(chat_key)
        except (TypeError, ValueError):
            logger.error(f'刷新: 聊天ID格式非法: {chat_key}')
            return None

        abs_id = abs(raw)
        candidates = [int(f'-100{abs_id}'), int(f'-{abs_id}'), raw]

        for candidate in candidates:
            try:
                entity = await self.user_client.get_entity(candidate)
                if entity:
                    self._peer_cache[chat_key] = entity
                    return entity
            except Exception:
                continue

        logger.warning(f'刷新: 无法解析聊天 {chat_key} 的实体')
        return None

    async def _persist_chat(self, session, chat_key: str, chat: Chat):
        """把处理位置写入数据库"""
        value = self._last_ids.get(chat_key)
        if value is None:
            return
        try:
            if getattr(chat, 'last_processed_message_id', None) != value:
                chat.last_processed_message_id = value
                session.commit()
        except Exception as e:
            logger.error(f'刷新: 持久化聊天 {chat_key} 处理位置失败: {e}')
            try:
                session.rollback()
            except Exception:
                pass

    async def _persist_all(self):
        """停止前把所有处理位置落盘"""
        session = get_session()
        try:
            for chat_key, value in self._last_ids.items():
                try:
                    chat = session.query(Chat).filter(
                        Chat.telegram_chat_id == chat_key
                    ).first()
                    if chat and chat.last_processed_message_id != value:
                        chat.last_processed_message_id = value
                        session.commit()
                except Exception as e:
                    logger.error(f'刷新: 持久化聊天 {chat_key} 处理位置失败: {e}')
                    try:
                        session.rollback()
                    except Exception:
                        pass
        finally:
            session.close()


# 模块级单例，供 main.py 与 message_listener 共用
message_refresher = MessageRefresher(user_client=None, bot_client=None)
