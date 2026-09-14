"""
媒体组去重缓存（共享）

Telethon 对同一媒体组（grouped_id）的每条消息都会触发一次事件，
但过滤器链会把整个组一起处理，因此必须去重，否则同一组会被转发多次。

事件驱动路径（message_listener）与定时刷新路径（MessageRefresher）
共用本模块，避免两种来源各自维护缓存导致重复转发。
"""
import asyncio
import logging

logger = logging.getLogger(__name__)

# 已处理过的媒体组，键为 f"{chat_id}:{grouped_id}"
_PROCESSED_GROUPS = set()

# 缓存保留时间（秒），与原先 message_listener 中的 5 分钟一致
GROUP_CACHE_TTL = 300


def make_group_key(chat_id, grouped_id) -> str:
    """生成媒体组缓存键"""
    return f"{chat_id}:{grouped_id}"


def is_group_processed(chat_id, grouped_id) -> bool:
    """该媒体组是否已处理过"""
    if not grouped_id:
        return False
    return make_group_key(chat_id, grouped_id) in _PROCESSED_GROUPS


def mark_group_processed(chat_id, grouped_id, ttl: int = GROUP_CACHE_TTL) -> bool:
    """标记媒体组已处理

    返回 True 表示本次是首次标记（调用方应继续处理）；
    返回 False 表示该组已处理过（调用方应跳过）。
    """
    if not grouped_id:
        # 非媒体组消息不做去重，始终返回 True
        return True

    key = make_group_key(chat_id, grouped_id)
    if key in _PROCESSED_GROUPS:
        logger.debug(f'媒体组已处理过，跳过: {key}')
        return False

    _PROCESSED_GROUPS.add(key)
    asyncio.create_task(_clear_group_cache(key, ttl))
    return True


async def _clear_group_cache(group_key: str, delay: int):
    """延迟清除已处理的媒体组记录"""
    await asyncio.sleep(delay)
    _PROCESSED_GROUPS.discard(group_key)
