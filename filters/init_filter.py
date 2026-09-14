import logging
import os
import pytz
import asyncio
from utils.constants import TEMP_DIR
from utils.media import get_max_media_size
from utils.media import has_downloadable_media

from filters.base_filter import BaseFilter

logger = logging.getLogger(__name__)

class InitFilter(BaseFilter):
    """
    初始化过滤器，为context添加基本信息
    """
    
    async def _process(self, context):
        """
        添加原始链接和发送者信息
        
        Args:
            context: 消息上下文
            
        Returns:
            bool: 是否继续处理
        """
        rule = context.rule
        event = context.event

        # 跳过纯文本消息：规则开启该设置后，无任何可下载媒体的消息直接中断整条过滤器链
        # （不下载、不转发、不推送、不写RSS），实现"直接跳过这一条"
        if getattr(rule, 'skip_pure_text', False) and not has_downloadable_media(event.message):
            logger.info(
                f'规则 {rule.id} 开启了"跳过纯文本消息"，消息 ID={event.message.id} 无媒体，直接跳过'
            )
            return False

        # logger.info(f"InitFilter处理消息前，context: {context.__dict__}")
        try:
            #处理媒体组消息
            if event.message.grouped_id:
                # 等待更长时间让所有媒体消息到达
                # await asyncio.sleep(1)
                
                # 收集媒体组的所有消息
                try:
                    async for message in event.client.iter_messages(
                        event.chat_id,
                        limit=20,
                        min_id=event.message.id - 10,
                        max_id=event.message.id + 10
                    ):
                        if message.grouped_id == event.message.grouped_id:
                            if message.text:    
                                # 保存第一条消息的文本和按钮
                                context.message_text = message.text or ''
                                context.original_message_text = message.text or ''
                                context.check_message_text = message.text or ''
                                context.buttons = message.buttons if hasattr(message, 'buttons') else None
                            logger.info(f'获取到媒体组文本并添加到context: {message.text}')
                        
                except Exception as e:
                    logger.error(f'收集媒体组消息时出错: {str(e)}')
                    context.errors.append(f"收集媒体组消息错误: {str(e)}")
           
        finally:
            # logger.info(f"InitFilter处理消息后，context: {context.__dict__}")
            return True
