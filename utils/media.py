import logging
import os

logger = logging.getLogger(__name__)

def has_downloadable_media(message) -> bool:
    """判断消息是否包含可下载的媒体文件

    判定为「有媒体」的类型：图片(photo)、文档(document)、视频(video)、
    音频(audio)、语音(voice)。其余情况（纯文本、webpage 链接预览等）
    均视为无可下载媒体。
    """
    media = getattr(message, 'media', None)
    if not media:
        return False
    return any([
        getattr(media, 'photo', None),
        getattr(media, 'document', None),
        getattr(media, 'video', None),
        getattr(media, 'audio', None),
        getattr(media, 'voice', None),
    ])


async def get_media_size(media):
    """获取媒体文件大小"""
    if not media:
        return 0

    try:
        # 对于所有类型的媒体，先尝试获取 document
        if hasattr(media, 'document') and media.document:
            return media.document.size

        # 对于照片，获取最大尺寸
        if hasattr(media, 'photo') and media.photo:
            # 获取最大尺寸的照片
            largest_photo = max(media.photo.sizes, key=lambda x: x.size if hasattr(x, 'size') else 0)
            return largest_photo.size if hasattr(largest_photo, 'size') else 0

        # 如果是其他类型，尝试直接获取 size 属性
        if hasattr(media, 'size'):
            return media.size

    except Exception as e:
        logger.error(f'获取媒体大小时出错: {str(e)}')

    return 0

async def get_max_media_size():
    """获取媒体文件大小上限"""
    max_media_size_str = os.getenv('MAX_MEDIA_SIZE')
    if not max_media_size_str:
        logger.error('未设置 MAX_MEDIA_SIZE 环境变量')
        raise ValueError('必须在 .env 文件中设置 MAX_MEDIA_SIZE')
    return float(max_media_size_str) * 1024 * 1024  # 转换为字节，支持小数