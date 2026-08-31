# coding: utf-8
from ...utils.log_cleanup import (
    LOG_CLEANUP_CONFIG_PATH, LOG_CLEANUP_STATUS_PATH, format_size,
    load_log_cleanup_config, load_log_cleanup_status
)


def get_log_cleanup_view_data(default_log_dir=''):
    config = load_log_cleanup_config(default_log_dir=default_log_dir)
    status = load_log_cleanup_status()
    status.setdefault('freed_size', format_size(status.get('freed_bytes', 0)))
    return dict(
        log_cleanup_config=config,
        log_cleanup_status=status,
        log_cleanup_config_path=LOG_CLEANUP_CONFIG_PATH,
        log_cleanup_status_path=LOG_CLEANUP_STATUS_PATH,
    )

