# coding: utf-8
import json
import logging
import os
from datetime import datetime

from .scheduler import scheduler
from ..vars import DATA_PATH


logger = logging.getLogger(__name__)

LOG_CLEANUP_JOB_ID = 'log_cleanup_job'
LOG_CLEANUP_CONFIG_PATH = os.path.join(DATA_PATH, 'log_cleanup_config.json')
LOG_CLEANUP_STATUS_PATH = os.path.join(DATA_PATH, 'log_cleanup_status.json')
LOG_SUFFIXES = ('.log', '.log.gz', '.txt', '.txt.gz', '.out', '.err')
MB = 1024 * 1024


def _default_config(default_log_dir=''):
    return dict(
        enabled=False,
        log_dir=default_log_dir or '',
        size_mb=500,
        keep_days=7,
        interval_hours=24,
    )


def _load_json(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        logger.exception('Failed to load json from %s', path)
        return {}


def _save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)


def normalize_config(data=None, default_log_dir=''):
    defaults = _default_config(default_log_dir)
    raw = {}
    if isinstance(data, dict):
        raw.update(data)

    log_dir = str(raw.get('log_dir', defaults['log_dir']) or defaults['log_dir']).strip()
    if log_dir:
        log_dir = os.path.abspath(log_dir)

    def _int_value(key, default, minimum):
        value = raw.get(key, default)
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = default
        return max(minimum, value)

    enabled = raw.get('enabled', defaults['enabled'])
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() in ('1', 'true', 'yes', 'on')
    else:
        enabled = bool(enabled)

    return dict(
        enabled=enabled,
        log_dir=log_dir,
        size_mb=_int_value('size_mb', defaults['size_mb'], 0),
        keep_days=_int_value('keep_days', defaults['keep_days'], 0),
        interval_hours=_int_value('interval_hours', defaults['interval_hours'], 1),
    )


def load_log_cleanup_config(default_log_dir=''):
    data = _load_json(LOG_CLEANUP_CONFIG_PATH)
    return normalize_config(data, default_log_dir=default_log_dir)


def save_log_cleanup_config(data, default_log_dir=''):
    config = normalize_config(data, default_log_dir=default_log_dir)
    _save_json(LOG_CLEANUP_CONFIG_PATH, config)
    return config


def load_log_cleanup_status():
    status = _load_json(LOG_CLEANUP_STATUS_PATH)
    return status if isinstance(status, dict) else {}


def save_log_cleanup_status(status):
    _save_json(LOG_CLEANUP_STATUS_PATH, status)
    return status


def format_size(size):
    try:
        value = float(size)
    except (TypeError, ValueError):
        return '0 B'
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == 'B':
                return '%d %s' % (int(value), unit)
            return '%.2f %s' % (value, unit)
        value /= 1024.0
    return '0 B'


def cleanup_logs(config=None, default_log_dir=''):
    config = normalize_config(config or load_log_cleanup_config(default_log_dir), default_log_dir=default_log_dir)
    now = datetime.now()
    threshold_bytes = config['size_mb'] * MB
    cutoff_ts = None if config['keep_days'] <= 0 else now.timestamp() - config['keep_days'] * 86400

    result = dict(
        started_at=now.strftime('%Y-%m-%d %H:%M:%S'),
        finished_at='',
        enabled=config['enabled'],
        log_dir=config['log_dir'],
        size_mb=config['size_mb'],
        keep_days=config['keep_days'],
        interval_hours=config['interval_hours'],
        scanned_files=0,
        matched_files=0,
        deleted_files=0,
        freed_bytes=0,
        errors=[],
        sample_deleted=[],
        sample_matched=[],
        message='',
    )

    log_dir = config['log_dir']
    if not log_dir:
        result['message'] = 'log_dir is empty'
        result['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_log_cleanup_status(result)
        return result
    if not os.path.isdir(log_dir):
        result['message'] = 'log_dir does not exist: %s' % log_dir
        result['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_log_cleanup_status(result)
        return result

    for root, _dirs, files in os.walk(log_dir):
        for filename in files:
            path = os.path.join(root, filename)
            if os.path.islink(path):
                continue
            if not filename.endswith(LOG_SUFFIXES):
                continue
            try:
                stat = os.stat(path)
            except OSError as err:
                result['errors'].append('%s: %s' % (path, err))
                continue
            result['scanned_files'] += 1
            if stat.st_size < threshold_bytes:
                continue
            if cutoff_ts is not None and stat.st_mtime > cutoff_ts:
                continue

            result['matched_files'] += 1
            if len(result['sample_matched']) < 20:
                result['sample_matched'].append(dict(path=path, size=stat.st_size))
            try:
                os.remove(path)
            except OSError as err:
                result['errors'].append('%s: %s' % (path, err))
            else:
                result['deleted_files'] += 1
                result['freed_bytes'] += stat.st_size
                if len(result['sample_deleted']) < 20:
                    result['sample_deleted'].append(dict(path=path, size=stat.st_size))

    result['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result['freed_size'] = format_size(result['freed_bytes'])
    result['message'] = 'deleted %s files, freed %s' % (result['deleted_files'], result['freed_size'])
    save_log_cleanup_status(result)
    logger.warning('Log cleanup finished: %s', result['message'])
    return result


def run_log_cleanup_job():
    config = load_log_cleanup_config()
    if not config.get('enabled'):
        logger.info('Skip log cleanup job because it is disabled')
        return load_log_cleanup_status()
    return cleanup_logs(config=config)


def refresh_log_cleanup_job(default_log_dir=''):
    config = load_log_cleanup_config(default_log_dir=default_log_dir)
    job = scheduler.get_job(LOG_CLEANUP_JOB_ID)
    if job:
        scheduler.remove_job(LOG_CLEANUP_JOB_ID)
    if not config.get('enabled'):
        return None
    scheduler.add_job(
        func=run_log_cleanup_job,
        trigger='interval',
        hours=config['interval_hours'],
        id=LOG_CLEANUP_JOB_ID,
        name='log cleanup',
        jobstore='memory',
        replace_existing=True,
    )
    return scheduler.get_job(LOG_CLEANUP_JOB_ID)

