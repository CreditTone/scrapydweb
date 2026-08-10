"""In-process event bus for incremental statistics updates."""

import logging
import threading
from collections import namedtuple
from queue import Empty, Queue


TASK_JOB_CREATED = 'task_job_created'
TASK_RESULT_UPDATED = 'task_result_updated'
JOB_FINISHED = 'job_finished'

StatsEvent = namedtuple('StatsEvent', 'name payload')

_logger = logging.getLogger(__name__)
_queue = Queue()
_worker = None
_worker_lock = threading.Lock()
_enabled = False


def publish(name, **payload):
    """Publish after the source database transaction has committed."""
    if not _enabled:
        return
    _queue.put(StatsEvent(name=name, payload=payload))


def publish_task_job_created(task_job_result_id):
    publish(TASK_JOB_CREATED, task_job_result_id=task_job_result_id)


def publish_task_result_updated(task_result_id):
    publish(TASK_RESULT_UPDATED, task_result_id=task_result_id)


def publish_job_finished(job_id, server=None):
    publish(JOB_FINISHED, job_id=job_id, server=server)


def dispatch(event):
    # Import lazily so model modules can publish without circular imports.
    from .handlers import HANDLERS
    handler = HANDLERS.get(event.name)
    if handler is None:
        _logger.warning('No daily-stats handler for event %s', event.name)
        return
    handler(**event.payload)


def _consume():
    while True:
        try:
            event = _queue.get(timeout=1)
        except Empty:
            continue
        try:
            dispatch(event)
        except Exception:
            _logger.exception('Daily-stats event failed: %s', event)
        finally:
            _queue.task_done()


def start_event_worker():
    global _enabled, _worker
    with _worker_lock:
        _enabled = True
        if _worker and _worker.is_alive():
            return _worker
        _worker = threading.Thread(target=_consume, name='daily-stats-events', daemon=True)
        _worker.start()
        return _worker
