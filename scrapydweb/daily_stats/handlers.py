"""Incremental handlers for events emitted by ScrapydWeb."""

from .common import TIMER_TASKS_DB, connect_readonly
from .events import JOB_FINISHED, TASK_JOB_CREATED, TASK_RESULT_UPDATED
from .reconcile import (fetch_remote_items, mark_result, seed_status_rows,
                        sync_independent_job, sync_timer_task_results_by_ids)


def _load_task_job_result(task_job_result_id):
    connection = connect_readonly(TIMER_TASKS_DB)
    try:
        row = connection.execute(
            '''
            SELECT tjr.id AS task_job_result_id, tr.task_id, tjr.task_result_id,
                   tjr.result AS job_id, tjr.server, t.project, t.spider, tjr.run_time
            FROM task_job_result tjr
            JOIN task_result tr ON tr.id = tjr.task_result_id
            JOIN task t ON t.id = tr.task_id
            WHERE tjr.id = ? AND tjr.status = 'ok'
            ''',
            (task_job_result_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def handle_task_job_created(task_job_result_id):
    row = _load_task_job_result(task_job_result_id)
    if not row:
        return
    seed_status_rows([row])
    items, error_text = fetch_remote_items(row)
    mark_result(task_job_result_id, items, error_text)


def handle_task_result_updated(task_result_id):
    sync_timer_task_results_by_ids([task_result_id])


def handle_job_finished(job_id, server=None):
    sync_independent_job(job_id)
    connection = connect_readonly(TIMER_TASKS_DB)
    try:
        query = '''
            SELECT id FROM task_job_result
            WHERE status = 'ok' AND result = ?
        '''
        params = [job_id]
        if server:
            query += ' AND server = ?'
            params.append(server)
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()
    for row in rows:
        handle_task_job_created(row['id'])


HANDLERS = {
    TASK_JOB_CREATED: handle_task_job_created,
    TASK_RESULT_UPDATED: handle_task_result_updated,
    JOB_FINISHED: handle_job_finished,
}
