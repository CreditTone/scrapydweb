# coding: utf-8
import os
import logging
import time
import threading
from collections import Counter
from datetime import datetime, timedelta

import requests

from .common import (
    DAILY_STATS_DB,
    JOBS_DB,
    JOB_TABLE_SERVER_MAP,
    SERVER_AUTH_MAP,
    TIMER_TASKS_DB,
    build_task_key,
    connect_readonly,
    connect_writable,
    ensure_status_db,
    format_date,
    get_cursor,
    get_existing_job_tables,
    get_month_string,
    get_week_start_string,
    get_year_int,
    now_string,
    set_cursor,
)


_background_thread = None
_background_lock = threading.Lock()


SYNC_DISCOVERY_INTERVAL = max(1, int(os.environ.get('DAILY_STATS_SYNC_INTERVAL', '60')))
SYNC_BATCH_SIZE = max(1, int(os.environ.get('DAILY_STATS_SYNC_BATCH_SIZE', '200')))
SYNC_LOOKBACK_MINUTES = max(1, int(os.environ.get('DAILY_STATS_SYNC_LOOKBACK_MINUTES', '60')))
SYNC_REQUEST_TIMEOUT = float(os.environ.get('DAILY_STATS_SYNC_REQUEST_TIMEOUT', '3'))
SYNC_REQUEST_INTERVAL = max(0.0, float(os.environ.get('DAILY_STATS_SYNC_REQUEST_INTERVAL', '1')))
SYNC_RETRY_SECONDS = max(1, int(os.environ.get('DAILY_STATS_SYNC_RETRY_SECONDS', '600')))
SYNC_JSON_RETENTION_DAYS = max(1, int(os.environ.get('DAILY_STATS_JSON_RETENTION_DAYS', '7')))
FACT_SYNC_BATCH_SIZE = max(1, int(os.environ.get('DAILY_STATS_FACT_SYNC_BATCH_SIZE', '500')))
FACT_REFRESH_LOOKBACK_HOURS = max(1, int(os.environ.get('DAILY_STATS_FACT_REFRESH_LOOKBACK_HOURS', '24')))
PRIORITY_BACKFILL_YEARS = [
    int(item.strip())
    for item in os.environ.get('DAILY_STATS_PRIORITY_BACKFILL_YEARS', '2026,2025,2024').split(',')
    if item.strip()
]
SQLITE_MAX_VARIABLES = max(1, int(os.environ.get('DAILY_STATS_SQLITE_MAX_VARIABLES', '800')))

SPECIAL_INDEPENDENT_SPIDERS = set([
    'che168_usedcar_car',
    'dongchedi_usedcar_car',
])
SPECIAL_INDEPENDENT_NAMES = {
    'dongchedi_usedcar_car': '懂车帝二手车源任务',
    'che168_usedcar_car': '汽车之家二手车源任务',
}

FAILURE_PATTERNS = (
    ('FileNotFoundError', '缺少部署文件'),
    ('ModuleNotFoundError', '模块导入失败'),
    ('ImportError', '模块导入失败'),
    ('TimeoutError', '执行超时'),
    ('ReadTimeout', '请求超时'),
    ('ConnectTimeout', '连接超时'),
    ('ConnectionError', '连接失败'),
    ('ProxyError', '代理异常'),
    ('DNSLookupError', 'DNS 解析失败'),
    ('404', '资源不存在'),
    ('403', '目标站拒绝访问'),
    ('429', '目标站限流'),
)

http_session = requests.Session()


def resolve_scrapyd_server(server_value):
    if not server_value:
        return ''
    if ':' in server_value:
        return server_value
    return JOB_TABLE_SERVER_MAP.get(server_value, '')


def fetch_running_job_ids_from_scrapyd(server, project):
    if not server or not project:
        return None, 'missing_server_or_project'
    url = 'http://{server}/listjobs.json'.format(server=server)
    try:
        response = http_session.get(
            url,
            params={'project': project},
            auth=SERVER_AUTH_MAP.get(server),
            timeout=SYNC_REQUEST_TIMEOUT,
        )
        if response.status_code != 200:
            return None, 'http_%s' % response.status_code
        payload = response.json()
        running_jobs = set()
        for row in payload.get('running', []) or []:
            job_id = row.get('id') or row.get('jobid')
            if job_id:
                running_jobs.add(job_id)
        return running_jobs, None
    except Exception as err:
        return None, str(err)
def get_recent_cutoff():
    return (datetime.now() - timedelta(minutes=SYNC_LOOKBACK_MINUTES)).strftime('%Y-%m-%d %H:%M:%S')


def get_json_retention_cutoff():
    return (datetime.now() - timedelta(days=SYNC_JSON_RETENTION_DAYS)).strftime('%Y-%m-%d %H:%M:%S')


def get_fact_refresh_cutoff():
    return (datetime.now() - timedelta(hours=FACT_REFRESH_LOOKBACK_HOURS)).strftime('%Y-%m-%d %H:%M:%S')


def normalize_task_name(name, task_id):
    return (name or 'task #%s' % task_id).replace(' - edit', '')


def normalize_failure_reason(error_text):
    if not error_text:
        return ''
    for pattern, label in FAILURE_PATTERNS:
        if pattern in error_text:
            return label
    for line in error_text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:120]
    return '未知失败'


def is_terminal_sync_error(error_text):
    # A log endpoint commonly returns 404 while a newly scheduled job is still
    # pending. Retain it for bounded retry instead of treating it as final.
    return False


def choose_latest_time(*values):
    filtered = [value for value in values if value]
    return max(filtered) if filtered else None


def chunked(sequence, size):
    for index in range(0, len(sequence), size):
        yield sequence[index:index + size]


def fetch_recent_task_job_results():
    cutoff = get_recent_cutoff()
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT
                tjr.id AS task_job_result_id,
                tr.task_id AS task_id,
                tjr.task_result_id AS task_result_id,
                tjr.result AS job_id,
                tjr.server AS server,
                t.project AS project,
                t.spider AS spider,
                tjr.run_time AS run_time
            FROM task_job_result tjr
            JOIN task_result tr ON tr.id = tjr.task_result_id
            JOIN task t ON t.id = tr.task_id
            WHERE tjr.status = 'ok' AND tjr.run_time >= ?
            ORDER BY tjr.id DESC
            ''',
            (cutoff,)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def seed_status_rows(records):
    conn = connect_writable(DAILY_STATS_DB)
    try:
        for record in records:
            conn.execute(
                '''
                INSERT OR IGNORE INTO task_job_status (
                    task_job_result_id, task_id, task_result_id, job_id, server, project, spider,
                    run_time, scraped_items, sync_status, attempt_count, last_error, next_retry_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 'pending', 0, NULL, NULL, ?)
                ''',
                (
                    record['task_job_result_id'],
                    record['task_id'],
                    record['task_result_id'],
                    record['job_id'],
                    record['server'],
                    record['project'],
                    record['spider'],
                    record['run_time'],
                    now_string(),
                )
            )
        conn.commit()
    finally:
        conn.close()


def reset_inflight_rows():
    conn = connect_writable(DAILY_STATS_DB)
    try:
        conn.execute(
            '''
            UPDATE task_job_status
            SET sync_status = 'retry',
                next_retry_at = NULL,
                updated_at = ?
            WHERE sync_status IN ('queued', 'in_progress')
            ''',
            (now_string(),)
        )
        conn.commit()
    finally:
        conn.close()


def load_pending_rows():
    cutoff = get_recent_cutoff()
    retention_cutoff = get_json_retention_cutoff()
    now = now_string()
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM task_job_status
            WHERE run_time >= ?
              AND (run_time >= ? OR updated_at >= ?)
              AND (scraped_items IS NULL OR sync_status != 'success')
              AND sync_status NOT IN ('success', 'not_found', 'queued', 'in_progress')
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY updated_at ASC, task_job_result_id ASC
            LIMIT ?
            ''',
            (retention_cutoff, cutoff, cutoff, now, SYNC_BATCH_SIZE)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def mark_sync_status(task_job_result_id, sync_status):
    conn = connect_writable(DAILY_STATS_DB)
    try:
        conn.execute(
            '''
            UPDATE task_job_status
            SET sync_status = ?, updated_at = ?
            WHERE task_job_result_id = ?
            ''',
            (sync_status, now_string(), task_job_result_id)
        )
        conn.commit()
    finally:
        conn.close()


def extract_scraped_items(stats):
    items = stats.get('items')
    if items is None:
        items = stats.get('crawler_stats', {}).get('item_scraped_count')
    return items if isinstance(items, int) else None


def fetch_jobs_db_items(row):
    job_id = row.get('job_id')
    if not job_id:
        return None, False

    print('[%s] jobs_db lookup task_job_result_id=%s job_id=%s' % (
        now_string(), row.get('task_job_result_id'), job_id
    ))
    conn = connect_readonly(JOBS_DB)
    try:
        for _index, table_name in get_existing_job_tables():
            db_row = conn.execute(
                '''
                SELECT items, status
                FROM "{table_name}"
                WHERE deleted = '0' AND job = ?
                ORDER BY update_time DESC, id DESC
                LIMIT 1
                '''.format(table_name=table_name),
                (job_id,)
            ).fetchone()
            if not db_row:
                continue
            if isinstance(db_row['items'], int):
                print('[%s] jobs_db hit task_job_result_id=%s table=%s items=%s status=%s' % (
                    now_string(), row.get('task_job_result_id'), table_name, db_row['items'], db_row['status']
                ))
                return db_row['items'], False
            if str(db_row['status']) == '2':
                print('[%s] jobs_db hit task_job_result_id=%s table=%s items=NULL status=2 fallback=json_once' % (
                    now_string(), row.get('task_job_result_id'), table_name
                ))
                return None, True
    finally:
        conn.close()
    print('[%s] jobs_db miss task_job_result_id=%s job_id=%s' % (
        now_string(), row.get('task_job_result_id'), job_id
    ))
    return None, False


def fetch_remote_items(row):
    items, fallback_zero_on_error = fetch_jobs_db_items(row)
    if isinstance(items, int):
        return items, None

    json_url = 'http://{server}/logs/{project}/{spider}/{job}.json'.format(
        server=row['server'],
        project=row['project'],
        spider=row['spider'],
        job=row['job_id'],
    )
    try:
        print('[%s] requesting task_job_result_id=%s url=%s' % (
            now_string(), row['task_job_result_id'], json_url
        ))
        response = http_session.get(
            json_url,
            auth=SERVER_AUTH_MAP.get(row['server']),
            timeout=SYNC_REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            if fallback_zero_on_error:
                print('[%s] json fallback task_job_result_id=%s status=%s fallback=0' % (
                    now_string(), row['task_job_result_id'], response.status_code
                ))
                return 0, None
            return None, 'http_%s' % response.status_code
        items = extract_scraped_items(response.json())
        if isinstance(items, int):
            return items, None
        if fallback_zero_on_error:
            print('[%s] json fallback task_job_result_id=%s missing_items fallback=0' % (
                now_string(), row['task_job_result_id']
            ))
            return 0, None
        return None, 'missing_items'
    except Exception as err:
        if fallback_zero_on_error:
            print('[%s] json fallback task_job_result_id=%s error=%s fallback=0' % (
                now_string(), row['task_job_result_id'], str(err)
            ))
            return 0, None
        return None, str(err)


def mark_result(task_job_result_id, items, error_text):
    affected_task_result_id = None
    conn = connect_writable(DAILY_STATS_DB)
    try:
        row = conn.execute(
            'SELECT task_result_id FROM task_job_status WHERE task_job_result_id = ?',
            (task_job_result_id,)
        ).fetchone()
        affected_task_result_id = row['task_result_id'] if row else None
        if isinstance(items, int):
            conn.execute(
                '''
                UPDATE task_job_status
                SET scraped_items = ?, sync_status = 'success', last_error = NULL,
                    next_retry_at = NULL, updated_at = ?
                WHERE task_job_result_id = ?
                ''',
                (items, now_string(), task_job_result_id)
            )
        else:
            sync_status = 'retry'
            next_retry_at = (datetime.now() + timedelta(seconds=SYNC_RETRY_SECONDS)).strftime('%Y-%m-%d %H:%M:%S')
            if is_terminal_sync_error(error_text):
                sync_status = 'not_found'
                next_retry_at = None
            conn.execute(
                '''
                UPDATE task_job_status
                SET sync_status = ?, attempt_count = attempt_count + 1, last_error = ?,
                    next_retry_at = ?, updated_at = ?
                WHERE task_job_result_id = ?
                ''',
                (
                    sync_status,
                    error_text[:500] if error_text else 'unknown',
                    next_retry_at,
                    now_string(),
                    task_job_result_id,
                )
            )
        conn.commit()
    finally:
        conn.close()
    if affected_task_result_id:
        sync_timer_task_results_by_ids([affected_task_result_id])


def fetch_timer_task_results_batch(last_id, limit):
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT tr.id, tr.task_id, tr.execute_time, tr.fail_count, tr.pass_count,
                   t.name AS task_name, t.project, t.spider
            FROM task_result tr
            JOIN task t ON t.id = tr.task_id
            WHERE tr.id > ?
            ORDER BY tr.id ASC
            LIMIT ?
            ''',
            (last_id, limit)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def fetch_recent_timer_task_results(cutoff):
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT tr.id, tr.task_id, tr.execute_time, tr.fail_count, tr.pass_count,
                   t.name AS task_name, t.project, t.spider
            FROM task_result tr
            JOIN task t ON t.id = tr.task_id
            WHERE tr.execute_time >= ?
            ORDER BY tr.id ASC
            ''',
            (cutoff,)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def fetch_timer_task_result_details(task_result_ids):
    if not task_result_ids:
        return {}
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        grouped = {}
        for batch in chunked(task_result_ids, SQLITE_MAX_VARIABLES):
            placeholders = ','.join('?' for _ in batch)
            rows = conn.execute(
                '''
                SELECT id, task_result_id, node, server, status_code, status, result, run_time
                FROM task_job_result
                WHERE task_result_id IN ({placeholders})
                ORDER BY id ASC
                '''.format(placeholders=placeholders),
                batch
            ).fetchall()
            for row in rows:
                grouped.setdefault(row['task_result_id'], []).append(dict(row))
        return grouped
    finally:
        conn.close()


def fetch_timer_task_result_ids_by_job_ids(job_ids):
    if not job_ids:
        return []
    normalized_job_ids = sorted(set(job_id for job_id in job_ids if job_id))
    if not normalized_job_ids:
        return []
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        task_result_ids = set()
        for batch in chunked(normalized_job_ids, SQLITE_MAX_VARIABLES):
            placeholders = ','.join('?' for _ in batch)
            rows = conn.execute(
                '''
                SELECT DISTINCT task_result_id
                FROM task_job_result
                WHERE result IN ({})
                ORDER BY task_result_id ASC
                '''.format(placeholders),
                batch
            ).fetchall()
            for row in rows:
                task_result_ids.add(row['task_result_id'])
        return sorted(task_result_ids)
    finally:
        conn.close()


def fetch_status_rows_by_task_job_result_ids(task_job_result_ids):
    if not task_job_result_ids:
        return {}
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        grouped = {}
        for batch in chunked(task_job_result_ids, SQLITE_MAX_VARIABLES):
            placeholders = ','.join('?' for _ in batch)
            rows = conn.execute(
                'SELECT * FROM task_job_status WHERE task_job_result_id IN ({})'.format(placeholders),
                batch
            ).fetchall()
            for row in rows:
                grouped[row['task_job_result_id']] = dict(row)
        return grouped
    finally:
        conn.close()


def build_timer_fact(task_result, task_job_results, status_rows, independent_fact_map=None):
    independent_fact_map = independent_fact_map or {}
    status = 'failed'
    if task_result['fail_count'] == 0 and task_result['pass_count'] > 0:
        status = 'success'
    elif task_result['fail_count'] == 0 and task_result['pass_count'] == 0:
        status = 'running'
    scraped_items = 0
    scraped_found = False
    job_id = None
    server = None
    node = None
    start_time = None
    finish_time = None
    failure_reason = ''
    for record in task_job_results:
        if not job_id and record.get('result'):
            job_id = record['result']
        if not server and record.get('server'):
            server = record['server']
        if node is None and record.get('node') is not None:
            node = record['node']
        start_time = choose_latest_time(start_time, record.get('run_time'))
        finish_time = choose_latest_time(finish_time, record.get('run_time'))
        if record.get('status') != 'ok' and not failure_reason:
            failure_reason = normalize_failure_reason(record.get('result', ''))
        status_row = status_rows.get(record['id'])
        if status_row and isinstance(status_row.get('scraped_items'), int):
            scraped_items += status_row['scraped_items']
            scraped_found = True
    execute_time = task_result['execute_time']
    fact = dict(
        source_type='timer',
        source_pk=str(task_result['id']),
        task_key=build_task_key('timer', task_result['task_id'], task_result['spider']),
        task_id=task_result['task_id'],
        task_name=normalize_task_name(task_result['task_name'], task_result['task_id']),
        project=task_result['project'],
        spider=task_result['spider'],
        job_id=job_id or '',
        server=server or '',
        node=node,
        planned_time=execute_time,
        start_time=start_time or execute_time,
        finish_time=finish_time or execute_time,
        run_date=format_date(execute_time),
        run_week_start=get_week_start_string(execute_time),
        run_month=get_month_string(execute_time),
        run_year=get_year_int(execute_time),
        status=status,
        scraped_items=scraped_items if scraped_found else None,
        failure_reason=failure_reason or '',
        is_timer_child=0,
    )
    return merge_timer_with_independent_fact(fact, independent_fact_map.get(job_id or ''))


def upsert_execution_facts(facts):
    if not facts:
        return
    conn = connect_writable(DAILY_STATS_DB)
    try:
        for fact in facts:
            created_at = now_string()
            updated_at = now_string()
            result = conn.execute(
                '''
                UPDATE task_execution_fact
                SET task_key = ?, task_id = ?, task_name = ?, project = ?, spider = ?, job_id = ?, server = ?,
                    node = ?, planned_time = ?, start_time = ?, finish_time = ?, run_date = ?, run_week_start = ?,
                    run_month = ?, run_year = ?, status = ?, scraped_items = ?, failure_reason = ?,
                    is_timer_child = ?, updated_at = ?
                WHERE source_type = ? AND source_pk = ?
                ''',
                (
                    fact['task_key'], fact['task_id'], fact['task_name'], fact['project'], fact['spider'],
                    fact['job_id'], fact['server'], fact['node'], fact['planned_time'], fact['start_time'],
                    fact['finish_time'], fact['run_date'], fact['run_week_start'], fact['run_month'],
                    fact['run_year'], fact['status'], fact['scraped_items'], fact['failure_reason'],
                    fact['is_timer_child'], updated_at, fact['source_type'], fact['source_pk']
                )
            )
            if result.rowcount:
                continue
            conn.execute(
                '''
                INSERT INTO task_execution_fact (
                    source_type, source_pk, task_key, task_id, task_name, project, spider, job_id, server, node,
                    planned_time, start_time, finish_time, run_date, run_week_start, run_month, run_year,
                    status, scraped_items, failure_reason, is_timer_child, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    fact['source_type'], fact['source_pk'], fact['task_key'], fact['task_id'], fact['task_name'],
                    fact['project'], fact['spider'], fact['job_id'], fact['server'], fact['node'],
                    fact['planned_time'], fact['start_time'], fact['finish_time'], fact['run_date'],
                    fact['run_week_start'], fact['run_month'], fact['run_year'], fact['status'],
                    fact['scraped_items'], fact['failure_reason'], fact['is_timer_child'], created_at, updated_at
                )
            )
        conn.commit()
    finally:
        conn.close()


def collect_affected_keys(facts):
    return dict(
        dates=sorted(set(fact['run_date'] for fact in facts if fact.get('run_date'))),
        weeks=sorted(set(fact['run_week_start'] for fact in facts if fact.get('run_week_start'))),
        months=sorted(set(fact['run_month'] for fact in facts if fact.get('run_month'))),
        years=sorted(set(fact['run_year'] for fact in facts if fact.get('run_year') is not None)),
    )


def rebuild_daily_aggregates(run_dates):
    if not run_dates:
        return
    placeholders = ','.join('?' for _ in run_dates)
    conn = connect_writable(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM task_execution_fact
            WHERE is_timer_child = 0 AND run_date IN ({})
            ORDER BY run_date, source_type, task_key, start_time
            '''.format(placeholders),
            run_dates
        ).fetchall()
        conn.execute(
            'DELETE FROM task_daily_agg WHERE run_date IN ({})'.format(placeholders),
            run_dates
        )
        grouped = {}
        for row in rows:
            record = dict(row)
            key = (record['run_date'], record['source_type'], record['task_key'])
            group = grouped.setdefault(key, dict(
                run_date=record['run_date'],
                run_week_start=record['run_week_start'],
                run_month=record['run_month'],
                run_year=record['run_year'],
                source_type=record['source_type'],
                task_key=record['task_key'],
                task_id=record['task_id'],
                task_name=record['task_name'],
                project=record['project'],
                spider=record['spider'],
                should_execute=0,
                actual_execute=0,
                success_count=0,
                failed_count=0,
                running_count=0,
                scraped_items_total=0,
                latest_execute_time=None,
                failure_counter=Counter(),
            ))
            group['actual_execute'] += 1
            if record['status'] == 'success':
                group['success_count'] += 1
            elif record['status'] == 'failed':
                group['failed_count'] += 1
            elif record['status'] == 'running':
                group['running_count'] += 1
            if isinstance(record.get('scraped_items'), int):
                group['scraped_items_total'] += record['scraped_items']
            if record.get('failure_reason'):
                group['failure_counter'][record['failure_reason']] += 1
            group['latest_execute_time'] = choose_latest_time(
                group['latest_execute_time'],
                record.get('finish_time'),
                record.get('start_time'),
                record.get('planned_time'),
            )
        for group in grouped.values():
            top_failure_reason = ''
            if group['failure_counter']:
                top_failure_reason = group['failure_counter'].most_common(1)[0][0]
            conn.execute(
                '''
                INSERT INTO task_daily_agg (
                    run_date, run_week_start, run_month, run_year, source_type, task_key, task_id, task_name,
                    project, spider, should_execute, actual_execute, success_count, failed_count, running_count,
                    scraped_items_total, latest_execute_time, top_failure_reason, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    group['run_date'], group['run_week_start'], group['run_month'], group['run_year'],
                    group['source_type'], group['task_key'], group['task_id'], group['task_name'],
                    group['project'], group['spider'], group['should_execute'], group['actual_execute'],
                    group['success_count'], group['failed_count'], group['running_count'],
                    group['scraped_items_total'], group['latest_execute_time'], top_failure_reason, now_string()
                )
            )
        conn.commit()
    finally:
        conn.close()


def rebuild_rollup_aggregates(source_table, target_table, period_column, period_values):
    if not period_values:
        return
    placeholders = ','.join('?' for _ in period_values)
    conn = connect_writable(DAILY_STATS_DB)
    try:
        conn.execute(
            'DELETE FROM {target_table} WHERE {period_column} IN ({placeholders})'.format(
                target_table=target_table, period_column=period_column, placeholders=placeholders
            ),
            period_values
        )
        rows = conn.execute(
            '''
            SELECT *
            FROM {source_table}
            WHERE {period_column} IN ({placeholders})
            ORDER BY {period_column}, source_type, task_key
            '''.format(source_table=source_table, period_column=period_column, placeholders=placeholders),
            period_values
        ).fetchall()
        grouped = {}
        for row in rows:
            record = dict(row)
            key = (record[period_column], record['source_type'], record['task_key'])
            group = grouped.setdefault(key, dict(
                period_value=record[period_column],
                source_type=record['source_type'],
                task_key=record['task_key'],
                task_id=record['task_id'],
                task_name=record['task_name'],
                project=record['project'],
                spider=record['spider'],
                should_execute=0,
                actual_execute=0,
                success_count=0,
                failed_count=0,
                running_count=0,
                scraped_items_total=0,
                latest_execute_time=None,
                failure_counter=Counter(),
            ))
            group['should_execute'] += record.get('should_execute', 0) or 0
            group['actual_execute'] += record.get('actual_execute', 0) or 0
            group['success_count'] += record.get('success_count', 0) or 0
            group['failed_count'] += record.get('failed_count', 0) or 0
            group['running_count'] += record.get('running_count', 0) or 0
            group['scraped_items_total'] += record.get('scraped_items_total', 0) or 0
            group['latest_execute_time'] = choose_latest_time(group['latest_execute_time'], record.get('latest_execute_time'))
            if record.get('top_failure_reason') and (record.get('failed_count', 0) or 0) > 0:
                group['failure_counter'][record['top_failure_reason']] += record.get('failed_count', 0) or 0
        for group in grouped.values():
            top_failure_reason = ''
            if group['failure_counter']:
                top_failure_reason = group['failure_counter'].most_common(1)[0][0]
            conn.execute(
                '''
                INSERT INTO {target_table} (
                    {period_column}, source_type, task_key, task_id, task_name, project, spider, should_execute,
                    actual_execute, success_count, failed_count, running_count, scraped_items_total,
                    latest_execute_time, top_failure_reason, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''.format(target_table=target_table, period_column=period_column),
                (
                    group['period_value'], group['source_type'], group['task_key'], group['task_id'],
                    group['task_name'], group['project'], group['spider'], group['should_execute'],
                    group['actual_execute'], group['success_count'], group['failed_count'],
                    group['running_count'], group['scraped_items_total'], group['latest_execute_time'],
                    top_failure_reason, now_string()
                )
            )
        conn.commit()
    finally:
        conn.close()


def rebuild_aggregates(affected):
    rebuild_daily_aggregates(affected['dates'])
    rebuild_rollup_aggregates('task_daily_agg', 'task_weekly_agg', 'run_week_start', affected['weeks'])
    rebuild_rollup_aggregates('task_daily_agg', 'task_monthly_agg', 'run_month', affected['months'])
    rebuild_rollup_aggregates('task_daily_agg', 'task_yearly_agg', 'run_year', affected['years'])


def sync_timer_task_results_by_ids(task_result_ids):
    if not task_result_ids:
        return
    task_results = []
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        for batch in chunked(task_result_ids, SQLITE_MAX_VARIABLES):
            placeholders = ','.join('?' for _ in batch)
            rows = conn.execute(
                '''
                SELECT tr.id, tr.task_id, tr.execute_time, tr.fail_count, tr.pass_count,
                       t.name AS task_name, t.project, t.spider
                FROM task_result tr
                JOIN task t ON t.id = tr.task_id
                WHERE tr.id IN ({})
                '''.format(placeholders),
                batch
            ).fetchall()
            task_results.extend(dict(row) for row in rows)
    finally:
        conn.close()
    task_job_results_map = fetch_timer_task_result_details(task_result_ids)
    task_job_result_ids = [
        record['id']
        for task_result_id in task_job_results_map
        for record in task_job_results_map[task_result_id]
    ]
    timer_job_ids = [
        record.get('result')
        for task_result_id in task_job_results_map
        for record in task_job_results_map[task_result_id]
        if record.get('result')
    ]
    status_rows = fetch_status_rows_by_task_job_result_ids(task_job_result_ids)
    independent_fact_map = build_independent_fact_map_by_job_ids(timer_job_ids)
    facts = []
    for task_result in task_results:
        facts.append(build_timer_fact(
            task_result,
            task_job_results_map.get(task_result['id'], []),
            status_rows,
            independent_fact_map=independent_fact_map,
        ))
    upsert_execution_facts(facts)
    rebuild_aggregates(collect_affected_keys(facts))


def sync_timer_facts_batch():
    last_id = int(get_cursor('timer_task_result_id', '0'))
    task_results = fetch_timer_task_results_batch(last_id, FACT_SYNC_BATCH_SIZE)
    if not task_results:
        return 0
    task_result_ids = [row['id'] for row in task_results]
    task_job_results_map = fetch_timer_task_result_details(task_result_ids)
    task_job_result_ids = [
        record['id']
        for task_result_id in task_job_results_map
        for record in task_job_results_map[task_result_id]
    ]
    timer_job_ids = [
        record.get('result')
        for task_result_id in task_job_results_map
        for record in task_job_results_map[task_result_id]
        if record.get('result')
    ]
    status_rows = fetch_status_rows_by_task_job_result_ids(task_job_result_ids)
    independent_fact_map = build_independent_fact_map_by_job_ids(timer_job_ids)
    facts = []
    for task_result in task_results:
        facts.append(build_timer_fact(
            task_result,
            task_job_results_map.get(task_result['id'], []),
            status_rows,
            independent_fact_map=independent_fact_map,
        ))
    upsert_execution_facts(facts)
    rebuild_aggregates(collect_affected_keys(facts))
    set_cursor('timer_task_result_id', task_results[-1]['id'])
    return len(facts)


def refresh_recent_timer_task_results():
    cutoff = get_fact_refresh_cutoff()
    task_results = fetch_recent_timer_task_results(cutoff)
    if not task_results:
        return 0
    task_result_ids = [row['id'] for row in task_results]
    task_job_results_map = fetch_timer_task_result_details(task_result_ids)
    task_job_result_ids = [
        record['id']
        for task_result_id in task_job_results_map
        for record in task_job_results_map[task_result_id]
    ]
    timer_job_ids = [
        record.get('result')
        for task_result_id in task_job_results_map
        for record in task_job_results_map[task_result_id]
        if record.get('result')
    ]
    status_rows = fetch_status_rows_by_task_job_result_ids(task_job_result_ids)
    independent_fact_map = build_independent_fact_map_by_job_ids(timer_job_ids)
    facts = []
    for task_result in task_results:
        facts.append(build_timer_fact(
            task_result,
            task_job_results_map.get(task_result['id'], []),
            status_rows,
            independent_fact_map=independent_fact_map,
        ))
    upsert_execution_facts(facts)
    rebuild_aggregates(collect_affected_keys(facts))
    return len(facts)


def build_independent_fact(table_name, row):
    start_time = row.get('start') or row.get('create_time') or row.get('update_time')
    if not start_time:
        return None
    finish_time = row.get('finish') or row.get('update_time')
    status = 'failed'
    if row.get('status') == '2':
        status = 'success'
    elif row.get('status') == '1':
        status = 'running'
    is_timer_child = 0
    if row.get('spider') not in SPECIAL_INDEPENDENT_SPIDERS and (row.get('job') or '').startswith('task_'):
        is_timer_child = 1
    failure_reason = ''
    if status == 'running':
        failure_reason = '执行中'
    elif status == 'failed':
        failure_reason = '状态未知'
    return dict(
        source_type='independent',
        source_pk='%s:%s' % (table_name, row['id']),
        task_key=build_task_key('independent', None, row['spider']),
        task_id=None,
        task_name=SPECIAL_INDEPENDENT_NAMES.get(row['spider'], '%s / %s' % (row['project'], row['spider'])),
        project=row['project'],
        spider=row['spider'],
        job_id=row.get('job') or '',
        server=table_name,
        node=None,
        planned_time=None,
        start_time=start_time,
        finish_time=finish_time,
        run_date=format_date(start_time),
        run_week_start=get_week_start_string(start_time),
        run_month=get_month_string(start_time),
        run_year=get_year_int(start_time),
        status=status,
        scraped_items=row.get('items') if isinstance(row.get('items'), int) else None,
        failure_reason=failure_reason,
        is_timer_child=is_timer_child,
    )


def fetch_independent_rows_by_job_ids(job_ids):
    if not job_ids:
        return {}
    normalized_job_ids = sorted(set(job_id for job_id in job_ids if job_id))
    if not normalized_job_ids:
        return {}
    grouped = {}
    conn = connect_readonly(JOBS_DB)
    try:
        for _index, table_name in get_existing_job_tables():
            for batch in chunked(normalized_job_ids, SQLITE_MAX_VARIABLES):
                placeholders = ','.join('?' for _ in batch)
                rows = conn.execute(
                    '''
                    SELECT *
                    FROM "{table_name}"
                    WHERE deleted = '0' AND job IN ({placeholders})
                    ORDER BY update_time DESC, id DESC
                    '''.format(table_name=table_name, placeholders=placeholders),
                    batch
                ).fetchall()
                for row in rows:
                    record = dict(row)
                    job_id = record.get('job')
                    if not job_id or job_id in grouped:
                        continue
                    grouped[job_id] = (table_name, record)
    finally:
        conn.close()
    return grouped


def build_independent_fact_map_by_job_ids(job_ids):
    fact_map = {}
    for job_id, payload in fetch_independent_rows_by_job_ids(job_ids).items():
        table_name, row = payload
        fact = build_independent_fact(table_name, row)
        if fact:
            fact_map[job_id] = fact
    return fact_map


def sync_independent_job(job_id):
    """Incrementally persist one manual or timer-child job after completion."""
    payloads = fetch_independent_rows_by_job_ids([job_id])
    payload = payloads.get(job_id)
    if not payload:
        return 0
    table_name, row = payload
    fact = build_independent_fact(table_name, row)
    if not fact:
        return 0
    upsert_execution_facts([fact])
    rebuild_aggregates(collect_affected_keys([fact]))
    return 1


def merge_timer_with_independent_fact(timer_fact, independent_fact):
    if not independent_fact:
        return timer_fact
    merged = dict(timer_fact)
    merged['status'] = independent_fact.get('status') or merged['status']
    merged['start_time'] = independent_fact.get('start_time') or merged['start_time']
    merged['finish_time'] = choose_latest_time(
        merged.get('finish_time'),
        independent_fact.get('finish_time'),
        independent_fact.get('start_time'),
    )
    if isinstance(independent_fact.get('scraped_items'), int):
        merged['scraped_items'] = independent_fact['scraped_items']
    if independent_fact.get('failure_reason'):
        merged['failure_reason'] = independent_fact['failure_reason']
    return merged


def fetch_independent_rows_batch(table_name, last_id, limit):
    conn = connect_readonly(JOBS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM "{table_name}"
            WHERE id > ? AND deleted = '0'
            ORDER BY id ASC
            LIMIT ?
            '''.format(table_name=table_name),
            (last_id, limit)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def fetch_independent_rows_by_year_batch(table_name, year, last_id, limit):
    conn = connect_readonly(JOBS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM "{table_name}"
            WHERE id > ?
              AND deleted = '0'
              AND start >= ?
              AND start < ?
            ORDER BY id ASC
            LIMIT ?
            '''.format(table_name=table_name),
            (last_id, '%s-01-01 00:00:00' % year, '%s-01-01 00:00:00' % (year + 1), limit)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def fetch_recent_updated_independent_rows(table_name, cutoff):
    conn = connect_readonly(JOBS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM "{table_name}"
            WHERE deleted = '0' AND update_time >= ?
            ORDER BY id ASC
            '''.format(table_name=table_name),
            (cutoff,)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def load_running_execution_facts():
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM task_execution_fact
            WHERE status = 'running'
            ORDER BY updated_at DESC, fact_id DESC
            '''
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def finalize_non_running_execution_facts(facts):
    if not facts:
        return 0
    conn = connect_writable(DAILY_STATS_DB)
    try:
        for fact in facts:
            final_status = 'success' if fact.get('finish_time') else 'failed'
            failure_reason = ''
            if final_status == 'failed':
                failure_reason = fact.get('failure_reason') or '状态未知'
            conn.execute(
                '''
                UPDATE task_execution_fact
                SET status = ?, failure_reason = ?, updated_at = ?
                WHERE fact_id = ?
                ''',
                (final_status, failure_reason, now_string(), fact['fact_id'])
            )
        conn.commit()
    finally:
        conn.close()
    rebuild_aggregates(collect_affected_keys(facts))
    return len(facts)


def reconcile_running_execution_facts():
    facts = load_running_execution_facts()
    if not facts:
        return 0

    grouped = {}
    unresolved = 0
    for fact in facts:
        server = resolve_scrapyd_server(fact.get('server'))
        project = fact.get('project') or ''
        job_id = fact.get('job_id') or ''
        if not server or not project or not job_id:
            unresolved += 1
            continue
        grouped.setdefault((server, project), []).append(fact)

    stale_facts = []
    for (server, project), rows in grouped.items():
        running_job_ids, error_text = fetch_running_job_ids_from_scrapyd(server, project)
        if running_job_ids is None:
            print('[%s] reconcile skipped server=%s project=%s error=%s rows=%s' % (
                now_string(), server, project, error_text, len(rows)
            ))
            continue
        for fact in rows:
            if fact.get('job_id') not in running_job_ids:
                stale_facts.append(fact)

    updated_count = finalize_non_running_execution_facts(stale_facts)
    print('[%s] running reconcile checked=%s stale=%s unresolved=%s updated=%s db=%s' % (
        now_string(), len(facts), len(stale_facts), unresolved, updated_count, DAILY_STATS_DB
    ))
    return updated_count


def sync_independent_table(table_name):
    cursor_key = 'independent:%s:id' % table_name
    last_id = int(get_cursor(cursor_key, '0'))
    rows = fetch_independent_rows_batch(table_name, last_id, FACT_SYNC_BATCH_SIZE)
    facts = []
    for row in rows:
        fact = build_independent_fact(table_name, row)
        if fact:
            facts.append(fact)
    if facts:
        upsert_execution_facts(facts)
        linked_task_result_ids = fetch_timer_task_result_ids_by_job_ids([
            fact.get('job_id') for fact in facts if fact.get('job_id')
        ])
        if linked_task_result_ids:
            sync_timer_task_results_by_ids(linked_task_result_ids)
        rebuild_aggregates(collect_affected_keys(facts))
        set_cursor(cursor_key, rows[-1]['id'])
    return len(facts)


def sync_independent_priority_backfill_once():
    for year in PRIORITY_BACKFILL_YEARS:
        for _index, table_name in get_existing_job_tables():
            cursor_key = 'independent_priority:%s:%s:id' % (table_name, year)
            done_key = 'independent_priority:%s:%s:done' % (table_name, year)
            if get_cursor(done_key, '0') == '1':
                continue
            last_id = int(get_cursor(cursor_key, '0'))
            rows = fetch_independent_rows_by_year_batch(table_name, year, last_id, FACT_SYNC_BATCH_SIZE)
            if not rows:
                set_cursor(done_key, '1')
                continue
            facts = []
            for row in rows:
                fact = build_independent_fact(table_name, row)
                if fact:
                    facts.append(fact)
            if facts:
                upsert_execution_facts(facts)
                linked_task_result_ids = fetch_timer_task_result_ids_by_job_ids([
                    fact.get('job_id') for fact in facts if fact.get('job_id')
                ])
                if linked_task_result_ids:
                    sync_timer_task_results_by_ids(linked_task_result_ids)
                rebuild_aggregates(collect_affected_keys(facts))
            set_cursor(cursor_key, rows[-1]['id'])
            print('[%s] priority independent backfill year=%s table=%s rows=%s' % (
                now_string(), year, table_name, len(facts)
            ))
            return len(facts)
        print('[%s] priority independent backfill year=%s completed' % (now_string(), year))
    return 0


def refresh_recent_independent_rows():
    cutoff = get_fact_refresh_cutoff()
    facts = []
    for _index, table_name in get_existing_job_tables():
        rows = fetch_recent_updated_independent_rows(table_name, cutoff)
        for row in rows:
            fact = build_independent_fact(table_name, row)
            if fact:
                facts.append(fact)
    if not facts:
        return 0
    upsert_execution_facts(facts)
    linked_task_result_ids = fetch_timer_task_result_ids_by_job_ids([
        fact.get('job_id') for fact in facts if fact.get('job_id')
    ])
    if linked_task_result_ids:
        sync_timer_task_results_by_ids(linked_task_result_ids)
    rebuild_aggregates(collect_affected_keys(facts))
    return len(facts)


def sync_analytics_once():
    timer_count = sync_timer_facts_batch()
    timer_refresh_count = refresh_recent_timer_task_results()
    independent_priority_count = sync_independent_priority_backfill_once()
    independent_count = 0
    if independent_priority_count == 0:
        for _index, table_name in get_existing_job_tables():
            independent_count += sync_independent_table(table_name)
    refreshed_count = refresh_recent_independent_rows()
    reconciled_count = reconcile_running_execution_facts()
    print('[%s] analytics synced timer=%s timer_refresh=%s independent_priority=%s independent=%s refreshed=%s reconciled=%s db=%s' % (
        now_string(), timer_count, timer_refresh_count, independent_priority_count, independent_count, refreshed_count, reconciled_count, DAILY_STATS_DB
    ))


def reconcile_pending_results():
    """Repair missed events and retry stats that were unavailable earlier."""
    records = fetch_recent_task_job_results()
    seed_status_rows(records)
    pending_rows = load_pending_rows()
    for row in pending_rows:
        mark_sync_status(row['task_job_result_id'], 'in_progress')
        items, error_text = fetch_remote_items(row)
        mark_result(row['task_job_result_id'], items, error_text)
        if SYNC_REQUEST_INTERVAL:
            time.sleep(SYNC_REQUEST_INTERVAL)
    return len(pending_rows)


def reconcile_once():
    sync_analytics_once()
    return reconcile_pending_results()


def reconcile_loop():
    while True:
        try:
            reconcile_once()
        except Exception:
            logging.getLogger(__name__).exception('Daily-stats reconciliation failed')
        time.sleep(SYNC_DISCOVERY_INTERVAL)


def start_reconcile_worker():
    ensure_status_db()
    reset_inflight_rows()
    print('[%s] daily stats reconcile started db=%s interval=%ss request_interval=%ss' % (
        now_string(), DAILY_STATS_DB, SYNC_DISCOVERY_INTERVAL, SYNC_REQUEST_INTERVAL
    ))
    global _background_thread
    with _background_lock:
        if _background_thread and _background_thread.is_alive():
            return _background_thread
        _background_thread = threading.Thread(
            target=reconcile_loop, name='daily-stats-reconcile', daemon=True
        )
        _background_thread.start()
        return _background_thread
