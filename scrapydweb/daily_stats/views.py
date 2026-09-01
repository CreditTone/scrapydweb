# coding: utf-8
import json
import os
import urllib.parse
import hashlib
import hmac
import base64
from datetime import datetime, timedelta

from flask import Blueprint, abort, current_app, flash, jsonify, render_template, request, send_file, url_for
try:
    import markdown as markdown_lib
except ImportError:
    markdown_lib = None

from .common import (APSCHEDULER_DB, DAILY_STATS_DB, DATABASE_DIR, JOBS_DB, SCRAPYD_SERVERS, SETTINGS_PATH,
                    TIMER_TASKS_DB, build_task_key, connect_readonly, connect_writable, ensure_status_db, format_datetime,
                    get_fire_times_by_day, get_existing_job_tables, get_job_tables, get_manual_job_status,
                    load_aggregate_rows, load_job_states, load_status_rows, load_task_job_results,
                    load_task_results_by_task, load_tasks)


bp = Blueprint('daily_stats', __name__, template_folder='templates', url_prefix='/stats')
if not os.getenv("GZ_DEBUG"):
    SPIDER_DOCS_DIR = os.environ.get('SPIDER_DOCS_DIR', '/root/maxcrawler/docs')
else:
    SPIDER_DOCS_DIR = os.environ.get('SPIDER_DOCS_DIR', '/Users/stephen/maxcrawler/docs')

SPIDER_MONITOR_MAIL_ENABLED = (
    os.environ.get('SPIDER_MONITOR_MAIL_ENABLED', '1').strip().lower() not in ('0', 'false', 'no')
    and not os.getenv("GZ_DEBUG")
)
SPIDER_MONITOR_MAIL_URL = os.environ.get(
    'SPIDER_MONITOR_MAIL_URL',
    'http://misc-commapi.guazi.com/misc/contact/sendMail'
).strip()
SPIDER_MONITOR_MAIL_APPKEY = os.environ.get('SPIDER_MONITOR_MAIL_APPKEY', '').strip()
SPIDER_MONITOR_MAIL_APP_SECRET = os.environ.get('SPIDER_MONITOR_MAIL_APP_SECRET', '').strip()
SPIDER_MONITOR_MAIL_TO = os.environ.get(
    'SPIDER_MONITOR_MAIL_TO',
    'js.search_crawler@guazi.com,qinbenyuan@guazi.com,zhangbohao2@guazi.com'
).strip()
DAILY_STATS_PUBLIC_BASE_URL = os.environ.get('DAILY_STATS_PUBLIC_BASE_URL', 'http://10.16.12.155').strip().rstrip('/')


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

WEEKLY_REPORT_INCLUDE_TIMER_CHILD_SPIDERS = set([
    'che168_usedcar_car',
    'dongchedi_usedcar_car',
])

WEEKLY_REPORT_SPECIAL_NAMES = {
    'dongchedi_usedcar_car': '懂车帝二手车源任务',
    'che168_usedcar_car': '汽车之家二手车源任务',
}


def is_excluded_report_spider(spider):
    spider = (spider or '').strip()
    return spider.startswith('room_util')


def clean_task_name(name):
    return (
        (name or '')
        .replace(' - edit', '')
        .replace(' - 添加代理', '')
        .strip()
    )


def build_spider_name_map(task_rows):
    spider_name_map = {}
    for task in sorted(task_rows, key=lambda item: item.get('id', 0)):
        spider = (task.get('spider') or '').strip()
        name = clean_task_name(task.get('name'))
        if spider and name and spider not in spider_name_map:
            spider_name_map[spider] = name
    return spider_name_map


def resolve_display_name(spider, project=None, fallback_name=None, spider_name_map=None):
    spider_name_map = spider_name_map or {}
    if spider_name_map.get(spider):
        return spider_name_map[spider]
    if WEEKLY_REPORT_SPECIAL_NAMES.get(spider):
        return WEEKLY_REPORT_SPECIAL_NAMES[spider]
    fallback_name = clean_task_name(fallback_name)
    if fallback_name:
        return fallback_name
    if project and spider:
        return '%s / %s' % (project, spider)
    return spider or '-'


def get_independent_run_type(spider, spider_name_map=None):
    spider_name_map = spider_name_map or {}
    if spider_name_map.get(spider):
        return '爬虫触发'
    return '独立任务'


def find_spider_doc_file(spider):
    spider = (spider or '').strip()
    if not spider or not os.path.isdir(SPIDER_DOCS_DIR):
        return None
    for extension in ('md', 'pdf'):
        doc_path = os.path.join(SPIDER_DOCS_DIR, '%s.%s' % (spider, extension))
        if os.path.isfile(doc_path):
            return doc_path
    return None


def render_markdown_content(content):
    if not markdown_lib:
        return None
    return markdown_lib.markdown(
        content,
        extensions=['fenced_code', 'tables'],
        output_format='html5',
    )


def format_duration_between(start_time_text, finish_time_text):
    if not start_time_text or not finish_time_text:
        return 'N/A'
    try:
        start_time = datetime.strptime(start_time_text, '%Y-%m-%d %H:%M:%S')
        finish_time = datetime.strptime(finish_time_text, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return 'N/A'
    if finish_time < start_time:
        return 'N/A'
    total_seconds = int((finish_time - start_time).total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return '%d:%02d:%02d' % (hours, minutes, seconds)
    return '%02d:%02d' % (minutes, seconds)


def get_duration_seconds(start_time_text, finish_time_text):
    if not start_time_text or not finish_time_text:
        return None
    try:
        start_time = datetime.strptime(start_time_text, '%Y-%m-%d %H:%M:%S')
        finish_time = datetime.strptime(finish_time_text, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None
    if finish_time < start_time:
        return None
    return int((finish_time - start_time).total_seconds())


def get_median_number(values):
    normalized_values = sorted([
        float(value) for value in values
        if isinstance(value, (int, float)) and value > 0
    ])
    if not normalized_values:
        return 0.0
    middle_index = len(normalized_values) // 2
    if len(normalized_values) % 2 == 1:
        return normalized_values[middle_index]
    return (normalized_values[middle_index - 1] + normalized_values[middle_index]) / 2.0


def format_duration_seconds_text(total_seconds):
    if not isinstance(total_seconds, (int, float)) or total_seconds <= 0:
        return '-'
    total_seconds = int(total_seconds)
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts = []
    if days > 0:
        parts.append('%d天' % days)
    if hours > 0:
        parts.append('%d小时' % hours)
    if minutes > 0:
        parts.append('%d分' % minutes)
    if not parts and seconds > 0:
        parts.append('%d秒' % seconds)
    return ''.join(parts[:2]) or '0秒'


def get_trigger_interval_seconds(trigger, sample_start=None, max_samples=8):
    if trigger is None:
        return None
    current_time = sample_start or datetime.now()
    timezone = getattr(trigger, 'timezone', None)
    if timezone:
        if hasattr(timezone, 'localize'):
            current_time = timezone.localize(current_time)
        else:
            current_time = current_time.replace(tzinfo=timezone)
    previous_fire_time = None
    fire_times = []
    for _ in range(max_samples):
        next_fire_time = trigger.get_next_fire_time(previous_fire_time, current_time)
        if not next_fire_time:
            break
        fire_times.append(next_fire_time)
        previous_fire_time = next_fire_time
        current_time = next_fire_time + timedelta(microseconds=1)
    if len(fire_times) < 2:
        return None
    intervals = []
    for index in range(1, len(fire_times)):
        interval_seconds = int((fire_times[index] - fire_times[index - 1]).total_seconds())
        if interval_seconds > 0:
            intervals.append(interval_seconds)
    if not intervals:
        return None
    return min(intervals)


def load_spider_expected_interval(spider):
    spider = (spider or '').strip()
    if not spider:
        return dict(seconds=None, text='-')
    job_states = load_job_states()
    interval_candidates = []
    for task in load_tasks():
        if (task.get('spider') or '').strip() != spider:
            continue
        job_state = job_states.get(str(task.get('id')))
        trigger = job_state.get('trigger') if job_state else None
        interval_seconds = get_trigger_interval_seconds(trigger)
        if isinstance(interval_seconds, int) and interval_seconds > 0:
            interval_candidates.append(interval_seconds)
    if not interval_candidates:
        return dict(seconds=None, text='-')
    expected_interval_seconds = min(interval_candidates)
    return dict(
        seconds=expected_interval_seconds,
        text=format_duration_seconds_text(expected_interval_seconds),
    )


def format_task_schedule(task):
    trigger = task.get('trigger') or '-'
    if trigger != 'cron':
        return trigger
    return 'minute={minute}, hour={hour}, day={day}, month={month}, day_of_week={day_of_week}'.format(
        minute=task.get('minute') or '*',
        hour=task.get('hour') or '*',
        day=task.get('day') or '*',
        month=task.get('month') or '*',
        day_of_week=task.get('day_of_week') or '*',
    )


def load_spider_task_configs(spider):
    rows = []
    for task in load_tasks():
        if task.get('spider') != spider:
            continue
        rows.append(dict(
            id=task.get('id'),
            name=clean_task_name(task.get('name')) or '-',
            project=task.get('project') or '-',
            version=task.get('version') or '-',
            trigger=task.get('trigger') or '-',
            schedule=format_task_schedule(task),
            selected_nodes=task.get('selected_nodes') or '-',
            settings_arguments=task.get('settings_arguments') or '-',
            start_date=task.get('start_date') or '-',
            end_date=task.get('end_date') or '-',
            timezone=task.get('timezone') or '-',
            update_time=format_datetime(task.get('update_time')) if task.get('update_time') else '-',
        ))
    return rows


@bp.app_context_processor
def inject_template_helpers():
    return dict(
        spider_doc_url=lambda spider: (
            url_for('daily_stats.spider_doc', spider=spider) if find_spider_doc_file(spider) else None
        ),
    )


def parse_selected_date():
    date_string = request.args.get('date', '').strip()
    if not date_string:
        return datetime.now()
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        flash('日期格式错误，已自动切回今天。', 'warning')
        return datetime.now()


def parse_selected_week_date():
    return parse_selected_date()


def parse_selected_year_date():
    return parse_selected_date()


def parse_taskstats_date(value):
    value = (value or '').strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d')
    except ValueError:
        return None


def format_big_number(value):
    if not isinstance(value, int):
        return '0'
    return '{:,}'.format(value)


def load_dashboard_summary():
    ensure_status_db()
    excluded_condition = "spider NOT LIKE 'room_util%'"
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        total_scraped_items = conn.execute(
            '''
            SELECT COALESCE(SUM(scraped_items), 0) AS value
            FROM task_execution_fact
            WHERE {excluded_condition}
              AND scraped_items IS NOT NULL
              AND scraped_items > 0
            '''.format(excluded_condition=excluded_condition)
        ).fetchone()['value']
        historical_task_count = conn.execute(
            '''
            SELECT COUNT(*) AS value
            FROM (
                SELECT DISTINCT
                    CASE
                        WHEN TRIM(COALESCE(job_id, '')) = '' THEN source_type || ':' || source_pk
                        ELSE job_id
                    END AS execution_key
                FROM task_execution_fact
                WHERE {excluded_condition}
            )
            '''.format(excluded_condition=excluded_condition)
        ).fetchone()['value']
        spider_count = conn.execute(
            '''
            SELECT COUNT(DISTINCT spider) AS value
            FROM task_execution_fact
            WHERE {excluded_condition}
              AND TRIM(COALESCE(spider, '')) != ''
            '''.format(excluded_condition=excluded_condition)
        ).fetchone()['value']
        running_task_count = conn.execute(
            '''
            SELECT COUNT(*) AS value
            FROM (
                SELECT DISTINCT
                    CASE
                        WHEN TRIM(COALESCE(job_id, '')) = '' THEN source_type || ':' || source_pk
                        ELSE job_id
                    END AS execution_key
                FROM task_execution_fact
                WHERE {excluded_condition}
                  AND status = 'running'
            )
            '''.format(excluded_condition=excluded_condition)
        ).fetchone()['value']
    finally:
        conn.close()

    timer_task_count = len([
        task for task in load_tasks()
        if not is_excluded_report_spider(task.get('spider'))
    ])
    cluster_node_count = len(set(server for _group, server, _auth in SCRAPYD_SERVERS))
    cards = [
        dict(label='历史累计抓取数量', value=format_big_number(int(total_scraped_items or 0))),
        dict(label='历史任务执行次数', value=format_big_number(int(historical_task_count or 0))),
        dict(label='Spider 总数', value=format_big_number(int(spider_count or 0))),
        dict(label='定时任务总数', value=format_big_number(int(timer_task_count or 0))),
        dict(label='集群节点数', value=format_big_number(int(cluster_node_count or 0))),
        dict(label='当前运行任务数', value=format_big_number(int(running_task_count or 0))),
    ]
    return cards


def get_week_start(selected_date):
    day_start = selected_date.replace(hour=0, minute=0, second=0, microsecond=0)
    report_weekday = 3  # Thursday
    days_since_report_start = (day_start.weekday() - report_weekday) % 7
    return day_start - timedelta(days=days_since_report_start)


def build_week_options(selected_date, previous_weeks=24, next_weeks=12):
    selected_week_start = get_week_start(selected_date)
    options = []
    for offset in range(-previous_weeks, next_weeks + 1):
        week_start = selected_week_start + timedelta(days=offset * 7)
        week_end = week_start + timedelta(days=6)
        options.append(dict(
            value=week_start.strftime('%Y-%m-%d'),
            label='%s ~ %s' % (
                week_start.strftime('%Y-%m-%d'),
                week_end.strftime('%Y-%m-%d'),
            ),
            is_selected=(offset == 0),
        ))
    return options


def get_fire_times_between(trigger, start_time, end_time):
    if trigger is None:
        return []
    timezone = getattr(trigger, 'timezone', None)
    if timezone:
        if hasattr(timezone, 'localize'):
            start_time = timezone.localize(start_time)
            end_time = timezone.localize(end_time)
        else:
            start_time = start_time.replace(tzinfo=timezone)
            end_time = end_time.replace(tzinfo=timezone)
    fire_times = []
    previous_fire_time = None
    current_time = start_time
    for _ in range(10000):
        next_fire_time = trigger.get_next_fire_time(previous_fire_time, current_time)
        if not next_fire_time or next_fire_time >= end_time:
            break
        fire_times.append(next_fire_time)
        previous_fire_time = next_fire_time
        current_time = next_fire_time + timedelta(microseconds=1)
    return fire_times


def get_timer_scraped_items(task_result, task_job_results_map, status_rows):
    records = task_job_results_map.get(task_result['id'], [])
    total_items = 0
    found = False
    for record in records:
        if record['status'] != 'ok':
            continue
        status_row = status_rows.get(record['id'])
        if not status_row:
            continue
        items = status_row.get('scraped_items')
        if isinstance(items, int):
            total_items += items
            found = True
    return total_items if found else 'N/A'


def count_timer_scraped_items(task_result, task_job_results_map, status_rows):
    records = task_job_results_map.get(task_result['id'], [])
    total_items = 0
    for record in records:
        if record['status'] != 'ok':
            continue
        status_row = status_rows.get(record['id'])
        if not status_row:
            continue
        items = status_row.get('scraped_items')
        if isinstance(items, int):
            total_items += items
    return total_items


def mark_lazy_sync_candidates(candidates):
    if not candidates:
        return
    ensure_status_db()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = connect_writable(DAILY_STATS_DB)
    try:
        for row in candidates:
            conn.execute(
                '''
                INSERT OR IGNORE INTO task_job_status (
                    task_job_result_id, task_id, task_result_id, job_id, server, project, spider,
                    run_time, scraped_items, sync_status, attempt_count, last_error, next_retry_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 'pending', 0, NULL, NULL, ?)
                ''',
                (
                    row['task_job_result_id'],
                    row['task_id'],
                    row['task_result_id'],
                    row['job_id'],
                    row['server'],
                    row['project'],
                    row['spider'],
                    row['run_time'],
                    now,
                )
            )
            conn.execute(
                '''
                UPDATE task_job_status
                SET sync_status = 'pending',
                    next_retry_at = NULL,
                    updated_at = ?
                WHERE task_job_result_id = ?
                  AND (scraped_items IS NULL OR sync_status != 'success')
                ''',
                (now, row['task_job_result_id'])
            )
        conn.commit()
    finally:
        conn.close()


def get_failure_reason(record):
    if record.get('status') == 'ok':
        return ''
    error_text = record.get('result') or ''
    for pattern, label in FAILURE_PATTERNS:
        if pattern in error_text:
            return label
    for line in error_text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:80]
    status_code = record.get('status_code')
    return '状态码 %s' % status_code if status_code else '未知失败'


def format_percent(numerator, denominator):
    if not denominator:
        return '0%'
    return '%.1f%%' % (float(numerator) * 100.0 / float(denominator))


def format_week_change(current_total, previous_total):
    if previous_total == 0:
        if current_total == 0:
            return '0%'
        return '上周为0'
    diff = current_total - previous_total
    percent = float(diff) * 100.0 / float(previous_total)
    sign = '+' if percent > 0 else ''
    return '%s%.1f%%' % (sign, percent)


def get_week_change_class(week_change):
    if week_change.startswith('+'):
        return 'positive'
    if week_change.startswith('-'):
        return 'negative'
    return 'neutral'


def merge_failure_reasons(*reasons):
    merged = []
    for reason in reasons:
        if not reason or reason == '-':
            continue
        if reason not in merged:
            merged.append(reason)
    if not merged:
        return '-'
    return '；'.join(merged)


def get_merged_run_type(row):
    source_types = row.get('source_types') or set()
    has_timer = 'timer' in source_types
    has_independent = 'independent' in source_types
    if has_timer and has_independent:
        return '定时+爬虫触发'
    if has_timer:
        return '定时'
    return row.get('independent_run_type') or '独立任务'


def merge_weekly_rows(rows):
    merged_rows = {}
    for row in rows:
        key = row['spider']
        current_total = row.pop('_current_scraped_total', row['scraped_total'])
        previous_total = row.pop('_previous_scraped_total', 0)
        success_count = row.pop('_success_count', 0)
        running_count = row.pop('_running_count', 0)
        source_type = row.pop('_source_type', 'independent')
        target = merged_rows.get(key)
        if not target:
            row['_current_scraped_total'] = current_total
            row['_previous_scraped_total'] = previous_total
            row['_success_count'] = success_count
            row['_running_count'] = running_count
            row['_source_types'] = set([source_type])
            row['_independent_run_type'] = row['run_type'] if source_type == 'independent' else None
            merged_rows[key] = row
            continue

        target['_current_scraped_total'] += current_total
        target['_previous_scraped_total'] += previous_total
        target['_success_count'] += success_count
        target['_running_count'] += running_count
        target['_source_types'].add(source_type)
        if source_type == 'independent' and not target.get('_independent_run_type'):
            target['_independent_run_type'] = row['run_type']
        target['actual_execute'] += row['actual_execute']
        target['scraped_total'] += row['scraped_total']
        target['failure_reason'] = merge_failure_reasons(target['failure_reason'], row['failure_reason'])
        if not isinstance(target['should_execute'], int):
            target['should_execute'] = row['should_execute']

    result = []
    for row in merged_rows.values():
        source_types = row.pop('_source_types')
        success_count = row.pop('_success_count')
        running_count = row.pop('_running_count')
        current_total = row.pop('_current_scraped_total')
        previous_total = row.pop('_previous_scraped_total')
        independent_run_type = row.pop('_independent_run_type')
        row['run_type'] = get_merged_run_type(dict(
            source_types=source_types,
            independent_run_type=independent_run_type,
        ))
        row['success_rate'] = format_percent(success_count, row['actual_execute'])
        row['average_daily_items'] = '%.1f' % (float(row['scraped_total']) / 7.0)
        row['week_change'] = format_week_change(current_total, previous_total)
        row['week_change_class'] = get_week_change_class(row['week_change'])
        row['highlight_danger'] = (
            running_count == 0 and row['actual_execute'] > 0 and success_count < row['actual_execute']
        )
        result.append(row)
    return result


def merge_annual_rows(rows, elapsed_days, elapsed_months):
    merged_rows = {}
    for row in rows:
        key = row['spider']
        current_total = row.pop('_current_scraped_total', row['scraped_total'])
        previous_total = row.pop('_previous_scraped_total', 0)
        success_count = row.pop('_success_count', 0)
        running_count = row.pop('_running_count', 0)
        source_type = row.pop('_source_type', 'independent')
        latest_execute_time = row.get('latest_execute_time')
        target = merged_rows.get(key)
        if not target:
            row['_current_scraped_total'] = current_total
            row['_previous_scraped_total'] = previous_total
            row['_success_count'] = success_count
            row['_running_count'] = running_count
            row['_source_types'] = set([source_type])
            row['_independent_run_type'] = row['run_type'] if source_type == 'independent' else None
            merged_rows[key] = row
            continue

        target['_current_scraped_total'] += current_total
        target['_previous_scraped_total'] += previous_total
        target['_success_count'] += success_count
        target['_running_count'] += running_count
        target['_source_types'].add(source_type)
        if source_type == 'independent' and not target.get('_independent_run_type'):
            target['_independent_run_type'] = row['run_type']
        target['actual_execute'] += row['actual_execute']
        target['scraped_total'] += row['scraped_total']
        target['failure_reason'] = merge_failure_reasons(target['failure_reason'], row['failure_reason'])
        if latest_execute_time and (not target.get('latest_execute_time') or latest_execute_time > target['latest_execute_time']):
            target['latest_execute_time'] = latest_execute_time
        if not isinstance(target['should_execute'], int):
            target['should_execute'] = row['should_execute']

    result = []
    for row in merged_rows.values():
        source_types = row.pop('_source_types')
        success_count = row.pop('_success_count')
        running_count = row.pop('_running_count')
        current_total = row.pop('_current_scraped_total')
        previous_total = row.pop('_previous_scraped_total')
        independent_run_type = row.pop('_independent_run_type')
        row['run_type'] = get_merged_run_type(dict(
            source_types=source_types,
            independent_run_type=independent_run_type,
        ))
        row['success_rate'] = format_percent(success_count, row['actual_execute'])
        row['average_daily_items'] = format_average_value(row['scraped_total'], elapsed_days)
        row['average_monthly_items'] = format_average_value(row['scraped_total'], elapsed_months)
        row['year_change'] = format_year_change(current_total, previous_total)
        row['year_change_class'] = get_week_change_class(row['year_change'])
        row['highlight_danger'] = (
            running_count == 0 and row['actual_execute'] > 0 and success_count < row['actual_execute']
        )
        result.append(row)
    return result


def load_aggregate_map(table_name, period_column, period_value):
    rows = load_aggregate_rows(table_name, period_column, period_value)
    return dict((row['task_key'], row) for row in rows)


def build_scraped_totals_payload(table_name, period_column, period_value):
    aggregate_map = load_aggregate_map(table_name, period_column, period_value)
    row_totals = {}
    summary_total = 0
    for task_key, row in aggregate_map.items():
        scraped_total = row['scraped_items_total'] or 0
        row_totals[task_key] = scraped_total
        summary_total += scraped_total
    return dict(
        summary_total_scraped_items=summary_total,
        row_totals=row_totals,
        updated_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    )


def build_rows_scraped_totals_payload(rows):
    row_totals = {}
    summary_total = 0
    for row in rows:
        task_key = row.get('task_key')
        scraped_total = row.get('scraped_total') or 0
        if task_key:
            row_totals[task_key] = scraped_total
        summary_total += scraped_total
    return dict(
        summary_total_scraped_items=summary_total,
        row_totals=row_totals,
        updated_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    )


def load_task_recent_execution_stats(spider, limit=100, start_date=None, end_date=None):
    spider = (spider or '').strip()
    if not spider:
        return None

    spider_name_map = build_spider_name_map(load_tasks())
    expected_interval = load_spider_expected_interval(spider)

    where_clauses = ['spider = ?']
    params = [spider]
    if start_date:
        where_clauses.append("COALESCE(start_time, planned_time, finish_time, created_at) >= ?")
        params.append(start_date.strftime('%Y-%m-%d 00:00:00'))
    if end_date:
        where_clauses.append("COALESCE(start_time, planned_time, finish_time, created_at) < ?")
        params.append((end_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00'))
    limit_clause = ''
    if limit and not start_date and not end_date:
        limit_clause = '\n            LIMIT ?'
        params.append(int(limit))

    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM task_execution_fact
            WHERE {where_sql}
            ORDER BY
              COALESCE(start_time, planned_time, finish_time, created_at) DESC,
              fact_id DESC
            {limit_clause}
            '''.format(
                where_sql=' AND '.join(where_clauses),
                limit_clause=limit_clause,
            ),
            params
        ).fetchall()
    finally:
        conn.close()
    deduped_rows = []
    seen_job_ids = set()
    pending_without_job_id = []
    for row in rows:
        record = dict(row)
        job_id = (record.get('job_id') or '').strip()
        if not job_id:
            pending_without_job_id.append(record)
            continue
        if job_id in seen_job_ids:
            continue
        seen_job_ids.add(job_id)
        deduped_rows.append(record)
    deduped_rows.extend(pending_without_job_id)
    if limit:
        rows = deduped_rows[:int(limit)]
    else:
        rows = deduped_rows
    rows = list(reversed(rows))

    coverage_point_map = dict(
        (item.get('job_id'), item)
        for item in load_task_recent_coverage_points(
            spider,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
        )
        if item.get('job_id')
    )

    points = []
    executions = []
    total_scraped = 0
    success_count = 0
    running_count = 0
    max_scraped = None
    min_scraped = None

    for index, record in enumerate(rows, start=1):
        scraped_items = normalize_optional_int(record.get('scraped_items'))
        if scraped_items == 0:
            continue
        start_time_text = format_datetime(record.get('start_time'))
        finish_time_text = format_datetime(record.get('finish_time'))
        duration_seconds = get_duration_seconds(start_time_text, finish_time_text)
        if isinstance(scraped_items, int) and scraped_items > 0:
            total_scraped += scraped_items
        if record.get('status') == 'success':
            success_count += 1
        if record.get('status') == 'running':
            running_count += 1
        if isinstance(scraped_items, int):
            if max_scraped is None or scraped_items > max_scraped:
                max_scraped = scraped_items
            if min_scraped is None or scraped_items < min_scraped:
                min_scraped = scraped_items

        display_time = (
            start_time_text
            or format_datetime(record.get('planned_time'))
            or finish_time_text
            or '-'
        )
        axis_date = display_time[:10] if display_time and display_time != '-' else '-'
        point_label = record.get('job_id') or ('#%s' % index)
        points.append(dict(
            index=len(executions) + 1,
            label=point_label,
            short_label=str(len(executions) + 1),
            axis_date=axis_date,
            scraped_items=scraped_items,
            duration_seconds=duration_seconds if isinstance(duration_seconds, int) and duration_seconds >= 0 else None,
            duration_text=format_duration_between(start_time_text, finish_time_text),
            status=record.get('status') or '-',
            display_time=display_time,
            display_timestamp=display_time,
            run_type='定时' if record.get('source_type') == 'timer' else get_independent_run_type(
                spider, spider_name_map=spider_name_map
            ),
        ))
        executions.append(dict(
            index=len(executions) + 1,
            job_id=record.get('job_id') or '-',
            run_type='定时' if record.get('source_type') == 'timer' else get_independent_run_type(
                spider, spider_name_map=spider_name_map
            ),
            status=record.get('status') or '-',
            scraped_items=scraped_items if scraped_items is not None else '-',
            coverage_rate=(
                coverage_point_map.get(record.get('job_id'), {}).get('coverage_rate_text') or '-'
            ),
            start_time=start_time_text,
            finish_time=finish_time_text,
            duration=format_duration_between(start_time_text, finish_time_text),
        ))

    task_name = resolve_display_name(
        spider,
        project=rows[0]['project'] if rows else None,
        fallback_name=rows[0]['task_name'] if rows else None,
        spider_name_map=spider_name_map,
    )
    summary = dict(
        task_name=task_name,
        spider=spider,
        execution_count=len(executions),
        total_scraped_items=total_scraped,
        success_count=success_count,
        running_count=running_count,
        success_rate=format_percent(success_count, len(executions)),
        max_scraped_items=max_scraped or 0,
        min_scraped_items=min_scraped or 0,
        expected_interval_seconds=expected_interval['seconds'],
        expected_interval_text=expected_interval['text'],
        query_mode='range' if (start_date or end_date) else 'recent',
    )
    return dict(
        summary=summary,
        points=points,
        executions=executions,
        coverage_points=list(coverage_point_map.values()),
    )


def load_task_recent_coverage_points(spider, limit=100, start_date=None, end_date=None):
    spider = (spider or '').strip()
    if not spider:
        return []
    where_clauses = [
        'spider_name = ?',
        "status = 'close'",
        'total_nums IS NOT NULL',
        'total_nums > 0',
        'items_nums IS NOT NULL',
    ]
    params = [spider]
    if start_date:
        where_clauses.append('event_time >= ?')
        params.append(start_date.strftime('%Y-%m-%d 00:00:00'))
    if end_date:
        where_clauses.append('event_time < ?')
        params.append((end_date + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00'))
    limit_clause = ''
    if not start_date and not end_date:
        limit_clause = '\n            LIMIT ?'
        params.append(int(limit))
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT spider_name, job_id, total_nums, items_nums, event_time
            FROM spider_monitor_coverage
            WHERE {where_sql}
            ORDER BY COALESCE(create_time_ms, 0) DESC, record_id DESC
            {limit_clause}
            '''.format(
                where_sql=' AND '.join(where_clauses),
                limit_clause=limit_clause,
            ),
            params
        ).fetchall()
    finally:
        conn.close()
    rows = list(reversed([dict(row) for row in rows]))
    points = []
    for index, record in enumerate(rows, start=1):
        total_nums = normalize_optional_int(record.get('total_nums'))
        items_nums = normalize_optional_int(record.get('items_nums'))
        coverage_rate = format_spider_monitor_rate(total_nums, items_nums)
        if coverage_rate is None:
            continue
        display_time = record.get('event_time') or '-'
        points.append(dict(
            index=index,
            job_id=record.get('job_id') or '-',
            label=record.get('job_id') or ('#%s' % index),
            short_label=str(index),
            axis_date=display_time[:10] if display_time and display_time != '-' else '-',
            coverage_rate=coverage_rate,
            coverage_rate_text='%.2f%%' % coverage_rate,
            total_nums=total_nums,
            items_nums=items_nums,
            display_time=display_time,
            display_timestamp=display_time,
            status='close',
            run_type='抓全率',
        ))
    return points


def load_cross_day_running_rows(day_start, day_end, spider_name_map=None):
    spider_name_map = spider_name_map or {}
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM task_execution_fact
            WHERE is_timer_child = 0
              AND start_time IS NOT NULL
              AND start_time < ?
              AND run_date < ?
              AND (
                    status = 'running'
                    OR finish_time IS NULL
                    OR finish_time >= ?
              )
            ORDER BY start_time ASC, spider ASC
            ''',
            (
                day_end.strftime('%Y-%m-%d %H:%M:%S'),
                day_start.strftime('%Y-%m-%d'),
                day_start.strftime('%Y-%m-%d %H:%M:%S'),
            )
        ).fetchall()
    finally:
        conn.close()

    result = []
    for row in rows:
        record = dict(row)
        if is_excluded_report_spider(record.get('spider')):
            continue
        is_running = record.get('status') == 'running'
        status_text = '执行中' if is_running else '跨日执行结束'
        status_class = 'warning' if record.get('status') == 'running' else 'normal'
        result.append(dict(
            name=resolve_display_name(
                record.get('spider'),
                project=record.get('project'),
                fallback_name=record.get('task_name'),
                spider_name_map=spider_name_map,
            ),
            spider=record.get('spider') or '-',
            job_id=record.get('job_id') or '-',
            source_type=record.get('source_type') or '-',
            start_time=format_datetime(record.get('start_time')),
            scraped_items=record.get('scraped_items') if isinstance(record.get('scraped_items'), int) else 'N/A',
            finish_time='N/A' if is_running else (
                format_datetime(record.get('finish_time')) if record.get('finish_time') else 'N/A'
            ),
            status_text=status_text,
            status_class=status_class,
        ))
    return result


def normalize_optional_int(value):
    if value in (None, ''):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def format_timestamp_ms(value):
    timestamp_ms = normalize_optional_int(value)
    if timestamp_ms is None:
        return None
    try:
        return datetime.fromtimestamp(float(timestamp_ms) / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    except (OverflowError, OSError, ValueError):
        return None


def build_spider_monitor_mail_signature(app_secret, params):
    encoded_pairs = []
    for key in sorted(params.keys()):
        encoded_pairs.append('%s=%s' % (
            key,
            urllib.parse.quote_plus(str(params[key]))
        ))
    sign_source = '&'.join(encoded_pairs)
    digest = hmac.new(
        app_secret.encode('utf-8'),
        sign_source.encode('utf-8'),
        hashlib.sha256,
    ).digest()
    return hashlib.md5(base64.b64encode(digest)).hexdigest()[5:15]


def format_spider_monitor_running_time(run_id, create_time_ms):
    if not isinstance(run_id, int):
        return '-'
    end_seconds = None
    if isinstance(create_time_ms, int):
        end_seconds = int(create_time_ms / 1000)
    if end_seconds is None or end_seconds < run_id:
        end_seconds = run_id
    return format_duration_seconds_text(end_seconds - run_id)


def format_spider_monitor_rate(total_nums, items_nums):
    if not isinstance(total_nums, int) or total_nums <= 0:
        return None
    if not isinstance(items_nums, int):
        return None
    return round(float(items_nums) * 100.0 / float(total_nums), 2)


def format_spider_monitor_delta(current_value, previous_value, unit=''):
    if current_value is None or previous_value is None:
        return '-'
    diff = current_value - previous_value
    if unit == '%':
        sign = '+' if diff > 0 else ''
        return '%s%.2f%s' % (sign, diff, unit)
    sign = '+' if diff > 0 else ''
    return '%s%s%s' % (sign, diff, unit)


def get_spider_monitor_mail_context(record):
    spider = record.get('spider_name') or ''
    spider_name_map = build_spider_name_map(load_tasks())
    task_name = resolve_display_name(
        spider,
        project=record.get('project'),
        fallback_name=record.get('spider_name'),
        spider_name_map=spider_name_map,
    )
    current_rate = format_spider_monitor_rate(record.get('total_nums'), record.get('items_nums'))
    previous_row = None
    recent_rows = []
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        previous_row = conn.execute(
            '''
            SELECT total_nums, items_nums, create_time_ms
            FROM spider_monitor_coverage
            WHERE spider_name = ?
              AND status = 'close'
              AND NOT (run_id = ? AND job_id = ? AND status = ?)
            ORDER BY COALESCE(create_time_ms, 0) DESC, record_id DESC
            LIMIT 1
            ''',
            (spider, record['run_id'], record['job_id'], record['status'])
        ).fetchone()
        recent_rows = conn.execute(
            '''
            SELECT total_nums, items_nums
            FROM spider_monitor_coverage
            WHERE spider_name = ?
              AND status = 'close'
            ORDER BY COALESCE(create_time_ms, 0) DESC, record_id DESC
            LIMIT 7
            ''',
            (spider,)
        ).fetchall()
    finally:
        conn.close()
    previous_rate = None
    previous_items = None
    if previous_row:
        previous_rate = format_spider_monitor_rate(previous_row['total_nums'], previous_row['items_nums'])
        previous_items = previous_row['items_nums'] if isinstance(previous_row['items_nums'], int) else None
    recent_rates = []
    for row in recent_rows:
        row_rate = format_spider_monitor_rate(row['total_nums'], row['items_nums'])
        if row_rate is not None:
            recent_rates.append(row_rate)
    recent_average_rate = (
        round(sum(recent_rates) / float(len(recent_rates)), 2)
        if recent_rates else None
    )
    taskstats_link = '%s/taskstats/?spider=%s' % (
        DAILY_STATS_PUBLIC_BASE_URL,
        urllib.parse.quote(spider),
    )
    spiderdocs_link = (
        '%s/spiderdocs/%s' % (DAILY_STATS_PUBLIC_BASE_URL, urllib.parse.quote(spider))
        if find_spider_doc_file(spider) else '-'
    )
    return dict(
        task_name=task_name,
        end_status=record.get('status') or '-',
        current_rate=current_rate,
        previous_rate=previous_rate,
        previous_items=previous_items,
        recent_average_rate=recent_average_rate,
        taskstats_link=taskstats_link,
        spiderdocs_link=spiderdocs_link,
    )


def build_spider_monitor_mail_body(record):
    total_nums = record.get('total_nums') or 0
    items_nums = record.get('items_nums') or 0
    url_nums = record.get('url_nums') or 0
    catch_rate = format_spider_monitor_rate(total_nums, items_nums)
    start_time = datetime.fromtimestamp(record['run_id']).strftime('%Y-%m-%d %H:%M:%S')
    end_time = record.get('event_time') or '-'
    running_time = format_spider_monitor_running_time(record.get('run_id'), record.get('create_time_ms'))
    context = get_spider_monitor_mail_context(record)
    return (
        '任务名      : {task_name}</br>'
        'SpiderName      : {spider_name}</br>'
        '任务ID      : {job_id}</br>'
        '结束状态    : {end_status}</br>'
        '开始时间    : {start_time}</br>'
        '结束时间    : {end_time}</br>'
        '运行耗时    : {running_time}</br>'
        '{separator}</br>'
        'URL数量     : {url_nums}</br>'
        '抓取数量   : {items_nums}</br>'
        '预期抓取    : {total_nums}</br>'
        '抓全率     : {catch_rate}</br>'
        '较上次抓全率变化 : {coverage_delta}</br>'
        '较上次抓取数量变化 : {items_delta}</br>'
        '最近7次平均抓全率 : {recent_average_rate}</br>'
        '{separator}</br>'
        'taskstats  : <a href="{taskstats_link}">{taskstats_link}</a></br>'
        'spiderdocs : {spiderdocs_link_html}</br>'
        '{separator}</br>'
        '本邮件由抓取监控系统自动发送</br>'
    ).format(
        task_name=context['task_name'],
        spider_name=record.get('spider_name') or '-',
        job_id=record.get('job_id') or '-',
        end_status=context['end_status'],
        start_time=start_time,
        end_time=end_time,
        running_time=running_time,
        separator='-' * 32,
        url_nums=url_nums,
        items_nums=items_nums,
        total_nums=total_nums,
        catch_rate=('-' if catch_rate is None else '%s%%' % catch_rate),
        coverage_delta=format_spider_monitor_delta(catch_rate, context['previous_rate'], '%'),
        items_delta=format_spider_monitor_delta(
            items_nums if isinstance(items_nums, int) else None,
            context['previous_items']
        ),
        recent_average_rate=(
            '-' if context['recent_average_rate'] is None else '%s%%' % context['recent_average_rate']
        ),
        taskstats_link=context['taskstats_link'],
        spiderdocs_link_html=(
            '<a href="{0}">{0}</a>'.format(context['spiderdocs_link'])
            if context['spiderdocs_link'] != '-' else '-'
        ),
    )


def send_spider_monitor_close_mail(record):
    if not SPIDER_MONITOR_MAIL_ENABLED:
        return False, 'mail disabled'
    if record.get('status') != 'close':
        return False, 'status is not close'
    if not SPIDER_MONITOR_MAIL_URL or not SPIDER_MONITOR_MAIL_APPKEY or not SPIDER_MONITOR_MAIL_APP_SECRET:
        return False, 'mail config missing'
    params = {
        'appkey': SPIDER_MONITOR_MAIL_APPKEY,
        'expires': int(datetime.now().timestamp()) + 100,
        'nonce': 'daily-stats-service',
        'to': SPIDER_MONITOR_MAIL_TO,
        'body': build_spider_monitor_mail_body(record),
        'subject': '[爬虫监控] %s %s' % (
            record.get('spider_name') or '-',
            record.get('job_id') or '-',
        ),
    }
    params['signature'] = build_spider_monitor_mail_signature(
        SPIDER_MONITOR_MAIL_APP_SECRET,
        params,
    )
    import requests
    response = requests.post(SPIDER_MONITOR_MAIL_URL, data=params, timeout=5)
    if response.status_code >= 400:
        raise RuntimeError('mail api status=%s body=%s' % (response.status_code, response.text[:500]))
    return True, response.text[:500]


def mark_spider_monitor_mail_result(record, sent_at=None, error_message=None):
    ensure_status_db()
    conn = connect_writable(DAILY_STATS_DB)
    try:
        conn.execute(
            '''
            UPDATE spider_monitor_coverage
            SET mail_sent_at = ?, mail_last_error = ?, updated_at = ?
            WHERE run_id = ? AND job_id = ? AND status = ?
            ''',
            (
                sent_at,
                error_message,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                record['run_id'],
                record['job_id'],
                record['status'],
            )
        )
        conn.commit()
    finally:
        conn.close()


def maybe_send_spider_monitor_close_mail(record):
    if record.get('status') != 'close':
        return
    ensure_status_db()
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        existing_row = conn.execute(
            '''
            SELECT mail_sent_at
            FROM spider_monitor_coverage
            WHERE run_id = ? AND job_id = ? AND status = ?
            LIMIT 1
            ''',
            (record['run_id'], record['job_id'], record['status'])
        ).fetchone()
    finally:
        conn.close()
    if existing_row and existing_row['mail_sent_at']:
        return
    try:
        _sent, message = send_spider_monitor_close_mail(record)
        mark_spider_monitor_mail_result(
            record,
            sent_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            error_message=None,
        )
    except Exception as err:
        mark_spider_monitor_mail_result(record, sent_at=None, error_message=str(err)[:500])


def save_spider_monitor_payload(payload):
    data = payload.get('data') or {}
    run_id = normalize_optional_int(payload.get('id'))
    if run_id is None:
        raise ValueError('missing id')

    job_type = str(payload.get('jobType') or '').strip() or 'spider'
    project = str(data.get('project') or '').strip()
    spider_name = str(data.get('spiderName') or '').strip()
    job_id = str(data.get('jobId') or '').strip()
    status = str(data.get('status') or '').strip()
    if not project:
        raise ValueError('missing data.project')
    if not spider_name:
        raise ValueError('missing data.spiderName')
    if not job_id:
        raise ValueError('missing data.jobId')
    if not status:
        raise ValueError('missing data.status')

    record = dict(
        run_id=run_id,
        job_type=job_type,
        project=project,
        spider_name=spider_name,
        job_id=job_id,
        job_main_id=str(data.get('jobMainId') or '').strip() or None,
        status=status,
        total_nums=normalize_optional_int(data.get('totalNums')),
        url_nums=normalize_optional_int(data.get('urlNums')),
        items_nums=normalize_optional_int(data.get('itemsNums')),
        create_time_ms=normalize_optional_int(data.get('createTime')),
        event_time=format_timestamp_ms(data.get('createTime')),
        raw_payload=json.dumps(payload, ensure_ascii=False, sort_keys=True),
    )

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ensure_status_db()
    conn = connect_writable(DAILY_STATS_DB)
    try:
        update_result = conn.execute(
            '''
            UPDATE spider_monitor_coverage
            SET job_type = ?, project = ?, spider_name = ?, job_main_id = ?, status = ?,
                total_nums = ?, url_nums = ?, items_nums = ?, create_time_ms = ?, event_time = ?,
                raw_payload = ?, updated_at = ?
            WHERE run_id = ? AND job_id = ? AND status = ?
            ''',
            (
                record['job_type'], record['project'], record['spider_name'], record['job_main_id'],
                record['status'], record['total_nums'], record['url_nums'], record['items_nums'],
                record['create_time_ms'], record['event_time'], record['raw_payload'], now,
                record['run_id'], record['job_id'], record['status'],
            )
        )
        if update_result.rowcount == 0:
            conn.execute(
                '''
                INSERT INTO spider_monitor_coverage (
                    run_id, job_type, project, spider_name, job_id, job_main_id, status,
                    total_nums, url_nums, items_nums, create_time_ms, event_time,
                    raw_payload, received_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    record['run_id'], record['job_type'], record['project'], record['spider_name'],
                    record['job_id'], record['job_main_id'], record['status'], record['total_nums'],
                    record['url_nums'], record['items_nums'], record['create_time_ms'],
                    record['event_time'], record['raw_payload'], now, now,
                )
            )
        conn.commit()
    finally:
        conn.close()
    return record


def format_coverage_rate(items_nums, total_nums):
    total = normalize_optional_int(total_nums)
    items = normalize_optional_int(items_nums)
    if total is None or total <= 0 or items is None:
        return '-'
    return '%.1f%%' % (float(items) * 100.0 / float(total))


def get_coverage_rate_class(items_nums, total_nums):
    total = normalize_optional_int(total_nums)
    items = normalize_optional_int(items_nums)
    if total is None or total <= 0 or items is None:
        return 'medium'
    rate = float(items) * 100.0 / float(total)
    if rate >= 95.0:
        return 'good'
    if rate >= 65.0:
        return 'medium'
    return 'low'


def load_coverage_report(limit=100):
    task_rows = load_tasks()
    spider_name_map = build_spider_name_map(task_rows)
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM spider_monitor_coverage
            WHERE status IN ('close', 'error')
            ORDER BY event_time DESC, record_id DESC
            LIMIT ?
            ''',
            (limit,)
        ).fetchall()
    finally:
        conn.close()

    result_rows = []
    total_items = 0
    total_expected = 0
    success_count = 0
    error_count = 0
    valid_expected_count = 0
    for row in rows:
        record = dict(row)
        total_nums = normalize_optional_int(record.get('total_nums'))
        items_nums = normalize_optional_int(record.get('items_nums'))
        url_nums = normalize_optional_int(record.get('url_nums'))
        if isinstance(items_nums, int):
            total_items += items_nums
        if isinstance(total_nums, int) and total_nums > 0:
            total_expected += total_nums
            valid_expected_count += 1

        status = record.get('status') or ''
        if status == 'close':
            success_count += 1
            status_text = '正常结束'
            status_class = 'safe'
        else:
            error_count += 1
            status_text = '异常结束'
            status_class = 'danger'

        result_rows.append(dict(
            name=resolve_display_name(
                record.get('spider_name'),
                project=record.get('project'),
                spider_name_map=spider_name_map,
            ),
            project=record.get('project') or '-',
            spider=record.get('spider_name') or '-',
            job_id=record.get('job_id') or '-',
            status_text=status_text,
            status_class=status_class,
            total_nums=total_nums if isinstance(total_nums, int) else 'N/A',
            url_nums=url_nums if isinstance(url_nums, int) else 'N/A',
            items_nums=items_nums if isinstance(items_nums, int) else 'N/A',
            coverage_rate=format_coverage_rate(items_nums, total_nums),
            rate_class=get_coverage_rate_class(items_nums, total_nums),
            event_time=record.get('event_time') or 'N/A',
        ))

    summary = dict(
        task_count=len(result_rows),
        success_count=success_count,
        error_count=error_count,
        total_items=total_items,
        overall_coverage_rate=format_coverage_rate(total_items, total_expected) if valid_expected_count else '-',
    )
    return result_rows, summary


def format_year_change(current_total, previous_total):
    if previous_total == 0:
        if current_total == 0:
            return '0%'
        return '上年为0'
    diff = current_total - previous_total
    percent = float(diff) * 100.0 / float(previous_total)
    sign = '+' if percent > 0 else ''
    return '%s%.1f%%' % (sign, percent)


def build_timer_job_ids(task_job_results_map):
    timer_job_ids = set()
    for rows in task_job_results_map.values():
        for record in rows:
            if record.get('result'):
                timer_job_ids.add(record['result'])
    return timer_job_ids


def collect_lazy_sync_candidates(task_results_map, task_job_results_map, status_rows, task_rows_map):
    candidates = []
    for task_result_id in task_job_results_map:
        task_result = None
        for results in task_results_map.values():
            for row in results:
                if row['id'] == task_result_id:
                    task_result = row
                    break
            if task_result:
                break
        if not task_result:
            continue
        task = task_rows_map.get(task_result['task_id'])
        if not task:
            continue
        for record in task_job_results_map.get(task_result_id, []):
            if record['status'] != 'ok':
                continue
            status_row = status_rows.get(record['id'])
            items = status_row.get('scraped_items') if status_row else None
            if isinstance(items, int):
                continue
            candidates.append(dict(
                task_job_result_id=record['id'],
                task_id=task_result['task_id'],
                task_result_id=task_result_id,
                job_id=record['result'],
                server=record['server'],
                project=task['project'],
                spider=task['spider'],
                run_time=record['run_time'],
            ))
    return candidates


def get_manual_failure_reason(record):
    if record.get('status') == '2':
        return ''
    if record.get('status') == '1':
        return '执行中'
    return '状态未知'


def load_manual_weekly_group_stats(day_start, day_end, timer_job_ids, spider_name_map=None):
    from .common import connect_readonly

    grouped = {}
    conn = connect_readonly(JOBS_DB)
    try:
        for _index, table_name in get_existing_job_tables():
            query = '''
                SELECT * FROM "{table}"
                WHERE deleted = '0' AND start IS NOT NULL AND start >= ? AND start < ?
                ORDER BY start DESC
            '''.format(table=table_name)
            rows = conn.execute(
                query,
                (day_start.strftime('%Y-%m-%d %H:%M:%S'), day_end.strftime('%Y-%m-%d %H:%M:%S'))
            ).fetchall()
            for row in rows:
                record = dict(row)
                if is_excluded_report_spider(record.get('spider')):
                    continue
                if (
                    record['spider'] not in WEEKLY_REPORT_INCLUDE_TIMER_CHILD_SPIDERS
                    and is_timer_related_job(record['job'], timer_job_ids)
                ):
                    continue
                key = (record['project'], record['spider'])
                display_name = resolve_display_name(
                    record['spider'],
                    project=record['project'],
                    spider_name_map=spider_name_map,
                )
                group = grouped.setdefault(key, dict(
                    project=record['project'],
                    spider=record['spider'],
                    name=display_name,
                    actual_execute=0,
                    success_count=0,
                    running_count=0,
                    scraped_total=0,
                    failure_reasons={},
                ))
                group['actual_execute'] += 1
                if record.get('status') == '2':
                    group['success_count'] += 1
                elif record.get('status') == '1':
                    group['running_count'] += 1
                if isinstance(record.get('items'), int):
                    group['scraped_total'] += record['items']
                reason = get_manual_failure_reason(record)
                if reason:
                    group['failure_reasons'][reason] = group['failure_reasons'].get(reason, 0) + 1
    finally:
        conn.close()
    return grouped


def load_manual_group_stats(period_start, period_end, timer_job_ids, spider_name_map=None):
    from .common import connect_readonly

    grouped = {}
    conn = connect_readonly(JOBS_DB)
    try:
        for _index, table_name in get_existing_job_tables():
            query = '''
                SELECT * FROM "{table}"
                WHERE deleted = '0' AND start IS NOT NULL AND start >= ? AND start < ?
                ORDER BY start DESC
            '''.format(table=table_name)
            rows = conn.execute(
                query,
                (period_start.strftime('%Y-%m-%d %H:%M:%S'), period_end.strftime('%Y-%m-%d %H:%M:%S'))
            ).fetchall()
            for row in rows:
                record = dict(row)
                if is_excluded_report_spider(record.get('spider')):
                    continue
                if (
                    record['spider'] not in WEEKLY_REPORT_INCLUDE_TIMER_CHILD_SPIDERS
                    and is_timer_related_job(record['job'], timer_job_ids)
                ):
                    continue
                key = (record['project'], record['spider'])
                display_name = resolve_display_name(
                    record['spider'],
                    project=record['project'],
                    spider_name_map=spider_name_map,
                )
                group = grouped.setdefault(key, dict(
                    project=record['project'],
                    spider=record['spider'],
                    name=display_name,
                    actual_execute=0,
                    success_count=0,
                    running_count=0,
                    scraped_total=0,
                    failure_reasons={},
                    latest_execute_time=None,
                ))
                group['actual_execute'] += 1
                if record.get('status') == '2':
                    group['success_count'] += 1
                elif record.get('status') == '1':
                    group['running_count'] += 1
                if isinstance(record.get('items'), int):
                    group['scraped_total'] += record['items']
                reason = get_manual_failure_reason(record)
                if reason:
                    group['failure_reasons'][reason] = group['failure_reasons'].get(reason, 0) + 1
                start_time = record.get('start')
                if start_time and (
                    not group['latest_execute_time'] or start_time > group['latest_execute_time']
                ):
                    group['latest_execute_time'] = start_time
    finally:
        conn.close()
    return grouped


def get_schedule_judgment(task_result, fire_time, selected_day, today, task_job_results_map, status_rows):
    execute_time = 'N/A'
    scraped_items = 'N/A'
    if task_result:
        execute_time = format_datetime(task_result['execute_time'])
        scraped_items = get_timer_scraped_items(task_result, task_job_results_map, status_rows)
        if task_result['fail_count'] == 0 and task_result['pass_count'] > 0:
            return '成功执行', 'safe', execute_time, scraped_items
        if task_result['fail_count'] == 0 and task_result['pass_count'] == 0:
            return '执行中', 'warning', execute_time, scraped_items
        return '失败执行', 'danger', execute_time, scraped_items

    if selected_day > today:
        return '未来计划', 'normal', execute_time, scraped_items
    if selected_day < today:
        return '未执行', 'danger', execute_time, scraped_items

    now = datetime.now(fire_time.tzinfo) if getattr(fire_time, 'tzinfo', None) else datetime.now()
    if fire_time > now:
        return '未到执行时间', 'normal', execute_time, scraped_items
    return '待执行', 'warning', execute_time, scraped_items


def build_timer_groups(selected_date):
    day_start = selected_date.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    selected_day = day_start.date()
    today = datetime.now().date()
    task_rows = load_tasks()
    spider_name_map = build_spider_name_map(task_rows)
    job_states = load_job_states()
    task_results_map = load_task_results_by_task(day_start, day_end)
    task_result_ids = [result['id'] for rows in task_results_map.values() for result in rows]
    task_job_results_map = load_task_job_results(task_result_ids)
    task_job_result_ids = [row['id'] for rows in task_job_results_map.values() for row in rows]
    status_rows = load_status_rows(task_job_result_ids)
    grouped_rows = {}
    timer_job_ids = set()
    lazy_sync_candidates = []
    task_rows_map = dict((task['id'], task) for task in task_rows)
    task_results_by_id = {}
    for results in task_results_map.values():
        for row in results:
            task_results_by_id[row['id']] = row

    if selected_day < today:
        for task_result_id in task_job_results_map:
            task_result = task_results_by_id.get(task_result_id)
            if not task_result:
                continue
            task = task_rows_map.get(task_result['task_id'])
            if not task:
                continue
            for record in task_job_results_map.get(task_result_id, []):
                if record['status'] != 'ok':
                    continue
                status_row = status_rows.get(record['id'])
                items = status_row.get('scraped_items') if status_row else None
                if isinstance(items, int):
                    continue
                lazy_sync_candidates.append(dict(
                    task_job_result_id=record['id'],
                    task_id=task_result['task_id'],
                    task_result_id=task_result_id,
                    job_id=record['result'],
                    server=record['server'],
                    project=task['project'],
                    spider=task['spider'],
                    run_time=record['run_time'],
                ))
        mark_lazy_sync_candidates(lazy_sync_candidates)

    for task in task_rows:
        if is_excluded_report_spider(task.get('spider')):
            continue
        job_state = job_states.get(str(task['id']))
        if not job_state:
            continue
        if job_state['next_run_time'] is None:
            continue
        trigger = job_state.get('trigger')
        if trigger is None:
            continue
        fire_times = get_fire_times_by_day(trigger, day_start, day_end)
        if not fire_times:
            continue

        task_results = task_results_map.get(task['id'], [])
        for index, fire_time in enumerate(fire_times):
            task_result = task_results[index] if index < len(task_results) else None
            if task_result:
                for record in task_job_results_map.get(task_result['id'], []):
                    if record.get('result'):
                        timer_job_ids.add(record['result'])
            judgment, judgment_class, execute_time, scraped_items = get_schedule_judgment(
                task_result, fire_time, selected_day, today, task_job_results_map, status_rows
            )
            name = resolve_display_name(
                task['spider'],
                project=task.get('project'),
                fallback_name=task.get('name') or 'task #%s' % task['id'],
                spider_name_map=spider_name_map,
            )
            row = dict(
                name=name,
                spider=task['spider'],
                fire_time=fire_time.strftime('%H:%M:%S'),
                execute_time=execute_time,
                judgment=judgment,
                judgment_class=judgment_class,
                scraped_items=scraped_items,
            )
            grouped_rows.setdefault((task['id'], name, task['spider']), []).append(row)

    groups = []
    for _key in grouped_rows:
        rows = grouped_rows[_key]
        if is_excluded_report_spider(_key[2]):
            continue
        groups.append(dict(
            name=_key[1],
            spider=_key[2],
            rows=rows,
            total=len(rows),
        ))
    return groups, timer_job_ids, day_start, day_end


def is_timer_related_job(job_id, timer_job_ids):
    if job_id in timer_job_ids:
        return True
    for timer_job_id in timer_job_ids:
        if job_id.startswith(timer_job_id):
            return True
    return False


def build_manual_jobs(day_start, day_end, timer_job_ids):
    from .common import connect_readonly

    manual_jobs = []
    conn = connect_readonly(JOBS_DB)
    try:
        for index, table_name in get_existing_job_tables():
            query = '''
                SELECT * FROM "{table}"
                WHERE deleted = '0' AND start IS NOT NULL AND start >= ? AND start < ?
                ORDER BY start DESC
            '''.format(table=table_name)
            rows = conn.execute(
                query,
                (day_start.strftime('%Y-%m-%d %H:%M:%S'), day_end.strftime('%Y-%m-%d %H:%M:%S'))
            ).fetchall()
            for row in rows:
                record = dict(row)
                if is_excluded_report_spider(record.get('spider')):
                    continue
                if is_timer_related_job(record['job'], timer_job_ids):
                    continue
                status, status_class = get_manual_job_status(record)
                manual_jobs.append(dict(
                    node=index,
                    project=record['project'],
                    spider=record['spider'],
                    job=record['job'],
                    start=format_datetime(record['start']),
                    finish=format_datetime(record['finish']),
                    status=status,
                    status_class=status_class,
                    scraped_items=record['items'] if isinstance(record['items'], int) else 'N/A',
                ))
    finally:
        conn.close()
    manual_jobs.sort(key=lambda item: item['start'], reverse=True)
    return manual_jobs


def get_daily_timer_status(selected_date, fire_times, aggregate_row):
    should_execute = len(fire_times)
    actual_execute = aggregate_row.get('actual_execute', 0) if aggregate_row else 0
    success_count = aggregate_row.get('success_count', 0) if aggregate_row else 0
    failed_count = aggregate_row.get('failed_count', 0) if aggregate_row else 0
    running_count = aggregate_row.get('running_count', 0) if aggregate_row else 0
    now = datetime.now()
    if failed_count > 0:
        return '失败执行', 'danger'
    if running_count > 0:
        return '执行中', 'warning'
    if actual_execute > 0 and success_count == actual_execute:
        return '成功执行', 'safe'
    if selected_date.date() > now.date():
        return '未来计划', 'normal'
    if selected_date.date() < now.date():
        return '未执行', 'danger'
    for fire_time in fire_times:
        comparable_now = datetime.now(fire_time.tzinfo) if getattr(fire_time, 'tzinfo', None) else now
        if fire_time <= comparable_now:
            return '待执行', 'warning'
    return '未到执行时间', 'normal'


def build_timer_period_sources(task_rows, job_states, aggregate_map, period_start, period_end):
    """Combine recorded execution facts with current scheduler state.

    Aggregate facts remain visible even after a task is paused, disabled, or
    removed from APScheduler. Scheduler state is only used to add enabled tasks
    that were expected to run but have no execution fact yet.
    """
    task_map = dict(
        (build_task_key('timer', task.get('id'), task.get('spider')), task)
        for task in task_rows
    )
    sources = []
    rendered = set()
    for task_key, aggregate_row in aggregate_map.items():
        if aggregate_row.get('source_type') != 'timer':
            continue
        task = task_map.get(task_key)
        task_id = aggregate_row.get('task_id') or (task.get('id') if task else None)
        state = job_states.get(str(task_id)) if task_id is not None else None
        trigger = state.get('trigger') if state else None
        fire_times = get_fire_times_between(trigger, period_start, period_end) if trigger else []
        sources.append(dict(
            task_key=task_key, task=task, aggregate_row=aggregate_row,
            fire_times=fire_times,
            should_execute=(len(fire_times) if trigger else aggregate_row.get('should_execute', 0)),
        ))
        rendered.add(task_key)
    for task in task_rows:
        task_key = build_task_key('timer', task.get('id'), task.get('spider'))
        if task_key in rendered:
            continue
        state = job_states.get(str(task.get('id')))
        if not state or state.get('next_run_time') is None:
            continue
        trigger = state.get('trigger')
        fire_times = get_fire_times_between(trigger, period_start, period_end) if trigger else []
        if fire_times:
            sources.append(dict(task_key=task_key, task=task, aggregate_row=None,
                                fire_times=fire_times, should_execute=len(fire_times)))
    return sources


def build_daily_report(selected_date):
    day_start = selected_date.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    run_date = selected_date.strftime('%Y-%m-%d')
    task_rows = load_tasks()
    spider_name_map = build_spider_name_map(task_rows)
    job_states = load_job_states()
    daily_agg_map = load_aggregate_map('task_daily_agg', 'run_date', run_date)
    carryover_rows = load_cross_day_running_rows(day_start, day_end, spider_name_map=spider_name_map)

    timer_rows = []
    timer_sources = build_timer_period_sources(task_rows, job_states, daily_agg_map, day_start, day_end)
    for source in timer_sources:
        task = source['task'] or {}
        aggregate_row = source['aggregate_row']
        spider = (aggregate_row or {}).get('spider') or task.get('spider')
        if is_excluded_report_spider(spider):
            continue
        fire_times = source['fire_times']
        should_execute = source['should_execute']
        status_text, status_class = get_daily_timer_status(selected_date, fire_times, aggregate_row)
        timer_rows.append(dict(
            name=resolve_display_name(
                spider,
                project=(aggregate_row or {}).get('project') or task.get('project'),
                fallback_name=(aggregate_row or {}).get('task_name') or task.get('name'),
                spider_name_map=spider_name_map,
            ),
            spider=spider,
            should_execute=should_execute,
            actual_execute=aggregate_row.get('actual_execute', 0) if aggregate_row else 0,
            success_count=aggregate_row.get('success_count', 0) if aggregate_row else 0,
            failed_count=aggregate_row.get('failed_count', 0) if aggregate_row else 0,
            running_count=aggregate_row.get('running_count', 0) if aggregate_row else 0,
            scraped_total=aggregate_row.get('scraped_items_total', 0) if aggregate_row else 0,
            latest_execute_time=format_datetime(aggregate_row.get('latest_execute_time')) if aggregate_row else 'N/A',
            failure_reason=(aggregate_row.get('top_failure_reason') if aggregate_row else '') or '-',
            status_text=status_text,
            status_class=status_class,
        ))

    manual_rows = []
    for task_key in daily_agg_map:
        row = daily_agg_map[task_key]
        if is_excluded_report_spider(row.get('spider')):
            continue
        if row['source_type'] != 'independent':
            continue
        success_count = row.get('success_count', 0)
        failed_count = row.get('failed_count', 0)
        running_count = row.get('running_count', 0)
        status_text = '成功执行'
        status_class = 'safe'
        if failed_count > 0:
            status_text = '失败执行'
            status_class = 'danger'
        elif running_count > 0:
            status_text = '执行中'
            status_class = 'warning'
        manual_rows.append(dict(
            name=resolve_display_name(
                row['spider'],
                project=row.get('project'),
                fallback_name=row.get('task_name'),
                spider_name_map=spider_name_map,
            ),
            spider=row['spider'],
            actual_execute=row.get('actual_execute', 0),
            success_count=success_count,
            failed_count=failed_count,
            running_count=running_count,
            scraped_total=row.get('scraped_items_total', 0),
            latest_execute_time=format_datetime(row.get('latest_execute_time')),
            failure_reason=row.get('top_failure_reason') or '-',
            status_text=status_text,
            status_class=status_class,
        ))

    timer_rows.sort(key=lambda item: (item['should_execute'] == 0, item['name']))
    manual_rows.sort(key=lambda item: item['latest_execute_time'], reverse=True)
    task_count = len(timer_rows) + len(manual_rows)
    total_should_execute = sum(row['should_execute'] for row in timer_rows)
    total_actual_execute = sum(row['actual_execute'] for row in timer_rows) + sum(
        row['actual_execute'] for row in manual_rows
    )
    total_success_count = sum(row['success_count'] for row in timer_rows) + sum(
        row['success_count'] for row in manual_rows
    )
    total_scraped_items = sum(row['scraped_total'] for row in timer_rows) + sum(
        row['scraped_total'] for row in manual_rows
    )
    summary = dict(
        task_count=task_count,
        total_should_execute=total_should_execute,
        total_actual_execute=total_actual_execute,
        total_scraped_items=total_scraped_items,
        overall_success_rate=format_percent(total_success_count, total_actual_execute),
    )
    return timer_rows, manual_rows, summary, carryover_rows


def build_weekly_report_legacy(selected_date):
    week_start = get_week_start(selected_date)
    week_end = week_start + timedelta(days=7)
    previous_week_start = week_start - timedelta(days=7)
    previous_week_end = week_start
    now = datetime.now()
    current_week_start = get_week_start(now)
    is_current_week = week_start.date() == current_week_start.date()
    should_execute_end = now if is_current_week and now < week_end else week_end
    task_rows = load_tasks()
    task_rows_map = dict((task['id'], task) for task in task_rows)
    spider_name_map = build_spider_name_map(task_rows)
    job_states = load_job_states()

    current_results_map = load_task_results_by_task(week_start, week_end)
    current_result_ids = [result['id'] for rows in current_results_map.values() for result in rows]
    current_job_results_map = load_task_job_results(current_result_ids)
    current_job_result_ids = [row['id'] for rows in current_job_results_map.values() for row in rows]
    current_status_rows = load_status_rows(current_job_result_ids)

    previous_results_map = load_task_results_by_task(previous_week_start, previous_week_end)
    previous_result_ids = [result['id'] for rows in previous_results_map.values() for result in rows]
    previous_job_results_map = load_task_job_results(previous_result_ids)
    previous_job_result_ids = [row['id'] for rows in previous_job_results_map.values() for row in rows]
    previous_status_rows = load_status_rows(previous_job_result_ids)
    current_timer_job_ids = build_timer_job_ids(current_job_results_map)
    previous_timer_job_ids = build_timer_job_ids(previous_job_results_map)

    if selected_date.date() < datetime.now().date():
        lazy_candidates = collect_lazy_sync_candidates(
            current_results_map, current_job_results_map, current_status_rows, task_rows_map
        )
        mark_lazy_sync_candidates(lazy_candidates)
        if lazy_candidates:
            current_status_rows = load_status_rows(current_job_result_ids)

    rows = []
    total_success_count = 0
    total_actual_execute = 0
    for task in task_rows:
        if is_excluded_report_spider(task.get('spider')):
            continue
        job_state = job_states.get(str(task['id']))
        if not job_state:
            continue
        if job_state['next_run_time'] is None:
            continue
        trigger = job_state.get('trigger') if job_state else None
        current_fire_times = get_fire_times_between(trigger, week_start, should_execute_end) if trigger else []
        current_task_results = current_results_map.get(task['id'], [])
        previous_task_results = previous_results_map.get(task['id'], [])
        should_execute = len(current_fire_times)
        actual_execute = len(current_task_results)

        if should_execute == 0 and actual_execute == 0:
            continue

        success_count = 0
        scraped_total = 0
        previous_scraped_total = 0
        failure_reasons = {}
        running_count = 0

        for task_result in current_task_results:
            if task_result['fail_count'] == 0 and task_result['pass_count'] > 0:
                success_count += 1
            elif task_result['fail_count'] == 0 and task_result['pass_count'] == 0:
                running_count += 1
            scraped_total += count_timer_scraped_items(task_result, current_job_results_map, current_status_rows)
            for record in current_job_results_map.get(task_result['id'], []):
                if record['status'] == 'ok':
                    continue
                reason = get_failure_reason(record)
                failure_reasons[reason] = failure_reasons.get(reason, 0) + 1

        for task_result in previous_task_results:
            previous_scraped_total += count_timer_scraped_items(
                task_result, previous_job_results_map, previous_status_rows
            )

        main_failure_reason = '-'
        if failure_reasons:
            main_failure_reason = sorted(
                failure_reasons.items(), key=lambda item: (-item[1], item[0])
            )[0][0]

        rows.append(dict(
            name=resolve_display_name(
                task['spider'],
                project=task.get('project'),
                fallback_name=task.get('name') or 'task #%s' % task['id'],
                spider_name_map=spider_name_map,
            ),
            spider=task['spider'],
            run_type='定时',
            should_execute=should_execute,
            actual_execute=actual_execute,
            success_rate=format_percent(success_count, actual_execute),
            scraped_total=scraped_total,
            average_daily_items='%.1f' % (float(scraped_total) / 7.0),
            week_change=format_week_change(scraped_total, previous_scraped_total),
            week_change_class=get_week_change_class(format_week_change(scraped_total, previous_scraped_total)),
            failure_reason=main_failure_reason,
            highlight_danger=(running_count == 0 and actual_execute > 0 and success_count < actual_execute),
            _source_type='timer',
            _success_count=success_count,
            _running_count=running_count,
            _current_scraped_total=scraped_total,
            _previous_scraped_total=previous_scraped_total,
        ))
        total_success_count += success_count
        total_actual_execute += actual_execute

    current_manual_groups = load_manual_weekly_group_stats(
        week_start, week_end, current_timer_job_ids, spider_name_map=spider_name_map
    )
    previous_manual_groups = load_manual_weekly_group_stats(
        previous_week_start, previous_week_end, previous_timer_job_ids, spider_name_map=spider_name_map
    )
    for key in current_manual_groups:
        group = current_manual_groups[key]
        if is_excluded_report_spider(group.get('spider')):
            continue
        previous_group = previous_manual_groups.get(key, {})
        main_failure_reason = '-'
        if group['failure_reasons']:
            main_failure_reason = sorted(
                group['failure_reasons'].items(), key=lambda item: (-item[1], item[0])
            )[0][0]
        rows.append(dict(
            name=group['name'],
            spider=group['spider'],
            run_type=get_independent_run_type(group['spider'], spider_name_map=spider_name_map),
            should_execute='-',
            actual_execute=group['actual_execute'],
            success_rate=format_percent(group['success_count'], group['actual_execute']),
            scraped_total=group['scraped_total'],
            average_daily_items='%.1f' % (float(group['scraped_total']) / 7.0),
            week_change=format_week_change(group['scraped_total'], previous_group.get('scraped_total', 0)),
            week_change_class=get_week_change_class(
                format_week_change(group['scraped_total'], previous_group.get('scraped_total', 0))
            ),
            failure_reason=main_failure_reason,
            highlight_danger=(
                group['running_count'] == 0
                and group['actual_execute'] > 0
                and group['success_count'] < group['actual_execute']
            ),
            _source_type='independent',
            _success_count=group['success_count'],
            _running_count=group['running_count'],
            _current_scraped_total=group['scraped_total'],
            _previous_scraped_total=previous_group.get('scraped_total', 0),
        ))
        total_success_count += group['success_count']
        total_actual_execute += group['actual_execute']

    rows = merge_weekly_rows(rows)
    rows.sort(key=lambda item: (-item['scraped_total'], item['run_type'] != '定时', item['spider']))
    summary = dict(
        task_count=len(rows),
        total_should_execute=sum(row['should_execute'] for row in rows if isinstance(row['should_execute'], int)),
        total_actual_execute=total_actual_execute,
        total_scraped_items=sum(row['scraped_total'] for row in rows),
    )
    summary['overall_success_rate'] = format_percent(total_success_count, total_actual_execute)
    return rows, summary, week_start, week_end, previous_week_start, is_current_week


def build_weekly_report(selected_date):
    week_start = get_week_start(selected_date)
    week_end = week_start + timedelta(days=7)
    previous_week_start = week_start - timedelta(days=7)
    now = datetime.now()
    current_week_start = get_week_start(now)
    is_current_week = week_start.date() == current_week_start.date()
    should_execute_end = now if is_current_week and now < week_end else week_end
    task_rows = load_tasks()
    spider_name_map = build_spider_name_map(task_rows)
    job_states = load_job_states()
    current_agg_map = load_aggregate_map('task_weekly_agg', 'run_week_start', week_start.strftime('%Y-%m-%d'))
    previous_agg_map = load_aggregate_map('task_weekly_agg', 'run_week_start', previous_week_start.strftime('%Y-%m-%d'))
    if not current_agg_map and not previous_agg_map:
        return build_weekly_report_legacy(selected_date)

    rows = []
    total_success_count = 0
    total_actual_execute = 0
    timer_sources = build_timer_period_sources(task_rows, job_states, current_agg_map, week_start, should_execute_end)
    for source in timer_sources:
        task = source['task'] or {}
        current_row = source['aggregate_row'] or {}
        task_key = source['task_key']
        spider = current_row.get('spider') or task.get('spider')
        if is_excluded_report_spider(spider):
            continue
        previous_row = previous_agg_map.get(task_key, {})
        should_execute = source['should_execute']
        actual_execute = current_row.get('actual_execute', 0)

        if should_execute == 0 and actual_execute == 0:
            continue

        success_count = current_row.get('success_count', 0)
        scraped_total = current_row.get('scraped_items_total', 0)
        previous_scraped_total = previous_row.get('scraped_items_total', 0)
        running_count = current_row.get('running_count', 0)
        main_failure_reason = current_row.get('top_failure_reason') or '-'

        rows.append(dict(
            task_key=task_key,
            name=resolve_display_name(
                spider,
                project=current_row.get('project') or task.get('project'),
                fallback_name=current_row.get('task_name') or task.get('name') or 'task #%s' % (task.get('id') or '?'),
                spider_name_map=spider_name_map,
            ),
            spider=spider,
            run_type='定时',
            should_execute=should_execute,
            actual_execute=actual_execute,
            success_rate=format_percent(success_count, actual_execute),
            scraped_total=scraped_total,
            average_daily_items='%.1f' % (float(scraped_total) / 7.0),
            week_change=format_week_change(scraped_total, previous_scraped_total),
            week_change_class=get_week_change_class(format_week_change(scraped_total, previous_scraped_total)),
            failure_reason=main_failure_reason,
            highlight_danger=(running_count == 0 and actual_execute > 0 and success_count < actual_execute),
            _source_type='timer',
            _success_count=success_count,
            _running_count=running_count,
            _current_scraped_total=scraped_total,
            _previous_scraped_total=previous_scraped_total,
        ))
        total_success_count += success_count
        total_actual_execute += actual_execute

    for task_key in current_agg_map:
        group = current_agg_map[task_key]
        if is_excluded_report_spider(group.get('spider')):
            continue
        if group['source_type'] != 'independent':
            continue
        previous_group = previous_agg_map.get(task_key, {})
        rows.append(dict(
            task_key=task_key,
            name=resolve_display_name(
                group['spider'],
                project=group.get('project'),
                fallback_name=group.get('task_name'),
                spider_name_map=spider_name_map,
            ),
            spider=group['spider'],
            run_type=get_independent_run_type(group['spider'], spider_name_map=spider_name_map),
            should_execute='-',
            actual_execute=group['actual_execute'],
            success_rate=format_percent(group['success_count'], group['actual_execute']),
            scraped_total=group['scraped_items_total'],
            average_daily_items='%.1f' % (float(group['scraped_items_total']) / 7.0),
            week_change=format_week_change(group['scraped_items_total'], previous_group.get('scraped_items_total', 0)),
            week_change_class=get_week_change_class(
                format_week_change(group['scraped_items_total'], previous_group.get('scraped_items_total', 0))
            ),
            failure_reason=group.get('top_failure_reason') or '-',
            highlight_danger=(
                group['running_count'] == 0
                and group['actual_execute'] > 0
                and group['success_count'] < group['actual_execute']
            ),
            _source_type='independent',
            _success_count=group['success_count'],
            _running_count=group['running_count'],
            _current_scraped_total=group['scraped_items_total'],
            _previous_scraped_total=previous_group.get('scraped_items_total', 0),
        ))
        total_success_count += group['success_count']
        total_actual_execute += group['actual_execute']

    rows = merge_weekly_rows(rows)
    rows.sort(key=lambda item: (-item['scraped_total'], item['run_type'] != '定时', item['spider']))
    summary = dict(
        task_count=len(rows),
        total_should_execute=sum(row['should_execute'] for row in rows if isinstance(row['should_execute'], int)),
        total_actual_execute=total_actual_execute,
        total_scraped_items=sum(row['scraped_total'] for row in rows),
    )
    summary['overall_success_rate'] = format_percent(total_success_count, total_actual_execute)
    return rows, summary, week_start, week_end, previous_week_start, is_current_week


def get_year_start(selected_date):
    return selected_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


def get_year_end(year_start):
    return year_start.replace(year=year_start.year + 1)


def format_average_value(total, divisor):
    if divisor <= 0:
        return '0.0'
    return '%.1f' % (float(total) / float(divisor))


def get_elapsed_months(period_start, period_end, is_current_year):
    if is_current_year:
        return max(1, period_end.month)
    return 12


def build_annual_report_legacy(selected_date):
    year_start = get_year_start(selected_date)
    year_end = get_year_end(year_start)
    previous_year_start = year_start.replace(year=year_start.year - 1)
    previous_year_end = year_start
    now = datetime.now()
    current_year_start = get_year_start(now)
    is_current_year = year_start.date() == current_year_start.date()
    actual_period_end = now if is_current_year and now < year_end else year_end
    elapsed_days = max(1, (actual_period_end - year_start).days + 1)
    elapsed_months = get_elapsed_months(year_start, actual_period_end, is_current_year)

    task_rows = load_tasks()
    spider_name_map = build_spider_name_map(task_rows)
    job_states = load_job_states()

    current_results_map = load_task_results_by_task(year_start, year_end)
    current_result_ids = [result['id'] for rows in current_results_map.values() for result in rows]
    current_job_results_map = load_task_job_results(current_result_ids)
    current_job_result_ids = [row['id'] for rows in current_job_results_map.values() for row in rows]
    current_status_rows = load_status_rows(current_job_result_ids)

    previous_results_map = load_task_results_by_task(previous_year_start, previous_year_end)
    previous_result_ids = [result['id'] for rows in previous_results_map.values() for result in rows]
    previous_job_results_map = load_task_job_results(previous_result_ids)
    previous_job_result_ids = [row['id'] for rows in previous_job_results_map.values() for row in rows]
    previous_status_rows = load_status_rows(previous_job_result_ids)

    current_timer_job_ids = build_timer_job_ids(current_job_results_map)
    previous_timer_job_ids = build_timer_job_ids(previous_job_results_map)
    if year_start.date() < current_year_start.date():
        task_rows_map = dict((task['id'], task) for task in task_rows)
        lazy_candidates = collect_lazy_sync_candidates(
            current_results_map, current_job_results_map, current_status_rows, task_rows_map
        )
        mark_lazy_sync_candidates(lazy_candidates)
        if lazy_candidates:
            current_status_rows = load_status_rows(current_job_result_ids)

    rows = []
    total_success_count = 0
    total_actual_execute = 0

    for task in task_rows:
        if is_excluded_report_spider(task.get('spider')):
            continue
        job_state = job_states.get(str(task['id']))
        if not job_state or job_state['next_run_time'] is None:
            continue
        trigger = job_state.get('trigger')
        current_fire_times = get_fire_times_between(trigger, year_start, actual_period_end) if trigger else []
        current_task_results = current_results_map.get(task['id'], [])
        previous_task_results = previous_results_map.get(task['id'], [])
        should_execute = len(current_fire_times)
        actual_execute = len(current_task_results)

        if should_execute == 0 and actual_execute == 0:
            continue

        success_count = 0
        scraped_total = 0
        previous_scraped_total = 0
        failure_reasons = {}
        running_count = 0
        latest_execute_time = None

        for task_result in current_task_results:
            if task_result['fail_count'] == 0 and task_result['pass_count'] > 0:
                success_count += 1
            elif task_result['fail_count'] == 0 and task_result['pass_count'] == 0:
                running_count += 1
            scraped_total += count_timer_scraped_items(task_result, current_job_results_map, current_status_rows)
            if task_result.get('execute_time') and (
                not latest_execute_time or task_result['execute_time'] > latest_execute_time
            ):
                latest_execute_time = task_result['execute_time']
            for record in current_job_results_map.get(task_result['id'], []):
                if record['status'] == 'ok':
                    continue
                reason = get_failure_reason(record)
                failure_reasons[reason] = failure_reasons.get(reason, 0) + 1

        for task_result in previous_task_results:
            previous_scraped_total += count_timer_scraped_items(
                task_result, previous_job_results_map, previous_status_rows
            )

        main_failure_reason = '-'
        if failure_reasons:
            main_failure_reason = sorted(
                failure_reasons.items(), key=lambda item: (-item[1], item[0])
            )[0][0]

        year_change = format_year_change(scraped_total, previous_scraped_total)
        rows.append(dict(
            name=resolve_display_name(
                task['spider'],
                project=task.get('project'),
                fallback_name=task.get('name') or 'task #%s' % task['id'],
                spider_name_map=spider_name_map,
            ),
            spider=task['spider'],
            run_type='定时',
            should_execute=should_execute,
            actual_execute=actual_execute,
            success_rate=format_percent(success_count, actual_execute),
            scraped_total=scraped_total,
            average_daily_items=format_average_value(scraped_total, elapsed_days),
            average_monthly_items=format_average_value(scraped_total, elapsed_months),
            year_change=year_change,
            year_change_class=get_week_change_class(year_change),
            failure_reason=main_failure_reason,
            latest_execute_time=format_datetime(latest_execute_time),
            highlight_danger=(running_count == 0 and actual_execute > 0 and success_count < actual_execute),
            _source_type='timer',
            _success_count=success_count,
            _running_count=running_count,
            _current_scraped_total=scraped_total,
            _previous_scraped_total=previous_scraped_total,
        ))
        total_success_count += success_count
        total_actual_execute += actual_execute

    current_manual_groups = load_manual_group_stats(
        year_start, year_end, current_timer_job_ids, spider_name_map=spider_name_map
    )
    previous_manual_groups = load_manual_group_stats(
        previous_year_start, previous_year_end, previous_timer_job_ids, spider_name_map=spider_name_map
    )
    for key in current_manual_groups:
        group = current_manual_groups[key]
        if is_excluded_report_spider(group.get('spider')):
            continue
        previous_group = previous_manual_groups.get(key, {})
        main_failure_reason = '-'
        if group['failure_reasons']:
            main_failure_reason = sorted(
                group['failure_reasons'].items(), key=lambda item: (-item[1], item[0])
            )[0][0]
        year_change = format_year_change(group['scraped_total'], previous_group.get('scraped_total', 0))
        rows.append(dict(
            name=group['name'],
            spider=group['spider'],
            run_type=get_independent_run_type(group['spider'], spider_name_map=spider_name_map),
            should_execute='-',
            actual_execute=group['actual_execute'],
            success_rate=format_percent(group['success_count'], group['actual_execute']),
            scraped_total=group['scraped_total'],
            average_daily_items=format_average_value(group['scraped_total'], elapsed_days),
            average_monthly_items=format_average_value(group['scraped_total'], elapsed_months),
            year_change=year_change,
            year_change_class=get_week_change_class(year_change),
            failure_reason=main_failure_reason,
            latest_execute_time=format_datetime(group.get('latest_execute_time')),
            highlight_danger=(
                group['running_count'] == 0
                and group['actual_execute'] > 0
                and group['success_count'] < group['actual_execute']
            ),
            _source_type='independent',
            _success_count=group['success_count'],
            _running_count=group['running_count'],
            _current_scraped_total=group['scraped_total'],
            _previous_scraped_total=previous_group.get('scraped_total', 0),
        ))
        total_success_count += group['success_count']
        total_actual_execute += group['actual_execute']

    rows = merge_annual_rows(rows, elapsed_days, elapsed_months)
    rows.sort(key=lambda item: (-item['scraped_total'], item['run_type'] != '定时', item['spider']))
    summary = dict(
        task_count=len(rows),
        total_should_execute=sum(row['should_execute'] for row in rows if isinstance(row['should_execute'], int)),
        total_actual_execute=total_actual_execute,
        total_scraped_items=sum(row['scraped_total'] for row in rows),
    )
    summary['overall_success_rate'] = format_percent(total_success_count, total_actual_execute)
    return rows, summary, year_start, year_end, is_current_year


def build_annual_report(selected_date):
    year_start = get_year_start(selected_date)
    year_end = get_year_end(year_start)
    previous_year_start = year_start.replace(year=year_start.year - 1)
    previous_year_end = year_start
    now = datetime.now()
    current_year_start = get_year_start(now)
    is_current_year = year_start.date() == current_year_start.date()
    actual_period_end = now if is_current_year and now < year_end else year_end
    elapsed_days = max(1, (actual_period_end - year_start).days + 1)
    elapsed_months = get_elapsed_months(year_start, actual_period_end, is_current_year)

    task_rows = load_tasks()
    spider_name_map = build_spider_name_map(task_rows)
    job_states = load_job_states()
    current_agg_map = load_aggregate_map('task_yearly_agg', 'run_year', year_start.year)
    previous_agg_map = load_aggregate_map('task_yearly_agg', 'run_year', previous_year_start.year)
    if not current_agg_map and not previous_agg_map:
        return build_annual_report_legacy(selected_date)

    rows = []
    total_success_count = 0
    total_actual_execute = 0

    timer_sources = build_timer_period_sources(task_rows, job_states, current_agg_map, year_start, actual_period_end)
    for source in timer_sources:
        task = source['task'] or {}
        current_row = source['aggregate_row'] or {}
        task_key = source['task_key']
        spider = current_row.get('spider') or task.get('spider')
        if is_excluded_report_spider(spider):
            continue
        previous_row = previous_agg_map.get(task_key, {})
        should_execute = source['should_execute']
        actual_execute = current_row.get('actual_execute', 0)

        if should_execute == 0 and actual_execute == 0:
            continue

        success_count = current_row.get('success_count', 0)
        scraped_total = current_row.get('scraped_items_total', 0)
        previous_scraped_total = previous_row.get('scraped_items_total', 0)
        running_count = current_row.get('running_count', 0)
        latest_execute_time = current_row.get('latest_execute_time')
        main_failure_reason = current_row.get('top_failure_reason') or '-'

        year_change = format_year_change(scraped_total, previous_scraped_total)
        rows.append(dict(
            task_key=task_key,
            name=resolve_display_name(
                spider,
                project=current_row.get('project') or task.get('project'),
                fallback_name=current_row.get('task_name') or task.get('name') or 'task #%s' % (task.get('id') or '?'),
                spider_name_map=spider_name_map,
            ),
            spider=spider,
            run_type='定时',
            should_execute=should_execute,
            actual_execute=actual_execute,
            success_rate=format_percent(success_count, actual_execute),
            scraped_total=scraped_total,
            average_daily_items=format_average_value(scraped_total, elapsed_days),
            average_monthly_items=format_average_value(scraped_total, elapsed_months),
            year_change=year_change,
            year_change_class=get_week_change_class(year_change),
            failure_reason=main_failure_reason,
            latest_execute_time=format_datetime(latest_execute_time),
            highlight_danger=(running_count == 0 and actual_execute > 0 and success_count < actual_execute),
            _source_type='timer',
            _success_count=success_count,
            _running_count=running_count,
            _current_scraped_total=scraped_total,
            _previous_scraped_total=previous_scraped_total,
        ))
        total_success_count += success_count
        total_actual_execute += actual_execute

    for task_key in current_agg_map:
        group = current_agg_map[task_key]
        if is_excluded_report_spider(group.get('spider')):
            continue
        if group['source_type'] != 'independent':
            continue
        previous_group = previous_agg_map.get(task_key, {})
        year_change = format_year_change(group['scraped_items_total'], previous_group.get('scraped_items_total', 0))
        rows.append(dict(
            task_key=task_key,
            name=resolve_display_name(
                group['spider'],
                project=group.get('project'),
                fallback_name=group.get('task_name'),
                spider_name_map=spider_name_map,
            ),
            spider=group['spider'],
            run_type=get_independent_run_type(group['spider'], spider_name_map=spider_name_map),
            should_execute='-',
            actual_execute=group['actual_execute'],
            success_rate=format_percent(group['success_count'], group['actual_execute']),
            scraped_total=group['scraped_items_total'],
            average_daily_items=format_average_value(group['scraped_items_total'], elapsed_days),
            average_monthly_items=format_average_value(group['scraped_items_total'], elapsed_months),
            year_change=year_change,
            year_change_class=get_week_change_class(year_change),
            failure_reason=group.get('top_failure_reason') or '-',
            latest_execute_time=format_datetime(group.get('latest_execute_time')),
            highlight_danger=(
                group['running_count'] == 0
                and group['actual_execute'] > 0
                and group['success_count'] < group['actual_execute']
            ),
            _source_type='independent',
            _success_count=group['success_count'],
            _running_count=group['running_count'],
            _current_scraped_total=group['scraped_items_total'],
            _previous_scraped_total=previous_group.get('scraped_items_total', 0),
        ))
        total_success_count += group['success_count']
        total_actual_execute += group['actual_execute']

    rows = merge_annual_rows(rows, elapsed_days, elapsed_months)
    rows.sort(key=lambda item: (-item['scraped_total'], item['run_type'] != '定时', item['spider']))
    summary = dict(
        task_count=len(rows),
        total_should_execute=sum(row['should_execute'] for row in rows if isinstance(row['should_execute'], int)),
        total_actual_execute=total_actual_execute,
        total_scraped_items=sum(row['scraped_total'] for row in rows),
    )
    summary['overall_success_rate'] = format_percent(total_success_count, total_actual_execute)
    return rows, summary, year_start, year_end, is_current_year


@bp.route('/')
def index():
    return render_template(
        'index.html',
        cards=load_dashboard_summary(),
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    )


@bp.route('/spiderdocs/<spider>')
def spider_doc(spider):
    doc_path = find_spider_doc_file(spider)
    if not doc_path:
        abort(404)
    doc_ext = os.path.splitext(doc_path)[1].lower()
    if doc_ext == '.pdf':
        task_configs = load_spider_task_configs(spider)
        return render_template(
            'spider_doc.html',
            spider=spider,
            doc_name=os.path.basename(doc_path),
            doc_path=doc_path,
            content='',
            rendered_html=None,
            pdf_url=url_for('daily_stats.spider_doc_asset', spider=spider),
            task_configs=task_configs,
            now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )
    try:
        with open(doc_path, 'r', encoding='utf-8') as handle:
            content = handle.read()
    except (OSError, UnicodeDecodeError):
        with open(doc_path, 'r', encoding='utf-8', errors='replace') as handle:
            content = handle.read()
    rendered_html = render_markdown_content(content)
    task_configs = load_spider_task_configs(spider)
    return render_template(
        'spider_doc.html',
        spider=spider,
        doc_name=os.path.basename(doc_path),
        doc_path=doc_path,
        content=content,
        rendered_html=rendered_html,
        task_configs=task_configs,
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    )


@bp.route('/spiderdocs/<spider>/asset')
def spider_doc_asset(spider):
    doc_path = find_spider_doc_file(spider)
    if not doc_path:
        abort(404)
    return send_file(doc_path)


@bp.route('/dailystats/')
def daily_stats():
    selected_date = parse_selected_date()
    timer_rows, manual_rows, summary, carryover_rows = build_daily_report(selected_date)
    use_aggregate = bool(timer_rows or manual_rows)
    if not use_aggregate:
        groups, timer_job_ids, day_start, day_end = build_timer_groups(selected_date)
        manual_jobs = build_manual_jobs(day_start, day_end, timer_job_ids)
        summary = dict(
            task_count=len(groups) + len(manual_jobs),
            total_should_execute=sum(group['total'] for group in groups),
            total_actual_execute=sum(group['total'] for group in groups) + len(manual_jobs),
            total_scraped_items=sum(
                row.get('scraped_items', 0) if isinstance(row.get('scraped_items'), int) else 0
                for group in groups for row in group['rows']
            ) + sum(
                job['scraped_items'] if isinstance(job.get('scraped_items'), int) else 0
                for job in manual_jobs
            ),
            overall_success_rate='-',
        )
    else:
        groups, manual_jobs = [], []
    return render_template(
        'daily_stats.html',
        summary=summary,
        use_aggregate=use_aggregate,
        timer_rows=timer_rows,
        manual_rows=manual_rows,
        carryover_rows=carryover_rows,
        groups=groups,
        manual_jobs=manual_jobs,
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        selected_date=selected_date.strftime('%Y-%m-%d'),
        previous_date=(selected_date - timedelta(days=1)).strftime('%Y-%m-%d'),
        next_date=(selected_date + timedelta(days=1)).strftime('%Y-%m-%d'),
        is_today=selected_date.date() == datetime.now().date(),
        database_dir=DATABASE_DIR,
        settings_path=SETTINGS_PATH,
        status_db_path=DAILY_STATS_DB,
    )


@bp.route('/coveragestats/')
def coverage_stats():
    rows, summary = load_coverage_report(limit=100)
    return render_template(
        'coverage_stats.html',
        rows=rows,
        summary=summary,
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        database_dir=DATABASE_DIR,
        settings_path=SETTINGS_PATH,
        status_db_path=DAILY_STATS_DB,
    )


@bp.route('/weeklystats/')
def weekly_stats():
    selected_date = parse_selected_week_date()
    rows, summary, week_start, week_end, previous_week_start, is_current_week = build_weekly_report(selected_date)
    next_week_start = week_start + timedelta(days=7)
    week_options = build_week_options(selected_date)
    week_blocks = [
        dict(
            label='前一周',
            start=previous_week_start.strftime('%Y-%m-%d'),
            end=(week_start - timedelta(days=1)).strftime('%Y-%m-%d'),
            css_class='previous',
        ),
        dict(
            label='所选周',
            start=week_start.strftime('%Y-%m-%d'),
            end=(week_end - timedelta(days=1)).strftime('%Y-%m-%d'),
            css_class='current',
        ),
        dict(
            label='后一周',
            start=next_week_start.strftime('%Y-%m-%d'),
            end=(next_week_start + timedelta(days=6)).strftime('%Y-%m-%d'),
            css_class='next',
        ),
    ]
    selected_week_days = []
    for offset in range(7):
        day = week_start + timedelta(days=offset)
        selected_week_days.append(dict(
            label=day.strftime('%m-%d'),
            weekday=['周一', '周二', '周三', '周四', '周五', '周六', '周日'][day.weekday()],
            is_selected=(day.date() == selected_date.date()),
        ))
    return render_template(
        'weekly_stats.html',
        rows=rows,
        summary=summary,
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        is_current_week=is_current_week,
        selected_date=selected_date.strftime('%Y-%m-%d'),
        week_label='%s ~ %s' % (
            week_start.strftime('%Y-%m-%d'),
            (week_end - timedelta(days=1)).strftime('%Y-%m-%d')
        ),
        previous_week_date=previous_week_start.strftime('%Y-%m-%d'),
        next_week_date=(week_start + timedelta(days=7)).strftime('%Y-%m-%d'),
        week_options=week_options,
        week_blocks=week_blocks,
        selected_week_days=selected_week_days,
        database_dir=DATABASE_DIR,
        settings_path=SETTINGS_PATH,
        status_db_path=DAILY_STATS_DB,
    )


@bp.route('/taskstats/')
def task_stats():
    spider = request.args.get('spider', '').strip()
    start_date = parse_taskstats_date(request.args.get('start_date'))
    end_date = parse_taskstats_date(request.args.get('end_date'))
    payload = load_task_recent_execution_stats(
        spider,
        limit=100,
        start_date=start_date,
        end_date=end_date,
    )
    if not payload:
        abort(404)
    return render_template(
        'task_stats.html',
        summary=payload['summary'],
        points=payload['points'],
        coverage_points=payload['coverage_points'],
        executions=payload['executions'],
        start_date=start_date.strftime('%Y-%m-%d') if start_date else '',
        end_date=end_date.strftime('%Y-%m-%d') if end_date else '',
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    )


@bp.route('/api/weeklystats/scraped-totals')
def weekly_stats_scraped_totals():
    selected_date = parse_selected_week_date()
    rows, _summary, _week_start, _week_end, _previous_week_start, _is_current_week = build_weekly_report(selected_date)
    return jsonify(build_rows_scraped_totals_payload(rows))


@bp.route('/annualstats/')
def annual_stats():
    selected_date = parse_selected_year_date()
    rows, summary, year_start, year_end, is_current_year = build_annual_report(selected_date)
    previous_year_start = year_start.replace(year=year_start.year - 1)
    next_year_start = year_start.replace(year=year_start.year + 1)
    year_blocks = [
        dict(
            label='前一年',
            start=previous_year_start.strftime('%Y-%m-%d'),
            title=str(previous_year_start.year),
            css_class='previous',
        ),
        dict(
            label='所选年',
            start=year_start.strftime('%Y-%m-%d'),
            title=str(year_start.year),
            css_class='current',
        ),
        dict(
            label='后一年',
            start=next_year_start.strftime('%Y-%m-%d'),
            title=str(next_year_start.year),
            css_class='next',
        ),
    ]
    return render_template(
        'annual_stats.html',
        rows=rows,
        summary=summary,
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        is_current_year=is_current_year,
        selected_date=selected_date.strftime('%Y-%m-%d'),
        year_label='%s-01-01 ~ %s-12-31' % (year_start.year, year_start.year),
        selected_year=str(year_start.year),
        year_blocks=year_blocks,
        database_dir=DATABASE_DIR,
        settings_path=SETTINGS_PATH,
        status_db_path=DAILY_STATS_DB,
    )


@bp.route('/api/annualstats/scraped-totals')
def annual_stats_scraped_totals():
    selected_date = parse_selected_year_date()
    rows, _summary, _year_start, _year_end, _is_current_year = build_annual_report(selected_date)
    return jsonify(build_rows_scraped_totals_payload(rows))


@bp.route('/api/task-status')
def task_status():
    """Return the completion status for a job id.

    ``finish_time`` is treated as authoritative for completion.  This is
    intentional because imported/older records can retain ``status=running``
    even though the close event populated ``finish_time``.
    """
    job_id = request.args.get('job_id', '').strip()
    if not job_id:
        return jsonify({'status': 'error', 'message': 'job_id is required'}), 400

    ensure_status_db()
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT fact_id, source_type, source_pk, task_key, task_id,
                   task_name, project, spider, job_id, server, planned_time,
                   start_time, finish_time, run_date, status, scraped_items,
                   failure_reason, updated_at
            FROM task_execution_fact
            WHERE job_id = ?
            ORDER BY fact_id
            ''',
            (job_id,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return jsonify({
            'status': 'not_found',
            'job_id': job_id,
            'completed': False,
            'message': 'job not found',
        }), 404

    terminal_statuses = {'success', 'failed', 'failure', 'cancelled', 'canceled', 'finished', 'stopped'}
    records = []
    for row in rows:
        item = dict(row)
        raw_status = (item.get('status') or '').strip().lower()
        completed = bool(item.get('finish_time')) or raw_status in terminal_statuses
        item['raw_status'] = item.get('status')
        item['effective_status'] = raw_status if raw_status in terminal_statuses else ('completed' if item.get('finish_time') else 'running')
        item['completed'] = completed
        records.append(item)

    completed = all(item['completed'] for item in records)
    effective_statuses = {item['effective_status'] for item in records}
    overall_status = next(iter(effective_statuses)) if len(effective_statuses) == 1 else ('completed' if completed else 'running')
    return jsonify({
        'status': 'ok',
        'job_id': job_id,
        'completed': completed,
        'effective_status': overall_status,
        'record_count': len(records),
        'records': records,
    })


@bp.route('/api/spider-tasks')
def spider_tasks():
    """Return recent execution details for a spider name."""
    spider_name = (request.args.get('spider_name') or request.args.get('spider') or '').strip()
    if not spider_name:
        return jsonify({'status': 'error', 'message': 'spider_name is required'}), 400
    try:
        limit = int(request.args.get('limit', '100'))
    except ValueError:
        return jsonify({'status': 'error', 'message': 'limit must be an integer'}), 400
    limit = max(1, min(limit, 1000))

    ensure_status_db()
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT fact_id, source_type, source_pk, task_key, task_id,
                   task_name, project, spider, job_id, server, node,
                   planned_time, start_time, finish_time, run_date,
                   status, scraped_items, failure_reason, is_timer_child,
                   created_at, updated_at
            FROM task_execution_fact
            WHERE spider = ?
            ORDER BY COALESCE(start_time, planned_time, created_at) DESC,
                     fact_id DESC
            LIMIT ?
            ''',
            (spider_name, limit),
        ).fetchall()
    finally:
        conn.close()

    terminal_statuses = {'success', 'failed', 'failure', 'cancelled', 'canceled', 'finished', 'stopped'}
    tasks = []
    for row in rows:
        item = dict(row)
        raw_status = (item.get('status') or '').strip().lower()
        item['raw_status'] = item.get('status')
        item['completed'] = bool(item.get('finish_time')) or raw_status in terminal_statuses
        item['effective_status'] = raw_status if raw_status in terminal_statuses else ('completed' if item.get('finish_time') else 'running')
        tasks.append(item)

    return jsonify({
        'status': 'ok',
        'spider_name': spider_name,
        'count': len(tasks),
        'limit': limit,
        'tasks': tasks,
    })


@bp.route('/api/spider-monitor/coverage', methods=['POST'])
def spider_monitor_coverage():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({'status': 'error', 'message': 'invalid json payload'}), 400
    try:
        record = save_spider_monitor_payload(payload)
        maybe_send_spider_monitor_close_mail(record)
    except ValueError as err:
        return jsonify({'status': 'error', 'message': str(err)}), 400
    return jsonify({
        'status': 'ok',
        'saved': 1,
        'run_id': record['run_id'],
        'job_id': record['job_id'],
        'event_status': record['status'],
    })


@bp.route('/healthz')
def healthz():
    return {
        'status': 'ok',
        'timer_tasks_db': TIMER_TASKS_DB,
        'jobs_db': JOBS_DB,
        'apscheduler_db': APSCHEDULER_DB,
        'status_db': DAILY_STATS_DB,
        'settings_path': SETTINGS_PATH,
    }
