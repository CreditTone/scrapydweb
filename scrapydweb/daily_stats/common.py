# coding: utf-8
import os
import pickle
import re
import sqlite3
from datetime import datetime, timedelta


BASE_DIR = os.getcwd()
from scrapydweb.vars import DATABASE_PATH as DATABASE_DIR, SCRAPYDWEB_SETTINGS_PY
TIMER_TASKS_DB = os.path.join(DATABASE_DIR, 'timer_tasks.db')
JOBS_DB = os.path.join(DATABASE_DIR, 'jobs.db')
APSCHEDULER_DB = os.path.join(DATABASE_DIR, 'apscheduler.db')
DAILY_STATS_DB = os.path.join(DATABASE_DIR, 'daily_stats.db')
SETTINGS_PATH = os.path.join(BASE_DIR, SCRAPYDWEB_SETTINGS_PY)

SCRAPYD_SERVER_PATTERN = re.compile(r"""
    ^
    (?:
        (?:(.*?):)
        (?:(.*?)@)
    )?
    (.*?)
    (?::(.*?))?
    (?:\#(.*?))?
    $
""", re.X)
STRICT_NAME_PATTERN = re.compile(r'[^0-9A-Za-z_]')

DATETIME_FORMATS = (
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S',
)
DATE_FORMAT = '%Y-%m-%d'


def parse_scrapyd_servers(settings_module):
    parsed = []
    for server in getattr(settings_module, 'SCRAPYD_SERVERS', []):
        if isinstance(server, tuple):
            if len(server) != 5:
                continue
            username, password, ip, port, group = server
        else:
            match = re.search(SCRAPYD_SERVER_PATTERN, server.strip())
            if not match:
                continue
            username, password, ip, port, group = match.groups()
        ip = ip.strip() if ip and ip.strip() else '127.0.0.1'
        port = port.strip() if port and port.strip() else '6800'
        group = group.strip() if group and group.strip() else ''
        auth = (username, password) if username and password else None
        parsed.append((group, '%s:%s' % (ip, port), auth))
    parsed = list(dict.fromkeys(parsed))
    return parsed


SCRAPYD_SERVERS = []
SERVER_AUTH_MAP = dict((server, auth) for _group, server, auth in SCRAPYD_SERVERS)
JOB_TABLE_SERVER_MAP = dict(
    (re.sub(STRICT_NAME_PATTERN, '_', server), server)
    for _group, server, _auth in SCRAPYD_SERVERS
)


def configure(config):
    """Bind the subsystem to ScrapydWeb's normalized runtime settings."""
    servers = []
    auths = config.get('SCRAPYD_SERVERS_AUTHS', [])
    groups = config.get('SCRAPYD_SERVERS_GROUPS', [])
    for index, server in enumerate(config.get('SCRAPYD_SERVERS', [])):
        servers.append((groups[index] if index < len(groups) else '', server,
                        auths[index] if index < len(auths) else None))
    SCRAPYD_SERVERS[:] = servers
    SERVER_AUTH_MAP.clear()
    SERVER_AUTH_MAP.update((server, auth) for _group, server, auth in servers)
    JOB_TABLE_SERVER_MAP.clear()
    JOB_TABLE_SERVER_MAP.update(
        (re.sub(STRICT_NAME_PATTERN, '_', server), server)
        for _group, server, _auth in servers
    )


def validate_scrapydweb_schema():
    """Reject incompatible databases without modifying them."""
    requirements = {
        'task': {'id', 'name', 'project', 'spider', 'trigger'},
        'task_result': {'id', 'task_id', 'execute_time', 'fail_count', 'pass_count'},
        'task_job_result': {'id', 'task_result_id', 'run_time', 'server', 'status', 'result'},
    }
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        for table, required in requirements.items():
            columns = {row['name'] for row in conn.execute(
                'PRAGMA table_info("%s")' % table
            ).fetchall()}
            missing = required - columns
            if missing:
                raise RuntimeError('Incompatible ScrapydWeb schema: table %s missing %s' %
                                   (table, ', '.join(sorted(missing))))
    finally:
        conn.close()


def connect_readonly(path):
    conn = sqlite3.connect('file:%s?mode=ro' % path, uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def connect_writable(path):
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_status_db():
    conn = connect_writable(DAILY_STATS_DB)
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS task_job_status (
                task_job_result_id INTEGER PRIMARY KEY,
                task_id INTEGER NOT NULL,
                task_result_id INTEGER NOT NULL,
                job_id TEXT NOT NULL,
                server TEXT NOT NULL,
                project TEXT NOT NULL,
                spider TEXT NOT NULL,
                run_time TEXT,
                scraped_items INTEGER,
                sync_status TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_retry_at TEXT,
                updated_at TEXT NOT NULL
            )
            '''
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_job_status_task_result_id ON task_job_status (task_result_id)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_job_status_next_retry_at ON task_job_status (next_retry_at)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_job_status_run_time ON task_job_status (run_time)'
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS sync_cursor (
                cursor_key TEXT PRIMARY KEY,
                cursor_value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS task_execution_fact (
                fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_pk TEXT NOT NULL,
                task_key TEXT NOT NULL,
                task_id INTEGER,
                task_name TEXT,
                project TEXT NOT NULL,
                spider TEXT NOT NULL,
                job_id TEXT,
                server TEXT,
                node INTEGER,
                planned_time TEXT,
                start_time TEXT,
                finish_time TEXT,
                run_date TEXT NOT NULL,
                run_week_start TEXT NOT NULL,
                run_month TEXT NOT NULL,
                run_year INTEGER NOT NULL,
                status TEXT NOT NULL,
                scraped_items INTEGER,
                failure_reason TEXT,
                is_timer_child INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(source_type, source_pk)
            )
            '''
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_execution_fact_run_date ON task_execution_fact (run_date)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_execution_fact_week ON task_execution_fact (run_week_start)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_execution_fact_month ON task_execution_fact (run_month)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_execution_fact_year ON task_execution_fact (run_year)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_execution_fact_task_key ON task_execution_fact (task_key)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_execution_fact_timer_child ON task_execution_fact (is_timer_child, run_date)'
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS task_daily_agg (
                run_date TEXT NOT NULL,
                run_week_start TEXT NOT NULL,
                run_month TEXT NOT NULL,
                run_year INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                task_key TEXT NOT NULL,
                task_id INTEGER,
                task_name TEXT,
                project TEXT NOT NULL,
                spider TEXT NOT NULL,
                should_execute INTEGER NOT NULL DEFAULT 0,
                actual_execute INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                running_count INTEGER NOT NULL DEFAULT 0,
                scraped_items_total INTEGER NOT NULL DEFAULT 0,
                latest_execute_time TEXT,
                top_failure_reason TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_date, source_type, task_key)
            )
            '''
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_daily_agg_week ON task_daily_agg (run_week_start)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_daily_agg_month ON task_daily_agg (run_month)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_task_daily_agg_year ON task_daily_agg (run_year)'
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS task_weekly_agg (
                run_week_start TEXT NOT NULL,
                source_type TEXT NOT NULL,
                task_key TEXT NOT NULL,
                task_id INTEGER,
                task_name TEXT,
                project TEXT NOT NULL,
                spider TEXT NOT NULL,
                should_execute INTEGER NOT NULL DEFAULT 0,
                actual_execute INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                running_count INTEGER NOT NULL DEFAULT 0,
                scraped_items_total INTEGER NOT NULL DEFAULT 0,
                latest_execute_time TEXT,
                top_failure_reason TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_week_start, source_type, task_key)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS task_monthly_agg (
                run_month TEXT NOT NULL,
                source_type TEXT NOT NULL,
                task_key TEXT NOT NULL,
                task_id INTEGER,
                task_name TEXT,
                project TEXT NOT NULL,
                spider TEXT NOT NULL,
                should_execute INTEGER NOT NULL DEFAULT 0,
                actual_execute INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                running_count INTEGER NOT NULL DEFAULT 0,
                scraped_items_total INTEGER NOT NULL DEFAULT 0,
                latest_execute_time TEXT,
                top_failure_reason TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_month, source_type, task_key)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS task_yearly_agg (
                run_year INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                task_key TEXT NOT NULL,
                task_id INTEGER,
                task_name TEXT,
                project TEXT NOT NULL,
                spider TEXT NOT NULL,
                should_execute INTEGER NOT NULL DEFAULT 0,
                actual_execute INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                running_count INTEGER NOT NULL DEFAULT 0,
                scraped_items_total INTEGER NOT NULL DEFAULT 0,
                latest_execute_time TEXT,
                top_failure_reason TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_year, source_type, task_key)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS spider_monitor_coverage (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                job_type TEXT NOT NULL,
                project TEXT NOT NULL,
                spider_name TEXT NOT NULL,
                job_id TEXT NOT NULL,
                job_main_id TEXT,
                status TEXT NOT NULL,
                total_nums INTEGER,
                url_nums INTEGER,
                items_nums INTEGER,
                create_time_ms INTEGER,
                event_time TEXT,
                raw_payload TEXT NOT NULL,
                mail_sent_at TEXT,
                mail_last_error TEXT,
                received_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (run_id, job_id, status)
            )
            '''
        )
        existing_columns = set(
            row['name']
            for row in conn.execute("PRAGMA table_info('spider_monitor_coverage')").fetchall()
        )
        if 'mail_sent_at' not in existing_columns:
            conn.execute('ALTER TABLE spider_monitor_coverage ADD COLUMN mail_sent_at TEXT')
        if 'mail_last_error' not in existing_columns:
            conn.execute('ALTER TABLE spider_monitor_coverage ADD COLUMN mail_last_error TEXT')
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_spider_monitor_coverage_job_id ON spider_monitor_coverage (job_id)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_spider_monitor_coverage_spider_name ON spider_monitor_coverage (spider_name)'
        )
        conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_spider_monitor_coverage_event_time ON spider_monitor_coverage (event_time)'
        )
        conn.commit()
    finally:
        conn.close()


def parse_db_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def now_string():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def parse_date_string(value):
    if not value:
        return None
    return datetime.strptime(value, DATE_FORMAT)


def format_date(value):
    dt = parse_db_datetime(value)
    return dt.strftime(DATE_FORMAT) if dt else ''


def get_week_start_from_date(value):
    dt = parse_db_datetime(value)
    if not dt:
        dt = parse_date_string(value)
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    report_weekday = 3
    days_since_report_start = (day_start.weekday() - report_weekday) % 7
    return day_start - timedelta(days=days_since_report_start)


def get_week_start_string(value):
    return get_week_start_from_date(value).strftime(DATE_FORMAT)


def get_month_string(value):
    dt = parse_db_datetime(value)
    if not dt:
        dt = parse_date_string(value)
    return dt.strftime('%Y-%m')


def get_year_int(value):
    dt = parse_db_datetime(value)
    if not dt:
        dt = parse_date_string(value)
    return dt.year


def build_task_key(source_type, task_id, spider):
    if task_id:
        return '%s:%s' % (source_type, task_id)
    return '%s:%s' % (source_type, spider)


def get_cursor(cursor_key, default='0'):
    ensure_status_db()
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        row = conn.execute(
            'SELECT cursor_value FROM sync_cursor WHERE cursor_key = ?',
            (cursor_key,)
        ).fetchone()
        return row['cursor_value'] if row else default
    finally:
        conn.close()


def set_cursor(cursor_key, cursor_value):
    ensure_status_db()
    conn = connect_writable(DAILY_STATS_DB)
    try:
        updated_at = now_string()
        result = conn.execute(
            '''
            UPDATE sync_cursor
            SET cursor_value = ?, updated_at = ?
            WHERE cursor_key = ?
            ''',
            (str(cursor_value), updated_at, cursor_key)
        )
        if result.rowcount == 0:
            conn.execute(
                '''
                INSERT INTO sync_cursor (cursor_key, cursor_value, updated_at)
                VALUES (?, ?, ?)
                ''',
                (cursor_key, str(cursor_value), updated_at)
            )
        conn.commit()
    finally:
        conn.close()


def format_datetime(value):
    dt = parse_db_datetime(value)
    return dt.strftime('%Y-%m-%d %H:%M:%S') if dt else 'N/A'


def get_fire_times_by_day(trigger, day_start, day_end):
    timezone = getattr(trigger, 'timezone', None)
    if timezone:
        if hasattr(timezone, 'localize'):
            day_start = timezone.localize(day_start)
            day_end = timezone.localize(day_end)
        else:
            day_start = day_start.replace(tzinfo=timezone)
            day_end = day_end.replace(tzinfo=timezone)
    fire_times = []
    previous_fire_time = None
    current_time = day_start
    for _ in range(1000):
        next_fire_time = trigger.get_next_fire_time(previous_fire_time, current_time)
        if not next_fire_time or next_fire_time >= day_end:
            break
        fire_times.append(next_fire_time)
        previous_fire_time = next_fire_time
        current_time = next_fire_time + timedelta(microseconds=1)
    return fire_times


def load_job_states():
    states = {}
    conn = connect_readonly(APSCHEDULER_DB)
    try:
        rows = conn.execute('SELECT id, next_run_time, job_state FROM apscheduler_jobs').fetchall()
        for row in rows:
            try:
                state = pickle.loads(row['job_state'])
            except Exception:
                continue
            states[str(row['id'])] = dict(
                next_run_time=row['next_run_time'],
                trigger=state.get('trigger'),
            )
    finally:
        conn.close()
    return states


def load_tasks():
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        rows = conn.execute('SELECT * FROM task ORDER BY id DESC').fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def load_task_results_by_task(day_start, day_end):
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        rows = conn.execute(
            '''
            SELECT *
            FROM task_result
            WHERE execute_time >= ? AND execute_time < ?
            ORDER BY execute_time DESC
            ''',
            (day_start.strftime('%Y-%m-%d %H:%M:%S'), day_end.strftime('%Y-%m-%d %H:%M:%S'))
        ).fetchall()
        grouped = {}
        for row in rows:
            grouped.setdefault(row['task_id'], []).append(dict(row))
        for task_id in grouped:
            grouped[task_id] = list(reversed(grouped[task_id]))
        return grouped
    finally:
        conn.close()


def load_task_job_results(task_result_ids):
    if not task_result_ids:
        return {}
    placeholders = ','.join('?' for _ in task_result_ids)
    conn = connect_readonly(TIMER_TASKS_DB)
    try:
        rows = conn.execute(
            'SELECT * FROM task_job_result WHERE task_result_id IN (%s) ORDER BY id DESC' % placeholders,
            task_result_ids
        ).fetchall()
        grouped = {}
        for row in rows:
            grouped.setdefault(row['task_result_id'], []).append(dict(row))
        return grouped
    finally:
        conn.close()


def load_status_rows(task_job_result_ids):
    ensure_status_db()
    if not task_job_result_ids:
        return {}
    placeholders = ','.join('?' for _ in task_job_result_ids)
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            'SELECT * FROM task_job_status WHERE task_job_result_id IN (%s)' % placeholders,
            task_job_result_ids
        ).fetchall()
        return dict((row['task_job_result_id'], dict(row)) for row in rows)
    finally:
        conn.close()


def get_job_tables():
    conn = connect_readonly(JOBS_DB)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        existing = set(row['name'] for row in rows)
        tables = []
        for index, (_group, server, _auth) in enumerate(SCRAPYD_SERVERS, 1):
            table_name = re.sub(STRICT_NAME_PATTERN, '_', server)
            if table_name in existing:
                tables.append((index, table_name))
        return tables
    finally:
        conn.close()


def get_existing_job_tables():
    conn = connect_readonly(JOBS_DB)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        ignored = set(['monitor', 'source_config'])
        tables = []
        index = 1
        for row in rows:
            table_name = row['name']
            if table_name in ignored:
                continue
            tables.append((index, table_name))
            index += 1
        return tables
    finally:
        conn.close()


def get_manual_job_status(record):
    if record['status'] == '1':
        return '执行中', 'warning'
    if record['status'] == '2':
        return '已结束', 'safe'
    return '待执行', 'normal'


def load_aggregate_rows(table_name, period_column, period_value):
    ensure_status_db()
    conn = connect_readonly(DAILY_STATS_DB)
    try:
        rows = conn.execute(
            'SELECT * FROM {table_name} WHERE {period_column} = ?'.format(
                table_name=table_name, period_column=period_column
            ),
            (period_value,)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()
