import sqlite3

import pytest

from scrapydweb import create_app
from scrapydweb.daily_stats import common
from scrapydweb.daily_stats import events
from scrapydweb.daily_stats.handlers import HANDLERS


def test_daily_stats_routes_render():
    app = create_app({'TESTING': True})
    client = app.test_client()

    for path in (
        '/stats/',
        '/stats/dailystats/',
        '/stats/weeklystats/',
        '/stats/annualstats/',
        '/stats/coveragestats/',
        '/stats/healthz',
    ):
        assert client.get(path).status_code == 200


def test_schema_guard_rejects_incompatible_database(tmp_path, monkeypatch):
    database = tmp_path / 'timer_tasks.db'
    connection = sqlite3.connect(str(database))
    try:
        connection.execute('CREATE TABLE task (id INTEGER PRIMARY KEY)')
        connection.execute('CREATE TABLE task_result (id INTEGER PRIMARY KEY)')
        connection.execute('CREATE TABLE task_job_result (id INTEGER PRIMARY KEY)')
        connection.commit()
    finally:
        connection.close()

    monkeypatch.setattr(common, 'TIMER_TASKS_DB', str(database))
    with pytest.raises(RuntimeError, match='Incompatible ScrapydWeb schema'):
        common.validate_scrapydweb_schema()


def test_event_dispatches_to_incremental_handler(monkeypatch):
    payloads = []
    monkeypatch.setitem(
        HANDLERS,
        events.JOB_FINISHED,
        lambda **payload: payloads.append(payload),
    )

    events.dispatch(events.StatsEvent(
        events.JOB_FINISHED,
        {'job_id': 'job-1', 'server': '127.0.0.1:6800'},
    ))

    assert payloads == [{'job_id': 'job-1', 'server': '127.0.0.1:6800'}]
