# coding: utf-8
from collections import OrderedDict, defaultdict
import re

from flask import flash, redirect, render_template, request, url_for
from logparser import SETTINGS_PY_PATH as LOGPARSER_SETTINGS_PY_PATH

from ...common import json_dumps
from ...utils.log_cleanup import cleanup_logs, refresh_log_cleanup_job, save_log_cleanup_config
from ...vars import SCHEDULER_STATE_DICT
from ..baseview import BaseView
from .log_cleanup import get_log_cleanup_view_data


class SettingsView(BaseView):
    methods = ['GET', 'POST']

    def __init__(self):
        super(SettingsView, self).__init__()

        self.template = 'scrapydweb/settings.html'
        self.kwargs = dict(node=self.node)

    def dispatch_request(self, **kwargs):
        if request.method == 'POST':
            self.handle_post()
            return redirect(url_for('settings', node=self.node))
        self.update_kwargs()
        return render_template(self.template, **self.kwargs)

    def handle_post(self):
        action = (request.form.get('action') or 'save_log_cleanup').strip()
        data = dict(
            enabled=request.form.get('enabled') == 'on',
            log_dir=(request.form.get('log_dir') or '').strip(),
            size_mb=request.form.get('size_mb', '').strip(),
            keep_days=request.form.get('keep_days', '').strip(),
            interval_hours=request.form.get('interval_hours', '').strip(),
        )
        config = save_log_cleanup_config(data, default_log_dir=self.LOCAL_SCRAPYD_LOGS_DIR)
        refresh_log_cleanup_job(default_log_dir=self.LOCAL_SCRAPYD_LOGS_DIR)
        flash('日志清理配置已保存', 'info')
        if action == 'run_log_cleanup_now':
            result = cleanup_logs(config=config, default_log_dir=self.LOCAL_SCRAPYD_LOGS_DIR)
            flash('日志清理已执行：%s' % result.get('message', ''), 'info')

    @staticmethod
    def json_dumps(obj, sort_keys=False):
        string = json_dumps(obj, sort_keys=sort_keys)
        return string.replace(' true', ' True').replace(' false', ' False').replace(' null', ' None')

    @staticmethod
    def protect(string):
        if not isinstance(string, str):
            return string
        length = len(string)
        if length < 4:
            return '*' * length
        elif length < 12:
            return ''.join([string[i] if not i%2 else '*' for i in range(0, length)])
        else:
            return re.sub(r'^.{4}(.*?).{4}$', r'****\1****', string)

    @staticmethod
    def hide_account(string):
        return re.sub(r'//.+@', '//', string)

    def update_kwargs(self):
        # User settings
        self.kwargs['DEFAULT_SETTINGS_PY_PATH'] = self.handle_slash(self.DEFAULT_SETTINGS_PY_PATH)
        self.kwargs['SCRAPYDWEB_SETTINGS_PY_PATH'] = self.handle_slash(self.SCRAPYDWEB_SETTINGS_PY_PATH)
        self.kwargs['MAIN_PID'] = self.MAIN_PID
        self.kwargs['LOGPARSER_PID'] = self.LOGPARSER_PID
        self.kwargs['POLL_PID'] = self.POLL_PID

        # ScrapydWeb
        self.kwargs['python_version'] = self.PYTHON_VERSION
        self.kwargs['scrapydweb_version'] = self.SCRAPYDWEB_VERSION
        self.kwargs['scrapydweb_server'] = self.json_dumps(dict(
            SCRAPYDWEB_BIND=self.SCRAPYDWEB_BIND,
            SCRAPYDWEB_PORT=self.SCRAPYDWEB_PORT,
            URL_SCRAPYDWEB=self.URL_SCRAPYDWEB,
            ENABLE_AUTH=self.ENABLE_AUTH,
            USERNAME=self.protect(self.USERNAME),
            PASSWORD=self.protect(self.PASSWORD)
        ))
        self.kwargs['ENABLE_HTTPS'] = self.ENABLE_HTTPS
        self.kwargs['enable_https_details'] = self.json_dumps(dict(
            CERTIFICATE_FILEPATH=self.CERTIFICATE_FILEPATH,
            PRIVATEKEY_FILEPATH=self.PRIVATEKEY_FILEPATH
        ))

        # Scrapy
        self.kwargs['scrapy_version'] = self.SCRAPY_VERSION
        self.kwargs['SCRAPY_PROJECTS_DIR'] = self.handle_slash(self.SCRAPY_PROJECTS_DIR) or "''"

        # Scrapyd
        self.kwargs['scrapyd_version'] = self.SCRAPYD_VERSION
        servers = defaultdict(list)
        for group, server, auth in zip(self.SCRAPYD_SERVERS_GROUPS, self.SCRAPYD_SERVERS, self.SCRAPYD_SERVERS_AUTHS):
            _server = '%s:%s@%s' % (self.protect(auth[0]), self.protect(auth[1]), server) if auth else server
            servers[group].append(_server)

        self.kwargs['servers'] = self.json_dumps(servers)
        self.kwargs['CHECK_SCRAPYD_SERVERS'] = self.CHECK_SCRAPYD_SERVERS
        self.kwargs['LOCAL_SCRAPYD_SERVER'] = self.LOCAL_SCRAPYD_SERVER or "''"
        self.kwargs['LOCAL_SCRAPYD_LOGS_DIR'] = self.handle_slash(self.LOCAL_SCRAPYD_LOGS_DIR) or "''"
        self.kwargs['SCRAPYD_LOG_EXTENSIONS'] = self.SCRAPYD_LOG_EXTENSIONS

        # LogParser
        self.kwargs['ENABLE_LOGPARSER'] = self.ENABLE_LOGPARSER
        self.kwargs['logparser_version'] = self.LOGPARSER_VERSION
        self.kwargs['logparser_settings_py_path'] = self.handle_slash(LOGPARSER_SETTINGS_PY_PATH)
        self.kwargs['BACKUP_STATS_JSON_FILE'] = self.BACKUP_STATS_JSON_FILE

        # Log cleanup
        self.kwargs.update(get_log_cleanup_view_data(default_log_dir=self.LOCAL_SCRAPYD_LOGS_DIR))
        self.kwargs['log_cleanup_config_display'] = self.json_dumps(self.kwargs['log_cleanup_config'])
        self.kwargs['log_cleanup_status_display'] = self.json_dumps(self.kwargs['log_cleanup_status'])
        job = self.scheduler.get_job('log_cleanup_job')
        self.kwargs['log_cleanup_next_run_time'] = self.remove_microsecond(job.next_run_time) if job and job.next_run_time else self.NA

        # Timer Tasks
        self.kwargs['scheduler_state'] = SCHEDULER_STATE_DICT[self.scheduler.state]
        self.kwargs['JOBS_SNAPSHOT_INTERVAL'] = self.JOBS_SNAPSHOT_INTERVAL
        self.kwargs['CHECK_TASK_RESULT_INTERVAL'] = self.CHECK_TASK_RESULT_INTERVAL
        self.kwargs['KEEP_TASK_RESULT_LIMIT'] = self.KEEP_TASK_RESULT_LIMIT
        self.kwargs['KEEP_TASK_RESULT_WITHIN_DAYS'] = self.KEEP_TASK_RESULT_WITHIN_DAYS

        # Run Spider
        self.kwargs['run_spider_details'] = self.json_dumps(dict(
            SCHEDULE_EXPAND_SETTINGS_ARGUMENTS=self.SCHEDULE_EXPAND_SETTINGS_ARGUMENTS,
            SCHEDULE_CUSTOM_USER_AGENT=self.SCHEDULE_CUSTOM_USER_AGENT,
            SCHEDULE_USER_AGENT=self.SCHEDULE_USER_AGENT,
            SCHEDULE_ROBOTSTXT_OBEY=self.SCHEDULE_ROBOTSTXT_OBEY,
            SCHEDULE_COOKIES_ENABLED=self.SCHEDULE_COOKIES_ENABLED,
            SCHEDULE_CONCURRENT_REQUESTS=self.SCHEDULE_CONCURRENT_REQUESTS,
            SCHEDULE_DOWNLOAD_DELAY=self.SCHEDULE_DOWNLOAD_DELAY,
            SCHEDULE_ADDITIONAL=self.SCHEDULE_ADDITIONAL
        ))

        # Page Display
        self.kwargs['page_display_details'] = self.json_dumps(dict(
            SHOW_SCRAPYD_ITEMS=self.SHOW_SCRAPYD_ITEMS,
            SHOW_JOBS_JOB_COLUMN=self.SHOW_JOBS_JOB_COLUMN,
            JOBS_FINISHED_JOBS_LIMIT=self.JOBS_FINISHED_JOBS_LIMIT,
            JOBS_RELOAD_INTERVAL=self.JOBS_RELOAD_INTERVAL,
            DAEMONSTATUS_REFRESH_INTERVAL=self.DAEMONSTATUS_REFRESH_INTERVAL
        ))

        # Send text
        self.kwargs['slack_details'] = self.json_dumps(dict(
            SLACK_TOKEN=self.protect(self.SLACK_TOKEN),
            SLACK_CHANNEL=self.SLACK_CHANNEL
        ))
        self.kwargs['telegram_details'] = self.json_dumps(dict(
            TELEGRAM_TOKEN=self.protect(self.TELEGRAM_TOKEN),
            TELEGRAM_CHAT_ID=self.TELEGRAM_CHAT_ID
        ))
        self.kwargs['email_details'] = self.json_dumps(dict(
            EMAIL_SUBJECT=self.EMAIL_SUBJECT,
        ))
        self.kwargs['email_sender_recipients'] = self.json_dumps(dict(
            EMAIL_USERNAME=self.EMAIL_USERNAME,
            EMAIL_PASSWORD=self.protect(self.EMAIL_PASSWORD),
            EMAIL_SENDER=self.EMAIL_SENDER,
            EMAIL_RECIPIENTS=self.EMAIL_RECIPIENTS
        ))
        self.kwargs['email_smtp_settings'] = self.json_dumps(dict(
            SMTP_SERVER=self.SMTP_SERVER,
            SMTP_PORT=self.SMTP_PORT,
            SMTP_OVER_SSL=self.SMTP_OVER_SSL,
            SMTP_CONNECTION_TIMEOUT=self.SMTP_CONNECTION_TIMEOUT,
        ))


        # Monitor & Alert
        self.kwargs['ENABLE_MONITOR'] = self.ENABLE_MONITOR
        self.kwargs['poll_interval'] = self.json_dumps(dict(
            POLL_ROUND_INTERVAL=self.POLL_ROUND_INTERVAL,
            POLL_REQUEST_INTERVAL=self.POLL_REQUEST_INTERVAL,
        ))
        self.kwargs['alert_switcher'] = self.json_dumps(dict(
            ENABLE_SLACK_ALERT=self.ENABLE_SLACK_ALERT,
            ENABLE_TELEGRAM_ALERT=self.ENABLE_TELEGRAM_ALERT,
            ENABLE_EMAIL_ALERT=self.ENABLE_EMAIL_ALERT,
        ))
        self.kwargs['alert_working_time'] = self.json_dumps([
            dict(
                ALERT_WORKING_DAYS="%s" % sorted(self.ALERT_WORKING_DAYS),  # stringify making it displayed in a line
                remark="Monday is 1 and Sunday is 7"
            ),
            dict(
                ALERT_WORKING_HOURS="%s" % sorted(self.ALERT_WORKING_HOURS),
                remark="From 0 to 23"
            )
        ])
        # alert triggers
        d = OrderedDict()
        d['ON_JOB_RUNNING_INTERVAL'] = self.ON_JOB_RUNNING_INTERVAL
        d['ON_JOB_FINISHED'] = self.ON_JOB_FINISHED

        for key in self.ALERT_TRIGGER_KEYS:
            keys = ['LOG_%s_THRESHOLD' % key, 'LOG_%s_TRIGGER_STOP' % key, 'LOG_%s_TRIGGER_FORCESTOP' % key]
            d[key] = {k: getattr(self, k) for k in keys}
        value = self.json_dumps(d)
        value = re.sub(r'True', "<b style='color: red'>True</b>", value)
        value = re.sub(r'(\s[1-9]\d*)', r"<b style='color: red'>\1</b>", value)
        self.kwargs['alert_triggers'] = value

        # System
        self.kwargs['DEBUG'] = self.DEBUG
        self.kwargs['VERBOSE'] = self.VERBOSE
        self.kwargs['DATA_PATH'] = self.DATA_PATH
        self.kwargs['database_details'] = self.json_dumps(dict(
            APSCHEDULER_DATABASE_URI=self.hide_account(self.APSCHEDULER_DATABASE_URI),
            SQLALCHEMY_DATABASE_URI=self.hide_account(self.SQLALCHEMY_DATABASE_URI),
            SQLALCHEMY_BINDS_METADATA=self.hide_account(self.SQLALCHEMY_BINDS['metadata']),
            SQLALCHEMY_BINDS_JOBS=self.hide_account(self.SQLALCHEMY_BINDS['jobs'])
        ))
