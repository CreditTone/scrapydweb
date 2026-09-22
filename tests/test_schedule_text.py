# coding: utf-8
import unittest

from scrapydweb.daily_stats.schedule_text import describe_schedule, raw_schedule


class ScheduleDescriptionTests(unittest.TestCase):
    def describe(self, **fields):
        return describe_schedule(dict(trigger='cron', **fields))

    def test_common_schedules(self):
        for fields, expected in (
            ({'hour': '8', 'minute': '30'}, '每天 08:30 启动'),
            ({'day_of_week': 'sat', 'hour': '12'}, '每周六 12:00 启动'),
            ({'day_of_week': 0, 'hour': 0}, '每周一 00:00 启动'),
            ({'day': '1,15', 'hour': '8'}, '每月 1、15 号 08:00 启动'),
            ({'day': 'last', 'hour': '8'}, '每月最后一天 08:00 启动'),
            ({'hour': '0,12', 'minute': '30'}, '每天 00:30、12:30 启动'),
            ({'hour': '*/6'}, '每天 00:00、06:00、12:00、18:00 启动'),
            ({'hour': '8', 'second': '30'}, '每天 08:00:30 启动'),
        ):
            with self.subTest(fields=fields):
                self.assertEqual(self.describe(**fields), expected)

    def test_restrictions_are_not_lost(self):
        text = self.describe(year='2026', month='jan-mar', day='1', day_of_week='mon-fri',
                             week='1-10', hour='8', timezone='Asia/Shanghai',
                             start_date='2026-01-01', end_date='2026-03-31', jitter=30)
        for value in ('2026', '1、2、3 月', '每月 1 号', '且每周一、周二、周三、周四、周五',
                      'ISO 周序号为 1-10', 'Asia/Shanghai', '2026-01-01', '2026-03-31', '最多 30 秒'):
            self.assertIn(value, text)

    def test_minute_intervals(self):
        self.assertEqual(self.describe(minute='*/10'),
                         '每天 每小时，每10分钟（从 0 起算），在 0秒 启动')

    def test_complex_and_invalid_rules_fall_back(self):
        for fields in ({'day': '3rd fri'}, {'hour': '99'}, {'minute': '*/0'}):
            self.assertIn('请查看原始配置', self.describe(**fields))

    def test_raw_fields_keep_zero_and_scheduler_defaults(self):
        text = raw_schedule({'trigger': 'cron', 'hour': 0, 'day_of_week': 0})
        for value in ('hour=0', 'day_of_week=0', 'minute=0', 'second=0', 'year=*', 'week=*'):
            self.assertIn(value, text)
