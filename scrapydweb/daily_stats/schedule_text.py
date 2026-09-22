# coding: utf-8
"""Readable descriptions of the cron fields used by the task scheduler."""
import re

from apscheduler.triggers.cron import CronTrigger


FIELDS = ('year', 'month', 'day', 'week', 'day_of_week', 'hour', 'minute', 'second')
WEEKDAYS = ('周一', '周二', '周三', '周四', '周五', '周六', '周日')


def cron_fields(task):
    # Match ScheduleView's defaults, preserving numeric zero.
    return {name: str(task[name]).strip() if task.get(name) not in (None, '')
            else ('0' if name in ('minute', 'second') else '*') for name in FIELDS}


def raw_schedule(task):
    if task.get('trigger') != 'cron':
        return str(task.get('trigger') or '-')
    return ', '.join('{}={}'.format(name, value) for name, value in cron_fields(task).items())


def _values(expression, minimum, maximum, aliases=()):
    """Expand simple cron lists/ranges/steps; leave special day rules explicit."""
    expression = expression.lower()
    for index, alias in enumerate(aliases):
        expression = expression.replace(alias, str(index + minimum))
    values = set()
    for part in expression.split(','):
        match = re.fullmatch(r'(\*|\d+(?:-\d+)?)(?:/(\d+))?', part.strip())
        if not match:
            raise ValueError('Unsupported expression')
        base, step = match.groups()
        if base == '*':
            start, end = minimum, maximum
        elif '-' in base:
            start, end = map(int, base.split('-'))
        else:
            start = int(base)
            end = maximum if step else start
        values.update(range(start, end + 1, int(step or 1)))
    return sorted(values)


def describe_schedule(task):
    if task.get('trigger') != 'cron':
        return '调度类型：{}'.format(task.get('trigger') or '未配置')
    fields = cron_fields(task)
    try:
        CronTrigger(**fields)  # Validate using the scheduler's own semantics.
        months = _values(fields['month'], 1, 12,
                         ('jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'))
        weekdays = _values(fields['day_of_week'], 0, 6,
                           ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'))
        dates = []
        if fields['year'] != '*':
            dates.append('年份为 {}'.format(fields['year']))
        if len(months) != 12:
            dates.append('每年 {} 月'.format('、'.join(map(str, months))))
        if fields['day'] == 'last':
            dates.append('每月最后一天')
        elif fields['day'] != '*':
            days = _values(fields['day'], 1, 31)
            dates.append('每月 {} 号'.format('、'.join(map(str, days))))
        if len(weekdays) != 7:
            dates.append('每' + '、'.join(WEEKDAYS[day] for day in weekdays))
        if fields['week'] != '*':
            dates.append('ISO 周序号为 {}'.format(fields['week']))
        date_text = '，且'.join(dates) if dates else '每天'
        hours = _values(fields['hour'], 0, 23)
        minutes = _values(fields['minute'], 0, 59)
        seconds = _values(fields['second'], 0, 59)
        if len(hours) * len(minutes) * len(seconds) <= 12:
            times = ['{:02d}:{:02d}{}'.format(h, m, '' if seconds == [0] else ':{:02d}'.format(s))
                     for h in hours for m in minutes for s in seconds]
            time_text = '、'.join(times)
        else:
            parts = []
            for name, values, maximum, unit in (
                    ('hour', hours, 24, '小时'), ('minute', minutes, 60, '分钟'),
                    ('second', seconds, 60, '秒')):
                expression = fields[name]
                if len(values) == maximum:
                    parts.append('每' + unit)
                elif re.fullmatch(r'\*/\d+', expression):
                    parts.append('每{}{}（从 0 起算）'.format(expression[2:], unit))
                else:
                    suffix = {'hour': '时', 'minute': '分', 'second': '秒'}[name]
                    parts.append('在 {}{}'.format('、'.join(map(str, values)), suffix))
            time_text = '，'.join(parts)
        description = '{} {} 启动'.format(date_text, time_text)
    except (ValueError, TypeError, KeyError):
        description = '复杂或无效调度，请查看原始配置'
    extras = []
    for name, label in (('timezone', '时区'), ('start_date', '生效时间'), ('end_date', '截止时间')):
        if task.get(name):
            extras.append('{}：{}'.format(label, task[name]))
    if task.get('jitter') not in (None, '', 0, '0'):
        extras.append('随机延迟最多 {} 秒'.format(task['jitter']))
    return '；'.join([description] + extras)
