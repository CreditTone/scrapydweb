# 统计接口说明

统计接口由 ScrapydWeb 的 `daily_stats` 模块提供，基础路径为：

```text
http://<scrapydweb-host>/stats
```

## 查询任务状态

### 请求

```http
GET /stats/api/task-status?job_id=<任务ID>
```

示例：

```bash
curl 'http://10.20.8.115/stats/api/task-status?job_id=task_example_2026-01-01T00_00_00'
```

### 返回字段

| 字段 | 说明 |
| --- | --- |
| `status` | 接口状态，成功为 `ok`，任务不存在为 `not_found` |
| `job_id` | 查询的任务 ID |
| `completed` | 是否已完成，布尔值 |
| `effective_status` | 归一化后的状态 |
| `record_count` | 匹配到的事实记录数 |
| `records` | 任务执行明细 |

`records` 中包含 `fact_id`、`source_type`、`task_key`、`task_name`、`project`、`spider`、`job_id`、`server`、`planned_time`、`start_time`、`finish_time`、`status`、`scraped_items`、`failure_reason` 等字段。

### 状态判定

- 原始状态为 `success`、`failed`、`failure`、`cancelled`、`canceled`、`finished` 或 `stopped` 时，任务视为已完成。
- 即使原始 `status` 仍为 `running`，只要 `finish_time` 不为空，也视为已完成，并返回 `completed: true`、`effective_status: completed`。
- 没有匹配记录时返回 HTTP `404`。

## 查询 Spider 任务明细

### 请求

```http
GET /stats/api/spider-tasks?spider_name=<Spider名称>&limit=<数量>
```

`spider` 可作为 `spider_name` 的别名。`limit` 默认 100，最大 1000。

示例：

```bash
curl 'http://10.20.8.115/stats/api/spider-tasks?spider_name=example_spider&limit=200'
```

### 返回字段

| 字段 | 说明 |
| --- | --- |
| `status` | 接口状态，成功为 `ok` |
| `spider_name` | 查询的 Spider 名称 |
| `count` | 本次返回数量 |
| `limit` | 本次查询限制 |
| `tasks` | 按开始时间倒序排列的任务明细 |

`tasks` 中每条记录包含任务 ID、任务名称、项目、服务器、计划时间、开始时间、结束时间、原始状态、归一化状态、是否完成、抓取数量和失败原因等信息。

## 示例响应

```json
{
  "status": "ok",
  "job_id": "task_53_2026-08-15T12_00_00",
  "completed": true,
  "effective_status": "completed",
  "record_count": 2,
  "records": []
}
```

## 鉴权

接口使用 ScrapydWeb 现有的访问控制配置。若配置了 Basic Auth，是否免鉴权由当前实例的统计页面公开访问配置决定。
