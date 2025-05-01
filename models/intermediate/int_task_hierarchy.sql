{{ config(materialized='ephemeral') }}

with recursive task_cte as (
  -- Anchor: start with tasks that have no parent
  select
    t.name,
    t.subject,
    t.status,
    p.project_name,
    t.parent_task,
    array[t.subject] as path
  from {{ ref('stg_tasks') }} t
  join {{ ref('stg_projects') }} p on t.project = p.name
  where t.parent_task is null

  union all

  -- Recursive part: join children to their parent
  select
    t.name,
    t.subject,
    t.status,
    p.project_name,
    t.parent_task,
    cte.path || t.subject
  from {{ ref('stg_tasks') }} t
  join {{ ref('stg_projects') }} p on t.project = p.name
  join task_cte cte on t.parent_task = cte.name
)

select * from task_cte