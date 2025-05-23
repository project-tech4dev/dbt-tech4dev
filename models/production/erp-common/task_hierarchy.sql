{{ config(
    tags=['projects']
) }}

with leaf_completion as (
  select
    t.name,
    case when t.status = 'Completed' then 100.0 else 0.0 end as leaf_completion
  from {{ ref('int_task_hierarchy') }} t
  where not exists (
    select 1 from {{ ref('int_tasks') }} t2
    where t2.parent_task = t.name
  )
),

ancestor_completion as (
  select
    ancestor.name as task_name,
    avg(lc.leaf_completion) as avg_completion
  from {{ ref('int_task_hierarchy') }} as ancestor
  join {{ ref('int_task_hierarchy') }} as descendant
    on descendant.path[1:array_length(ancestor.path,1)] = ancestor.path
  join leaf_completion lc on descendant.name = lc.name
  group by ancestor.name
)

select
  case when array_length(t.path, 1) >= 1 then t.path[1] end as "level_1",
  case when array_length(t.path, 1) >= 2 then t.path[2] end as "level_2",
  t.status,
  t.predecessor,
  t.successor,
  t.project_name,
  t.description,
  substring(description, 'https?:\/\/[^\s<>"]+') AS extracted_link,
  round(coalesce(ac.avg_completion, 
         case when t.status = 'Completed' then 100.0 else 0.0 end), 2) as "completion_float"
from {{ ref('int_task_hierarchy') }} t
left join ancestor_completion ac on t.name = ac.task_name
where array_length(t.path, 1) <= 2
order by t.path