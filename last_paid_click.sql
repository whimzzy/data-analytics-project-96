with new as (
    select
        visitor_id,
        visit_date,
        source as utm_source,
        medium as utm_medium,
        campaign as utm_campaign
    from sessions
    order by visitor_id asc, visit_date desc
),

new2 as (
    select distinct on (visitor_id) *
    from new
    where utm_medium != 'organic'
)

select
    n.visitor_id,
    n.visit_date,
    n.utm_source,
    n.utm_medium,
    n.utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
from new2 as n
left join leads as l
    on n.visitor_id = l.visitor_id and n.visit_date <= l.created_at
order by
    l.amount desc nulls last, n.visit_date asc, n.utm_source asc,
    n.utm_medium asc, n.utm_campaign asc
limit 10




