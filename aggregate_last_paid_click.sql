with new as (
    select
        s.visitor_id,
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.amount,
        l.status_id,
        row_number()
        over (partition by s.visitor_id order by s.visit_date desc)
        as rn
    from sessions as s
    left join leads as l
        on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
    where s.medium != 'organic'
    order by s.visitor_id asc, s.visit_date desc
),

new2 as (
    select
        visit_date::date as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        count(distinct visitor_id) as visitors_count,
        count(distinct lead_id) as leads_count,
        count(distinct lead_id) filter (
            where status_id = 142
        ) as purchases_count,
        sum(amount) as revenue
    from new
    where rn = 1
    group by
        visit_date::date, utm_source,
        utm_medium,
        utm_campaign
),

tab as (
    select
        utm_source,
        utm_medium,
        utm_campaign,
        campaign_date::date as campaign_date,
        sum(daily_spent) as total_cost
    from vk_ads
    group by campaign_date::date, utm_source, utm_medium, utm_campaign
    union all
    select
        utm_source,
        utm_medium,
        utm_campaign,
        campaign_date::date as campaign_date,
        sum(daily_spent) as total_cost
    from ya_ads
    group by campaign_date::date, utm_source, utm_medium, utm_campaign
)

select
    n2.visit_date,
    n2.visitors_count,
    n2.utm_source,
    n2.utm_medium,
    n2.utm_campaign,
    t.total_cost,
    n2.leads_count,
    n2.purchases_count,
    n2.revenue
from new2 as n2
left join tab as t
    on
        n2.utm_source = t.utm_source
        and n2.utm_medium = t.utm_medium
        and n2.utm_campaign = t.utm_campaign
        and n2.visit_date = t.campaign_date
order by
    n2.revenue desc nulls last,
    n2.visit_date asc,
    n2.visitors_count desc,
    n2.utm_source asc,
    n2.utm_medium asc,
    n2.utm_campaign asc
limit 15
