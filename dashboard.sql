select count(distinct visitor_id), visit_date::date
from sessions 
group by visit_date::date


select count(distinct visitor_id), to_char(visit_date, 'DD') as day, source, medium, campaign
from sessions 
group by to_char(visit_date, 'DD'), source, medium, campaign

select count(distinct visitor_id), to_char(visit_date, 'W') as week, source, medium, campaign
from sessions 
group by to_char(visit_date, 'W'), source, medium, campaign

select count(distinct visitor_id), to_char(visit_date, 'MM') as month, source, medium, campaign
from sessions 
group by to_char(visit_date, 'MM'), source, medium, campaign

select count(distinct l.lead_id), s.visit_date::date
from sessions s 
left join leads l
on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
group by s.visit_date::date


select round(count(distinct l.lead_id)::numeric / count(distinct s.visitor_id)::numeric, 4) as conversion_rate, 
s.visit_date::date as visitdate
from sessions s 
left join leads l
on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
group by s.visit_date::date


select round(count(distinct status_id)::numeric / count(distinct lead_id)::numeric, 4) as paying_leads,
created_at::date as created_at
from leads
where status_id = 142
group by created_at::date



select *
from (
select utm_medium, 
campaign_date::date as campaign_date, sum(daily_spent) as total_cost
from vk_ads 
group by campaign_date::date, utm_medium
union all 
select utm_medium, 
campaign_date::date as campaign_date, sum(daily_spent) as total_cost
from ya_ads 
group by campaign_date::date, utm_medium)
tab
order by campaign_date


with new as (
select s.visitor_id as visitor_id, s.visit_date as visit_date, s.source as utm_source, 
s.medium as utm_medium, s.campaign as utm_campaign, l.lead_id as lead_id, 
l.amount as amount, l.status_id as status_id,
row_number() over (partition by s.visitor_id order by s.visit_date desc) as rn 
from sessions s
left join leads l
on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
where s.medium != 'organic'
order by s.visitor_id, s.visit_date desc
), new2 as (
select
        count(distinct visitor_id) as visitors_count,
        visit_date::date as visit_date,
        utm_source as utm_source,
        utm_medium as utm_medium,
        utm_campaign as utm_campaign,
        count(distinct lead_id) as leads_count, 
        count(distinct lead_id) filter (where status_id = 142) as purchases_count, 
        sum(amount) as revenue
    from new
    where rn = 1
    group by visit_date::date, utm_source,
        utm_medium,
        utm_campaign
), tab as (
select utm_source, utm_medium, utm_campaign, 
campaign_date::date as campaign_date, sum(daily_spent) as total_cost
from vk_ads 
group by campaign_date::date, utm_source, utm_medium, utm_campaign
union all 
select utm_source, utm_medium, utm_campaign, 
campaign_date::date as campaign_date, sum(daily_spent) as total_cost
from ya_ads 
group by campaign_date::date, utm_source, utm_medium, utm_campaign
), tab2 as (
	select n2.visit_date as visit_date, 
	n2.visitors_count as visitors_count,
	n2.utm_source as utm_source, 
	n2.utm_medium as utm_medium,
	n2.utm_campaign as utm_campaign, 
	t.total_cost::float as total_cost,
	n2.leads_count as leads_count, 
	n2.purchases_count as purchases_count,
	n2.revenue::float as revenue
from new2 n2
left join tab t
on n2.utm_source = t.utm_source
and n2.utm_medium = t.utm_medium
and n2.utm_campaign = t.utm_campaign
and n2.visit_date = t.campaign_date
order by n2.revenue desc nulls last, 
n2.visit_date asc,
n2.visitors_count desc,
n2.utm_source asc,
n2.utm_medium asc,
n2.utm_campaign asc
)
select utm_source, utm_medium, utm_campaign, 
(revenue - total_cost)/total_cost * 100.00 as roi
from tab2


with tab as (
select utm_source, utm_medium, utm_campaign, 
campaign_date::date as campaign_date, sum(daily_spent) as total_cost
from vk_ads 
group by campaign_date::date, utm_source, utm_medium, utm_campaign
union all 
select utm_source, utm_medium, utm_campaign, 
campaign_date::date as campaign_date, sum(daily_spent) as total_cost
from ya_ads 
group by campaign_date::date, utm_source, utm_medium, utm_campaign
)
	select s.visit_date::date as visit_date,  
	s.medium as utm_medium,
	count(distinct s.visitor_id) as visitors_count  
from sessions s
left join tab t
on s.source = t.utm_source
and s.medium = t.utm_medium
and s.campaign = t.utm_campaign
and s.visit_date = t.campaign_date
group by s.visit_date, s.medium
order by s.visit_date::date asc


with tab as (
    select
        s.visitor_id,
        l.lead_id,
        s.visit_date::date as visit_date,
        l.created_at::date as created_at
    from sessions as s
    left join leads as l
        on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
    where l.status_id = 142
),

tab2 as (
    select
        count(distinct lead_id) as cnt_lead,
        created_at - visit_date as hmdays
    from tab
    group by created_at - visit_date
),

tab3 as (
    select
        cnt_lead,
        sum(hmdays) over (partition by cnt_lead) as sumdays
    from tab2
)

select
    sumdays,
    sum(cnt_lead) as sumlead,
    sum(cnt_lead) / sumdays
from tab3
where sumdays > 0
group by sumdays

