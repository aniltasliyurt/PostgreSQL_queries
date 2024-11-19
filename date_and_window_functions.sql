with facebook_data as (
select
	ad_date,
	date_trunc('month',
	ad_date)::date as ad_month,
	nullif(LOWER(substring(url_parameters from 'utm_campaign=([^&]+)')),
	'nan') as utm_campaign,
	fabd.campaign_id,
	fabd.adset_id,
	coalesce(fabd.spend,
	0) as spend,
	coalesce(fabd.impressions,
	0) as impressions,
	coalesce(fabd.reach,
	0) as reach,
	coalesce(fabd.clicks,
	0) as clicks,
	coalesce(fabd.leads,
	0) as leads,
	coalesce(fabd.value,
	0) as value,
	fa.adset_name,
	fc.campaign_name
from
	facebook_ads_basic_daily fabd
inner join facebook_adset fa on
	fa.adset_id = fabd.adset_id
inner join facebook_campaign fc on
	fc.campaign_id = fabd.campaign_id
),
google_data as (
select
	ad_date,
	date_trunc('month',
	ad_date)::date as ad_month,
	nullif(LOWER(substring(url_parameters from 'utm_campaign=([^&]+)')),
	'nan') as utm_campaign,
	null as campaign_id,
	null as adset_id,
	coalesce(gabd.spend,
	0) as spend,
	coalesce(gabd.impressions,
	0) as impressions,
	coalesce(gabd.reach,
	0) as reach,
	coalesce(gabd.clicks,
	0) as clicks,
	coalesce(gabd.leads,
	0) as leads,
	coalesce(gabd.value,
	0) as value,
	gabd.adset_name,
	gabd.campaign_name
from
	google_ads_basic_daily gabd
),
main_data as (
select
	*
from
	facebook_data
union all
select
	*
from
	google_data
),
monthly_aggregated_CTE as (
select
	ad_month,
	utm_campaign,
	SUM(spend) as total_spend,
	SUM(impressions) as total_impressions,
	SUM(clicks) as total_clicks,
	SUM(value) as total_value,
	case
		when SUM(clicks) = 0 then NULL
			else ROUND((SUM(spend)::numeric / nullif(SUM(clicks),
			0))::numeric,
			2)
		end as CPC,
		case
			when SUM(impressions) = 0 then null
			else ROUND((SUM(spend)::numeric / nullif(SUM(impressions),
			0))* 1000 ::numeric,
			2)
		end as CPM,
		case
			when SUM(impressions) = 0 then null
			else ROUND((SUM(clicks)::numeric / nullif(SUM(impressions),
			0)) * 100 ::numeric,
			2)
		end as CTR,
		case
			when SUM(spend) = 0 then null
			else ROUND(((SUM(value)::numeric - SUM(spend)::numeric) / nullif(SUM(spend),
			0)) * 100 :: numeric,
			2)
		end as ROMI
	from
		main_data
	group by
		ad_month,
		utm_campaign
)
select
	ad_month,
	utm_campaign,
	total_spend,
	total_impressions,
	total_clicks,
	total_value,
	CPC,
	CPM,
	CTR,
	ROMI,
	case 
		when CPM is null then null
		when lag(CPM,
		1) over (partition by utm_campaign
	order by
		ad_month) = 0 then null
		else (CPM - lag(CPM,
		1) over(partition by utm_campaign
	order by
		ad_month) / nullif(lag(CPM,
		1)over (partition by utm_campaign
	order by
		ad_month),
		0))* 1000
	end as cpm_percentage_change,
	case 
		when CTR is null then null
		when lag(CTR,
		1) over (partition by utm_campaign
	order by
		ad_month) = 0 then null
		else (CTR - lag(CTR,
		1) over(partition by utm_campaign
	order by
		ad_month) / nullif(lag(CTR,
		1)over (partition by utm_campaign
	order by
		ad_month),
		0))* 100
	end as ctr_percentage_change,
	case 
		when ROMI is null then null
		when lag(ROMI,
		1) over (partition by utm_campaign
	order by
		ad_month) = 0 then null
		else (ROMI - lag(ROMI,
		1) over(partition by utm_campaign
	order by
		ad_month) / nullif(lag(ROMI,
		1)over (partition by utm_campaign
	order by
		ad_month),
		0))* 100
	end as romi_percentage_change
from
	monthly_aggregated_CTE
order by
	ad_month,
	utm_campaign;
