WITH facebook_data AS (
    SELECT
        ad_date,
        nullif(LOWER(substring(url_parameters from 'utm_campaign=([^&]+)')), 'nan') AS utm_campaign,
        fabd.campaign_id,
        fabd.adset_id,
        COALESCE(fabd.spend, 0) AS spend,
        COALESCE(fabd.impressions, 0) AS impressions,
        COALESCE(fabd.reach, 0) AS reach,
        COALESCE(fabd.clicks, 0) AS clicks,
        COALESCE(fabd.leads, 0) AS leads,
        COALESCE(fabd.value, 0) AS value,
        fa.adset_name,
        fc.campaign_name
    FROM facebook_ads_basic_daily fabd
    INNER JOIN facebook_adset fa ON fa.adset_id = fabd.adset_id
    INNER JOIN facebook_campaign fc ON fc.campaign_id = fabd.campaign_id
),
google_data AS (
    SELECT
        ad_date,
        nullif(LOWER(substring(url_parameters from 'utm_campaign=([^&]+)')), 'nan') AS utm_campaign,
        NULL AS campaign_id,
        NULL AS adset_id,
        COALESCE(gabd.spend, 0) AS spend,
        COALESCE(gabd.impressions, 0) AS impressions,
        COALESCE(gabd.reach, 0) AS reach,
        COALESCE(gabd.clicks, 0) AS clicks,
        COALESCE(gabd.leads, 0) AS leads,
        COALESCE(gabd.value, 0) AS value,
        gabd.adset_name,
        gabd.campaign_name
    FROM google_ads_basic_daily gabd
),
main_data AS (
    SELECT * FROM facebook_data
    UNION ALL
    SELECT * FROM google_data
)
SELECT
	ad_date,
    campaign_name,
	utm_campaign,
	SUM(spend) as total_spend,
	SUM(impressions) as total_impressions,
	SUM(clicks) as total_clicks,
	SUM(value) as total_value,
	case
		when SUM(clicks) = 0
			or SUM(spend) = 0 then null
			else ROUND((SUM(spend)::numeric / nullif(SUM(clicks),
			0))::numeric,
			2)
		end as CPC,
		case
			when SUM(impressions) = 0 then null
			else ROUND((SUM(spend)::numeric / nullif(SUM(impressions),
			0))*1000 ::numeric,
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
FROM main_data
GROUP BY ad_date, campaign_name, utm_campaign;



