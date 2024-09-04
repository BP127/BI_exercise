WITH weekly_metrics AS (
    SELECT 
        DATE_TRUNC('week', month) AS week_start,
        SUM(initials) AS initial_engagements,
        SUM(completes) AS completed_engagements,
        SUM(clicks) AS total_clicks,
        SUM(interactions) AS total_interactions,
        SUM(video_started) AS video_started,
        SUM(video_completed) AS video_completed,
        SUM(time_spent) AS total_time_spent,
        SUM(time_spent_count) AS total_time_spent_count,
        SUM(bots) AS bot_traffic
    FROM 
        {{ ref('stg_ad_analytics_transformed') }}
    GROUP BY 
        DATE_TRUNC('week', month)
)

SELECT
    *
FROM 
    weekly_metrics
ORDER BY 
    week_start;
