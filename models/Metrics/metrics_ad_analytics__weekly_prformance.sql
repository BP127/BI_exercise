with weekly_metrics
as (
	select 
         date_trunc('week', month) as week_start
		,sum(initials) as initial_engagements
		,sum(completes) as completed_engagements
		,sum(clicks) as total_clicks
		,sum(interactions) as total_interactions
		,sum(video_started) as video_starts
		,sum(video_completed) as video_completes
		,sum(time_spent) as total_time_spent
		,sum(time_spent_count) as total_time_spent_count
		,sum(bots) as bot_traffic
	from {{ ref('stg_ad_analytics_transformed') }}
	group by date_trunc('week', month)
	),

weekly_perfomance_metrics as (  
select 
     week_start,
     lag(week_start) over( order by week_start) as previous_week,
     datediff('day', previous_week, week_start) as days_diff
	,initial_engagements
	,completed_engagements
	,total_clicks
	,total_interactions
	,video_starts
	,video_completes
	,total_time_spent
	,bot_traffic
	,round(completed_engagements / initial_engagements, 2) as completion_rate
	,round(total_clicks / completed_engagements, 2) as click_thru_rate
	,round(total_interactions / initial_engagements, 2) as interaction_rate
	,round(video_completes / video_starts, 2) as video_completion_rate
	,round(total_time_spent / total_time_spent_count, 2) as average_time_spent_in_seconds
from weekly_metrics
order by week_start
)


-- days_diff for each row is more than 7 so week over week percent change will always be 0 but can be compared with previous available week 

,weekly_report as
(
select 
    *,
        (abs(completion_rate - lag(completion_rate) over(order by week_start)) / (lag(completion_rate) over(order by week_start))) * 100 as percent_change_in_completion_rate,
        (abs(click_thru_rate - lag(click_thru_rate) over(order by week_start)) / (lag(click_thru_rate) over(order by week_start))) * 100 as percent_change_in_click_thru_rate,
        (abs(interaction_rate - lag(interaction_rate) over(order by week_start)) / (lag(interaction_rate) over(order by week_start))) * 100 as percent_change_in_interaction_rate,
        (abs(video_completion_rate - lag(video_completion_rate) over(order by week_start)) / (lag(video_completion_rate) over(order by week_start))) * 100 as percent_change_in_video_completion_rate,
        (abs(average_time_spent_in_seconds - lag(average_time_spent_in_seconds) over(order by week_start)) / (lag(average_time_spent_in_seconds) over(order by week_start))) * 100 as percent_change_in_average_time_spent_in_seconds
from weekly_perfomance_metrics
)

select 
    week_start
    ,coalesce(previous_week, week_start) as previous_week
    ,coalesce(days_diff, 0) as days_diff
	,initial_engagements
	,completed_engagements
	,total_clicks
	,total_interactions
	,video_starts
	,video_completes
	,total_time_spent
	,bot_traffic
	,completion_rate
	,click_thru_rate
	,interaction_rate
	,video_completion_rate
	,average_time_spent_in_seconds
    ,coalesce(round(percent_change_in_completion_rate, 2), 0) as percent_change_in_completion_rate
    ,coalesce(round(percent_change_in_click_thru_rate,2), 0) as percent_change_in_click_thru_rate
    ,coalesce(round(percent_change_in_interaction_rate,2), 0) as percent_change_in_interaction_rate
    ,coalesce(round(percent_change_in_video_completion_rate,2), 0) as percent_change_in_video_completion_rate
    ,coalesce(round(percent_change_in_average_time_spent_in_seconds,2), 0) as percent_change_in_average_time_spent_in_seconds
from weekly_report
order by week_start
