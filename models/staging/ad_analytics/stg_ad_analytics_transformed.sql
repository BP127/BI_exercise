-- there are lot of rows where publisher_name, advertiser_name is null so need to remove this that doesn't help to analysis

with stg_raw_data as 
(   select 
        *  
    from {{ ref('stg_ad_analytics__raw_data') }}
    where 
    publisher_name is not null
    and advertiser_name is not null
),

-- there are missing values in base metric columns replace null with 0 and publisher and advertiser shouldn't be null
deduped_data as 
(
    select 
        *
    from stg_raw_data

    -- there are duplicate records in data requires dedupe these
    qualify row_number() over( partition by 
                        month,
                        publisher_name,
                        publisher_type,
                        publisher_vertical,
                        device_group,
                        parent_account_name,
                        account_name,
                        advertiser_industry,
                        advertiser_name,
                        campaign_name,
                        initials,
                        completes,
                        clicks,
                        interactions,
                        video_started,
                        video_completed,
                        time_spent,
                        time_spent_count,
                        bots
                        order by random(1)
                ) = 1

),

--missing categorical values need to replace with Unknown and numerical values with 0 as it will be aggregated later 
-- column names required to standardise
fixed_data as (
select 
    coalesce(month, 'Unknown') as month,
    coalesce(publisher_name, 'Unknown') as publisher_name,
    coalesce(publisher_type, 'Unknown') as publisher_type,
    coalesce(publisher_vertical, 'Unknown') as publisher_vertical,
    coalesce(device_group, 'Unknown') as device_group,
    coalesce(parent_account_name, 'Unknown') as parent_account_name,
    coalesce(account_name, 'Unknown') as account_name,
    coalesce(advertiser_industry, 'Unknown') as advertiser_industry,
    coalesce(advertiser_name, 'Unknown') as advertiser_name,
    coalesce(campaign_name, 'Unknown') as campaign_name,
    coalesce(initials, 0) as initials,
    coalesce(completes, 0) as completes,
    coalesce(clicks, 0) as clicks,
    coalesce(interactions, 0) as interactions,
    coalesce(video_started, 0) as video_started,
    coalesce(video_completed, 0) as video_completed,
    coalesce(time_spent, 0) as time_spent,
    coalesce(time_spent_count, 0) as time_spent_count,
    coalesce(bots, 0) as bots
 from deduped_data
)

select * from fixed_data