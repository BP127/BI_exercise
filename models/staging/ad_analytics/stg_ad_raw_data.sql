with ad_raw_data as 
(select * from {{ source('ad_analytics', 'raw_ad_data') }})

select * from ad_raw_data