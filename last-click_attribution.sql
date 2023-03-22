#抓取landing Pageview 的 utm 跟 purchase mapping

select 
  purchase.transaction_id, 
  purchase.user_id, 
  purchase.timestamp, 
  purchase.device_category, 
  purchase.tier_type,  
  purchase.bd,
  concat (SPLIT(purchase.transaction_id, ' ')[OFFSET(0)]) as pnr_number , 
  source.source, 
  source.campaign, 
  source.content
from (
  select 
    ecommerce.transaction_id, 
    user_id, 
    user_pseudo_id, 
    timestamp_micros(event_timestamp) as timestamp, 
    device.category as device_category, 
    (select value.string_value from unnest(user_properties) where key = 'tier_type') as tier_type, 
    (select value.string_value from unnest(user_properties) where key = 'bd') as bd, 
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id, 
  from 
    `ca-gstobq.analytics_279288529.events_*`
  where
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
    and event_name = 'purchase'
    and user_id is not null
    and SPLIT(ecommerce.transaction_id, ' ')[OFFSET(0)] != 'undefined'
    and SPLIT(ecommerce.transaction_id, ' ')[OFFSET(3)] != 'undefined'
) purchase
inner join (
  select *
  from (
    select 
      user_pseudo_id,  
      (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id, 
      (select value.string_value from unnest(event_params) where key = 'source') as source, 
      (select value.string_value from unnest(event_params) where key = 'campaign') as campaign, 
      (select value.string_value from unnest(event_params) where key = 'content') as content, 
    from 
      `ca-gstobq.analytics_279288529.events_*`
    where
      _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      and event_name = 'page_view'
  )
  where source in ('EDM', 'SMS', 'APP')
  group by user_pseudo_id, session_id, source, campaign, content
) source
on purchase.user_pseudo_id = source.user_pseudo_id and purchase.session_id = source.session_id