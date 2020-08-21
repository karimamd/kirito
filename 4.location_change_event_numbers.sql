WITH session_transactions AS (
SELECT
fullvisitorid || visitId as sessionId,
FIRST_VALUE( h.transactionId )
  OVER (
    PARTITION BY fullvisitorid || visitId
    ORDER BY
      CASE WHEN h.transactionId IS NULL then 0 ELSE 1 END DESC
     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS transactionId,

ROW_NUMBER() OVER (PARTITION BY fullvisitorid || visitId ) AS row_n
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
,UNNEST (hit) AS h,
UNNEST (h.customDimensions) AS cd
where
transactionId is not null
)

,longitudes AS (
SELECT
fullvisitorid || visitId as sessionId,
FIRST_VALUE( case when safe_cast(cd.value as string) ='NA' or cast(cd.value as string) =''
or cd.value is null then 0 else safe_cast(cd.value as float64) end )
  OVER (
    PARTITION BY fullvisitorid || visitId
    ORDER BY
      CASE WHEN cd.value IS NULL then 0
           WHEN safe_cast(cd.value as string) ='NA' or safe_cast(cd.value as string) ='' or cd.value is null then 0
           ELSE 1  END DESC
     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS longitude,
ROW_NUMBER() OVER (PARTITION BY fullvisitorid || visitId ) AS row_n
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
,UNNEST (hit) AS h,
UNNEST (h.customDimensions) AS cd
WHERE
LOWER(h.eventAction) LIKE '%geolocation.requested%'
and cd.index = 18
),


latitudes AS (
SELECT
fullvisitorid || visitId as sessionId,
FIRST_VALUE( case when safe_cast(cd.value as string) ='NA' or safe_cast(cd.value as string) =''
or cd.value is null then 0 else safe_cast(cd.value as float64) end )
  OVER (
    PARTITION BY fullvisitorid || visitId
    ORDER BY
      CASE WHEN cd.value IS NULL then 0
           WHEN safe_cast(cd.value as string) ='NA' or safe_cast(cd.value as string) ='' or cd.value is null then 0
           ELSE 1  END DESC
     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS latitude,
ROW_NUMBER() OVER (PARTITION BY fullvisitorid || visitId ) AS row_n
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
,UNNEST (hit) AS h,
UNNEST (h.customDimensions) AS cd
WHERE
LOWER(h.eventAction) LIKE '%geolocation.requested%'
and cd.index = 19
),

screens AS (
SELECT
fullvisitorid || visitId as sessionId,
FIRST_VALUE(  cd.value )
  OVER (
    PARTITION BY fullvisitorid || visitId
    ORDER BY
      CASE WHEN cd.value IS NULL then 0
           WHEN  safe_cast(cd.value as string) ='' or cd.value is null then 0
           ELSE 1  END DESC
     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS screen_name,
ROW_NUMBER() OVER (PARTITION BY fullvisitorid || visitId ) AS row_n
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
,UNNEST (hit) AS h,
UNNEST (h.customDimensions) AS cd
WHERE
LOWER(h.eventAction) LIKE '%geolocation.requested%'
and cd.index = 11
),

earlier_inputs AS (
SELECT
  longitudes.sessionId,
  session_transactions.transactionId,
  longitudes.longitude,
  latitudes.latitude,
  screens.screen_name
FROM longitudes
INNER JOIN latitudes on longitudes.sessionId = latitudes.sessionId
INNER JOIN screens on screens.sessionId = latitudes.sessionId
INNER JOIN session_transactions ON longitudes.sessionId = session_transactions.sessionId
where
longitudes.row_n =1
and latitudes.row_n=1
and screens.row_n=1
and session_transactions.row_n=1
and session_transactions.transactionId is not null
),


backend_deduped AS (
SELECT *,
ROW_NUMBER() over (PARTITION BY frontendOrderId || status_id ORDER BY
      CASE WHEN geopointCustomer IS NULL then 0 ELSE 1 END DESC
      ) AS row_n
FROM `dhh-analytics-hiringspace.BackendDataSample.transactionalData`
),
backend_transactions AS(
SELECT
  ST_X(geopointCustomer) as longitude_back,
  ST_Y(geopointCustomer) as latitude_back ,
  ST_X(geopointDropoff) as longitude_delivery,
  ST_Y(geopointDropoff) as latitude_delivery ,
  *
FROM backend_deduped where row_n =1
and safe_cast(ST_X(geopointCustomer) as string) != 'NA' and  safe_cast(ST_X(geopointCustomer) as string) != ''
and  safe_cast(ST_X(geopointCustomer) as string) is not null
and safe_cast(ST_Y(geopointCustomer) as string) != 'NA' and  safe_cast(ST_Y(geopointCustomer) as string) != ''
and  safe_cast(ST_Y(geopointCustomer) as string) is not null
and safe_cast(ST_X(geopointDropoff) as string) != 'NA'
and  safe_cast(ST_X(geopointDropoff) as string) != ''
and  safe_cast(ST_X(geopointDropoff) as string) is not null
and safe_cast(ST_Y(geopointDropoff) as string) != 'NA'
and  safe_cast(ST_Y(geopointDropoff) as string) != ''
and  safe_cast(ST_Y(geopointDropoff) as string) is not null
ORDER BY frontendOrderId,status_id
)

,final as(
SELECT
abs(safe_cast (earlier_inputs.longitude as FLOAT64)
- safe_cast(backend_transactions.longitude_back as FLOAT64))
AS long_diff,
abs(safe_cast (earlier_inputs.latitude as FLOAT64)
- safe_cast(backend_transactions.latitude_back as FLOAT64))
AS lat_diff,
case when (abs(safe_cast (earlier_inputs.longitude as FLOAT64)
  - safe_cast(backend_transactions.longitude_back as FLOAT64)) >0 )
  then 1 else 0 end
as is_longitude_changed,
case when (abs(safe_cast (earlier_inputs.latitude as FLOAT64)
   - safe_cast(backend_transactions.latitude_back as FLOAT64)) >0 )
   then 1 else 0 end
as is_latitude_changed,

abs(safe_cast (backend_transactions.longitude_delivery as FLOAT64)
- safe_cast(backend_transactions.longitude_back as FLOAT64))
AS long_diff_delivery,
abs(safe_cast (backend_transactions.latitude_delivery as FLOAT64)
- safe_cast(backend_transactions.latitude_back as FLOAT64))
AS lat_diff_delivery,
case when (abs(safe_cast (backend_transactions.longitude_delivery as FLOAT64)
  - safe_cast(backend_transactions.longitude_back as FLOAT64)) >0.0003 )
  then 1 else 0 end
as is_delivery_longitude_changed,
case when (abs(safe_cast (backend_transactions.latitude_delivery  as FLOAT64)
   - safe_cast(backend_transactions.latitude_back as FLOAT64)) >0.0003 )
   then 1 else 0 end
as is_delivery_latitude_changed,

earlier_inputs.screen_name as locScreenName,
case when backend_transactions.geopointCustomer is not null then 1 else 0 end as order_is_made,
case when backend_transactions.geopointDropoff is not null then 1 else 0 end as order_is_delivered,
*
FROM earlier_inputs
INNER JOIN backend_transactions
ON earlier_inputs.transactionId = backend_transactions.frontendOrderId
where
earlier_inputs.longitude is not null
and backend_transactions.longitude_back is not null
and earlier_inputs.latitude is not null
and earlier_inputs.screen_name is not null
and backend_transactions.latitude_back is not null
and backend_transactions.longitude_delivery is not null
and backend_transactions.latitude_delivery is not null
)

SELECT
final.locScreenName,
AVG(long_diff) AS longitude_average_diff,
AVG(lat_diff) AS latitude_average_diff,
SUM(is_longitude_changed) AS total_longitude_changes,
SUM(is_latitude_changed) AS total_latitude_changes,
SUM(CASE WHEN (is_longitude_changed=1 or is_latitude_changed=1 )then 1 else 0 end) AS total_location_changes,
SUM( case when order_is_made= 1 and (is_longitude_changed=1 or is_latitude_changed=1 )then 1 else 0 end ) AS total_orders_made,
SUM(case when order_is_delivered=1  and (is_longitude_changed=1 or is_latitude_changed=1 ) then 1 else 0 end) AS total_orders_delivered,

SUM(case when order_is_delivered=1
  and (is_longitude_changed=1 or is_latitude_changed=1 )
  and (is_delivery_longitude_changed=1 or is_delivery_latitude_changed=1 )
  then 1 else 0 end) AS total_delivery_unmatched_coordinates,
AVG(long_diff_delivery) AS delivery_longitude_average_diff,
AVG(lat_diff_delivery) AS delivery_latitude_average_diff,

FROM
final
GROUP BY 1
ORDER BY 1,total_latitude_changes DESC,total_longitude_changes DESC
;
