# Big Query Task

### 1- How many sessions are there?  
211,904 sessions

Query used:

```
SELECT
COUNT( DISTINCT visitId  ||  fullvisitorid )
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`;
```

•	According to the [docs](https://support.google.com/analytics/answer/3437719?hl=en) combining visitId  and fullvisitorid should get a unique ID , however if Distinct is removed then there can be multiple rows with this combination (max 2) and they are not duplicates

•	The extra records are for sessions that moved from one day to another (started before 12 am and ended after it) and they are completion of the old sessions and should be merged into those rows

•	[Visualization for both questions 1 and 2](https://public.tableau.com/profile/kareem.abdelsalam1054#!/vizhome/Bigquery/SessionsandUsersovertime)

### 2- How many sessions does each visitor create?

*	To get number of sessions created IN THE DATASET for each user by fullvisitorid :

```
SELECT
COUNT(DISTINCT visitId),
fullvisitorid
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
GROUP BY 2
ORDER BY 1 DESC;
```

* To get number of sessions created in general by the user

```
SELECT
MAX(visitNumber),
fullvisitorid
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
GROUP BY 2
ORDER BY 1 DESC;
```

*	To switch between numbers in the dataset vs numbers in general we switch COUNT with MAX(visitNumber) in all the following queries
*	To get average of the number of sessions : 2.127 = 2 sessions on average
* [Visualization for average number of sessions over time](https://public.tableau.com/profile/kareem.abdelsalam1054#!/vizhome/Bigquery/AverageSessionsperday)
* 1 is min number of sessions and 80 is max

Query:
```
WITH sessions AS (
SELECT
COUNT(DISTINCT visitId) AS cnt,
fullvisitorid
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
GROUP BY 2 ORDER BY 1 DESC
)
SELECT AVG(cnt), min(cnt), max(cnt) FROM sessions;
```

*	percentiles, 1st quantile and median are ( 1 session )
*	third quantile (75%) is 2 sessions

Query:
```
WITH sessions AS (
SELECT
COUNT(DISTINCT visitId) AS cnt, fullvisitorid
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
GROUP BY 2 ORDER BY 1 DESC
),
quantiles as (
SELECT approx_quantiles(cnt,100) percentiles from sessions
)
SELECT
percentiles[offset(25)] as p25,percentiles[offset(50)] as median,
percentiles[offset(75)] as p75,
percentiles[offset(100)] as max
FROM quantiles;
```

### 3. How much time does it take on average to reach the order_confirmation screen per session (in minutes)?

17.14 Minutes

[Visualization](https://public.tableau.com/profile/kareem.abdelsalam1054#!/vizhome/Bigquery/Averageorder_confirmationscreenreachtime)

Query:

```

WITH first_time_confirmation_in_session AS
(
SELECT
  FIRST_VALUE(time) OVER (PARTITION BY fullvisitorid || visitId
  ORDER BY date,time) AS confirmation_reach_time,
  ROW_NUMBER() OVER (PARTITION BY fullvisitorid || visitId ORDER BY date) AS row_n
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
  ,UNNEST (hit) AS h,
  UNNEST (h.customDimensions) AS cd
WHERE
  cd.value = 'order_confirmation'
  AND cd.index = 11
)
SELECT AVG(confirmation_reach_time/(60*1000)) FROM first_time_confirmation_in_session
WHERE
  row_n =1
  AND confirmation_reach_time !=0;
```
* this query’s average calculation can possibly have a reduced average than the real one, because of those sessions split on two rows would have time start at 0 from the second row , so this would be problematic for sessions where order_confirmation is reached in second screen

### 4.1. By using the ​GoogleAnalyticsSample​ data and BackendDataSample tables, analyse how often users tend to change their location in the beginning of their journey (screens like home and listing) versus in checkout and on order placement

Users tend to change their location in the earlier screens like shop_list screen (85k times), then home screen (75k times)
then the next biggest number is in later screens like checkout (68k times)
and rest of screens have negligible numbers compared to those for geolocation.requested event

This event was chosen out of all location events because it was the only event having Latitude and Longitude data in each event firing so that we could compare the change
other events like ‘other_location.clicked’ and ‘Change Location’ have very little data for screens and no data for coordinates (and even so they confirm the conclusion of having it changed in earlier screens)

Queries used:

```
SELECT
landingScreenName,
h.eventAction AS eventName,
count(*)
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
,UNNEST (hit) AS h,
UNNEST (h.customDimensions) AS cd
WHERE
LOWER(h.eventAction) LIKE '%location%'
group by 1,2
order by 2 asc ,3 desc;
```

```
SELECT
CASE WHEN cd.index = 11 THEN  cd.value ELSE 'NOT SCREEN' END AS screen ,
CASE WHEN cd.index = 11 THEN 'SCREEN TYPE' WHEN cd.index = 18 THEN 'LONGITUDE'
  WHEN cd.index = 19 THEN 'LATITUDE' ELSE 'OTHER' END,
h.eventAction AS eventName,
count(*)
FROM
`dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
,UNNEST (hit) AS h,
UNNEST (h.customDimensions) AS cd
WHERE
LOWER(h.eventAction) LIKE '%location%'
and ( cd.index = 11 OR cd.index = 18 OR cd.index = 19)
group by 1,2,3
order by 3 asc ,4 desc;
```

### 4.2. and demonstrate the the deviation between earlier and later inputs (if any) in terms of coordinates change.
In terms of actual coordinates changes  I have the exact opposite conclusion, checkout screen has 4552 location changes, then followed by shop_list and home screens having their combined numbers less than checkout location changes.

Which shows that the users only attempt to change their locations in earlier screens but more actual changes to location happen at checkout !


[Visualization](https://public.tableau.com/profile/kareem.abdelsalam1054#!/vizhome/Bigquery/Locationchangeseffectonscreens)

### 4.3. Then, using the ​BackendDataSample​ table, see if those customers who changed their address ended placing orders and if those orders were delivered successfully, if so, did they match their destination.

Yes all customers who changed their locations ended up placing orders and all of them were delivered.

None of the delivery coordinates did exactly match the customer coordinates which makes sense
But to measure if they matched or not we must put a sensible margin of error.

One latitude degree equals about 69 miles which equals about 111 kilometers, so if we make our margin of error equals 0.0003 latitude and longitude degrees in difference from the customers’ set location so we allow 300 square meters difference then almost half of all deliveries match their destination
And if we allowed 500 square meters then only about 30% of deliveries do not match while the remaining 70% matches

Finally if the threshold was decreased to 100 meters then most of the deliveries will not match destinations set by users (about 90%)

So the answer to this question depends on the acceptable margin of error in coordinates.

Query used:
```
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
ORDER BY 1,total_latitude_changes DESC,total_longitude_changes DESC;
```

[Success Funnel Visual for Checkout screen](https://public.tableau.com/profile/kareem.abdelsalam1054#!/vizhome/Bigquery/CheckoutScreenSuccessFunnel)

[Success Funnel Visual for shop_list screen](https://public.tableau.com/profile/kareem.abdelsalam1054#!/vizhome/Bigquery/ListingScreenSuccessFunnel)

[Success Funnel Visual for Home screen](https://public.tableau.com/profile/kareem.abdelsalam1054#!/vizhome/Bigquery/HomeScreenSuccessFunnel)

# Part 2 - Python Questions

All questions are answered in the iPython notebook attached (DHH.ipynb)

To view and run directly in Colab: [link](https://colab.research.google.com/drive/1UaHRsmrNRNTj_Wp50Uwghtq6xYXIX3tk?usp=sharing)

[Extra Tableau Visualization](https://public.tableau.com/profile/kareem.abdelsalam1054#!/vizhome/BTC-stock/Metrics?publish=yes)
