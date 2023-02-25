{{ config(materialized='table') }}

with fhv_data as (
    select *,
        'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
)
select
    fhv_data.tripid,
    fhv_data.service_type,
    fhv_data.pickup_locationid,
    fhv_data.dropoff_locationid,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid
