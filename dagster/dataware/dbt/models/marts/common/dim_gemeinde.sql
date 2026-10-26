-- Gemeinden of the latest VG250 stichtag with the centroid of their land
-- area (Geofaktor 4; water parts like coastal waters are left out) in WGS84.
-- The centroid is computed in the projected source CRS (UTM zone 32N) and
-- only the resulting point is converted; a centroid computed on lon/lat
-- degrees would be distorted.
{% set centroid_wgs84 = utm_to_wgs84('easting', 'northing', 32) %}

with gemeinden as (

    select * from {{ ref('stg_bkg__vg250_gemeinden') }}
    where geofaktor = 4

),

latest as (

    select *
    from gemeinden
    where stichtag = (select max(stichtag) from gemeinden)

),

centroids as (

    select
        *,
        st_x(st_centroid(geometry)) as easting,
        st_y(st_centroid(geometry)) as northing
    from latest

)

select
    {{ dbt_utils.generate_surrogate_key(['ags']) }} as gemeinde_key,
    ags,
    ars,
    gemeinde_name,
    gemeinde_bezeichnung,
    stichtag,
    {{ centroid_wgs84.lon }} as centroid_lon,
    {{ centroid_wgs84.lat }} as centroid_lat
from centroids
