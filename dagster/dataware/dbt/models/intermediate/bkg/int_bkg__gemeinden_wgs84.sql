-- Land areas (Geofaktor 4) of the Gemeinden of the latest VG250 stichtag,
-- converted to WGS84 so they can be intersected with the OSM postcode
-- areas. A table, as the conversion of ~2 million vertices takes about a
-- minute; geometries are stored as WKB (Iceberg has no geometry type).
{{ config(materialized='table') }}

with gemeinden as (

    select * from {{ ref('stg_bkg__vg250_gemeinden') }}
    where geofaktor = 4

),

latest as (

    select *
    from gemeinden
    where stichtag = (select max(stichtag) from gemeinden)

)

select
    ags,
    gemeinde_name,
    stichtag,
    st_asbinary({{ utm_geometry_to_wgs84('geometry', 32) }}) as geometry_wkb
from latest
