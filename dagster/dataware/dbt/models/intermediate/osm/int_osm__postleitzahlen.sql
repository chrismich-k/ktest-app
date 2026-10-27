-- Postcode areas of the latest OSM release, one row per postcode; a
-- postcode made of several relations is merged into one geometry.
-- Geometries are stored as WKB (Iceberg has no geometry type).
{{ config(materialized='table') }}

with postleitzahlen as (

    select * from {{ ref('stg_osm__postleitzahlen') }}

),

latest as (

    select *
    from postleitzahlen
    where release = (select max(release) from postleitzahlen)

)

select
    plz,
    release,
    count(*) as osm_relation_count,
    bool_or(is_geometry_repaired) as is_geometry_repaired,
    st_asbinary(geometry_union_agg(geometry)) as geometry_wkb
from latest
group by plz, release
