-- OSM postcode areas, one row per OSM relation and release.
with source as (

    select * from {{ source('osm', 'postleitzahlen') }}

)

select
    postcode as plz,
    rel as osm_relation_id,
    release,
    srid,
    wkb_geometry_repaired is not null as is_geometry_repaired,
    st_setsrid(
        st_geomfrombinary(coalesce(wkb_geometry_repaired, wkb_geometry)),
        srid
    ) as geometry
from source
