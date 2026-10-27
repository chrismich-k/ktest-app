-- Every overlap of a postcode area and a Gemeinde, including the thin
-- slivers caused by OSM and BKG boundaries not lining up exactly; those are
-- filtered out in dim_plz_gemeinde. A table, as the spatial join takes
-- several minutes. Areas are in square degrees, so only their ratios are
-- meaningful.
{{ config(materialized='table') }}

with postleitzahlen as (

    select
        plz,
        release,
        st_geomfrombinary(geometry_wkb) as geometry
    from {{ ref('int_osm__postleitzahlen') }}

),

gemeinden as (

    select
        ags,
        gemeinde_name,
        stichtag,
        st_geomfrombinary(geometry_wkb) as geometry
    from {{ ref('int_bkg__gemeinden_wgs84') }}

),

overlaps as (

    select
        postleitzahlen.plz,
        gemeinden.ags,
        gemeinden.gemeinde_name,
        postleitzahlen.release,
        gemeinden.stichtag,
        st_intersection(postleitzahlen.geometry, gemeinden.geometry)
            as raw_intersection,
        st_area(postleitzahlen.geometry) as plz_area,
        st_area(gemeinden.geometry) as gemeinde_area
    from postleitzahlen
    inner join gemeinden
        on st_intersects(postleitzahlen.geometry, gemeinden.geometry)

),

polygonal as (

    -- a few intersections are collections with line or point leftovers
    -- along the boundary; only their areas are kept
    select
        *,
        case
            when st_geometrytype(raw_intersection) = 'ST_GeomCollection'
                then geometry_union(filter(
                    st_geometries(raw_intersection),
                    part -> st_geometrytype(part)
                        in ('ST_Polygon', 'ST_MultiPolygon')
                ))
            else raw_intersection
        end as intersection
    from overlaps

)

select
    plz,
    ags,
    gemeinde_name,
    release,
    stichtag,
    st_area(intersection) / plz_area as share_of_plz,
    st_area(intersection) / gemeinde_area as share_of_gemeinde,
    st_asbinary(intersection) as intersection_wkb
from polygonal
-- areas that only touch along their boundary
where st_area(intersection) > 0
