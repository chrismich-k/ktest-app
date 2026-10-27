-- Combinations of postcode area and Gemeinde with their shared area and its
-- centroid in WGS84. Overlaps below plz_gemeinde_min_share of both the
-- postcode area and the Gemeinde are slivers from OSM and BKG boundaries
-- not lining up and are left out. The centroid is computed on lon/lat
-- degrees, which is off by only a few metres at this size.
with intersections as (

    select
        *,
        st_geomfrombinary(intersection_wkb) as intersection
    from {{ ref('int_plz_gemeinde_intersections') }}
    where greatest(share_of_plz, share_of_gemeinde)
        >= {{ var('plz_gemeinde_min_share') }}

),

centroids as (

    select
        *,
        st_centroid(intersection) as centroid
    from intersections

)

select
    {{ dbt_utils.generate_surrogate_key(['plz', 'ags']) }}
        as plz_gemeinde_key,
    plz,
    ags,
    gemeinde_name,
    share_of_plz,
    share_of_gemeinde,
    st_area(to_spherical_geography(intersection)) / 1e6 as area_km2,
    st_x(centroid) as centroid_lon,
    st_y(centroid) as centroid_lat,
    -- false for e.g. U-shaped or multi-part areas
    st_contains(intersection, centroid) as centroid_within_area,
    release,
    stichtag
from centroids
