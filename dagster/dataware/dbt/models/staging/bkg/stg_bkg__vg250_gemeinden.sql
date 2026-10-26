-- Gemeinde boundaries of BKG VG250, one row per Gemeinde, Geofaktor and
-- stichtag.
with source as (

    select * from {{ source('bkg', 'vg250_gemeinden') }}

)

select
    -- Amtlicher Gemeindeschlüssel, 8 digits; matches MaStR's
    -- Gemeindeschluessel
    ags,
    -- Amtlicher Regionalschlüssel, 12 digits
    ars,
    gen as gemeinde_name,
    bez as gemeinde_bezeichnung,
    -- 4 = land, 2 = water part (e.g. coastal waters, Bodensee)
    gf as geofaktor,
    cast(stichtag as date) as stichtag,
    srid,
    -- WKB carries no SRID, so it is set from the source's srid column
    st_setsrid(st_geomfrombinary(geom), srid) as geometry
from source
