{#
  Inverse UTM projection (northern hemisphere) on the GRS80 ellipsoid,
  Krüger series to n^3 (sub-millimetre inside a zone). Returns SQL
  expressions {'lon': ..., 'lat': ...} in degrees. ETRS89 is treated as
  equal to WGS84 (they differ by well under a metre).

  Stand-in for st_transform(..., 4326), which Trino only has from a newer
  version than the cluster's (481).
#}
{% macro utm_to_wgs84(easting, northing, zone) %}
    {%- set a = 6378137.0 -%}
    {%- set f = 1 / 298.257222101 -%}
    {%- set n = f / (2 - f) -%}
    {%- set k0_a = 0.9996 * a / (1 + n) * (1 + n ** 2 / 4 + n ** 4 / 64) -%}
    {%- set beta = [
        n / 2 - 2 * n ** 2 / 3 + 37 * n ** 3 / 96,
        n ** 2 / 48 + n ** 3 / 15,
        17 * n ** 3 / 480,
    ] -%}
    {%- set delta = [
        2 * n - 2 * n ** 2 / 3 - 2 * n ** 3,
        7 * n ** 2 / 3 - 8 * n ** 3 / 5,
        56 * n ** 3 / 15,
    ] -%}
    {%- set lon0 = zone * 6 - 183 -%}

    {%- set xi = "((" ~ northing ~ ") / " ~ k0_a ~ ")" -%}
    {%- set eta = "((" ~ easting ~ " - 500000) / " ~ k0_a ~ ")" -%}

    {%- set xi_p = ["(" ~ xi] -%}
    {%- set eta_p = ["(" ~ eta] -%}
    {%- for b in beta -%}
        {%- set j2 = 2 * loop.index -%}
        {%- do xi_p.append(" - " ~ b ~ " * sin(" ~ j2 ~ " * " ~ xi ~ ") * cosh(" ~ j2 ~ " * " ~ eta ~ ")") -%}
        {%- do eta_p.append(" - " ~ b ~ " * cos(" ~ j2 ~ " * " ~ xi ~ ") * sinh(" ~ j2 ~ " * " ~ eta ~ ")") -%}
    {%- endfor -%}
    {%- set xi_p = xi_p | join("") ~ ")" -%}
    {%- set eta_p = eta_p | join("") ~ ")" -%}

    {%- set chi = "asin(sin(" ~ xi_p ~ ") / cosh(" ~ eta_p ~ "))" -%}
    {%- set lat = ["(" ~ chi] -%}
    {%- for d in delta -%}
        {%- do lat.append(" + " ~ d ~ " * sin(" ~ 2 * loop.index ~ " * " ~ chi ~ ")") -%}
    {%- endfor -%}

    {{ return({
        "lon": lon0 ~ " + degrees(atan(sinh(" ~ eta_p ~ ") / cos(" ~ xi_p ~ ")))",
        "lat": "degrees(" ~ lat | join("") ~ "))",
    }) }}
{% endmacro %}
