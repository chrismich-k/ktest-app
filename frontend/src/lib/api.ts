// Client for the ktest API (api/), served under /api.

export type Grain = "hour" | "day" | "week" | "month"
export type DayBasis = "gas_day" | "calendar_date"

export type CostFilters = {
  grain: Grain
  start: string // yyyy-mm-dd, inclusive
  end: string // yyyy-mm-dd, inclusive
  dayBasis: DayBasis
  includeLto: boolean
  gasQuality: string[] // empty: all
}

export type Meta = {
  stack_labels: { stack_label: string; source: string }[]
  gas_qualities: string[]
  location_types: string[]
  first_gas_day: string
  last_gas_day: string
  last_balancing_gas_day: string
}

export type Series = {
  key: string
  stack_label: string
  stack_side: "buy" | "sell"
}

export type Bucket = {
  bucket: string
  // signed amounts per series key (> 0 cost, < 0 revenue)
  values: Record<string, number>
  net: number
  // delivered quantities (MWh) per series key, signed like the series;
  // balancing actions only
  quantities: Record<string, number>
  // value of those quantities at the reference price (VTP daily average
  // price), signed the same way; only hours with a reference price
  reference_values: Record<string, number>
  // excess cost over the reference: amount - reference value, > 0
  // unfavourable (bought above / sold below the reference)
  excess: Record<string, number>
  // false if some hours have no reference price yet (current gas day)
  reference_complete: boolean
}

export type Timeseries = {
  grain: Grain
  series: Series[]
  buckets: Bucket[]
}

export type LocationKind = "storage" | "zone" | "unknown"

// prices of balancing actions in EUR/MWh; null for LTO capacity prices
export type Prices = {
  quantity_mwh: number | null
  price_eur_per_mwh: number | null
  // VTP daily average price of the hours' gas days, volume-weighted
  reference_price_eur_per_mwh: number | null
  // price / reference - 1, over the hours that have a reference price
  price_deviation: number | null
  // false if the current gas day has no reference price yet
  reference_complete: boolean | null
}

export type DirectionSlice = Prices & {
  direction: string
  amount_eur: number
}

export type Slice = Prices & {
  direction: string
  source: string
  product_label: string
  location: string
  // how it is traded: "Exchange" | "OTC", null for tender-only locations
  location_type: string | null
  exchange: string | null
  // what it is
  location_kind: LocationKind
  amount_eur: number
}

export type Breakdown = {
  inner: DirectionSlice[]
  outer: Slice[]
}

async function get<T>(path: string, params?: URLSearchParams): Promise<T> {
  const response = await fetch(params ? `${path}?${params}` : path)
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    throw new Error(body?.detail ?? `${response.status} ${response.statusText}`)
  }
  return response.json() as Promise<T>
}

function filterParams(filters: CostFilters) {
  const params = new URLSearchParams({
    grain: filters.grain,
    start: filters.start,
    end: filters.end,
    day_basis: filters.dayBasis,
    include_lto: String(filters.includeLto),
  })
  for (const quality of filters.gasQuality) params.append("gas_quality", quality)
  return params
}

export const fetchMeta = () => get<Meta>("/api/balancing-costs/meta")

export const fetchTimeseries = (filters: CostFilters) =>
  get<Timeseries>("/api/balancing-costs/timeseries", filterParams(filters))

export function fetchBreakdown(
  filters: CostFilters,
  bucket: string,
  stackLabel: string,
) {
  const params = filterParams(filters)
  params.set("bucket", bucket)
  params.set("stack_label", stackLabel)
  return get<Breakdown>("/api/balancing-costs/breakdown", params)
}
