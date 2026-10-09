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
  values: Record<string, number>
  net: number
}

export type Timeseries = {
  grain: Grain
  series: Series[]
  buckets: Bucket[]
}

export type Slice = {
  direction: string
  product_label: string | null
  location_label: string | null
  amount_eur: number
}

export type Breakdown = {
  inner: Slice[]
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
