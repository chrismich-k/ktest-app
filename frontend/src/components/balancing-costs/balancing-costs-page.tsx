import { keepPreviousData, useQuery } from "@tanstack/react-query"
import { useState } from "react"

import { CostChart } from "./cost-chart"
import { CostFilterBar, type ChartKind } from "./cost-filters"
import { CostLegend } from "./cost-legend"
import { stackColors } from "./colors"
import { addDays, formatEur, formatMwh, formatPrice } from "./format"
import { QuantityChart } from "./quantity-chart"
import { averagePrice, niceAxis, scaledAxis, stackExtent, type Axis } from "./scale"

// hovering one chart shows the tooltip of the same bucket in the other
const SYNC_ID = "balancing-costs"
import { fetchMeta, fetchTimeseries, type CostFilters, type Meta } from "@/lib/api"

export function BalancingCostsPage() {
  const meta = useQuery({
    queryKey: ["balancing-costs", "meta"],
    queryFn: fetchMeta,
  })
  return (
    <div className="flex flex-col gap-4">
      <h1 className="text-2xl font-bold">Kosten für Regelenergie</h1>
      {meta.error && <p className="text-destructive">{meta.error.message}</p>}
      {meta.isPending && <p className="text-muted-foreground">lädt …</p>}
      {meta.data && <BalancingCosts meta={meta.data} />}
    </div>
  )
}

function BalancingCosts({ meta }: { meta: Meta }) {
  const [filters, setFilters] = useState<CostFilters>(() => ({
    grain: "month",
    // the last 12 months up to the latest gas day with balancing actions
    start: `${addDays(meta.last_balancing_gas_day, -365).slice(0, 7)}-01`,
    end: meta.last_balancing_gas_day,
    dayBasis: "gas_day",
    includeLto: true,
    gasQuality: [],
  }))
  const [hidden, setHidden] = useState<Set<string>>(new Set())
  const [charts, setCharts] = useState<ChartKind[]>(["cost", "quantity"])

  const timeseries = useQuery({
    queryKey: ["balancing-costs", "timeseries", filters],
    // remember which filters the data belongs to: while new data loads the
    // previous chart stays visible, and its pie requests need its filters
    queryFn: async () => ({ filters, ...(await fetchTimeseries(filters)) }),
    placeholderData: keepPreviousData,
  })

  const colors = stackColors(meta)
  const order = new Map(meta.stack_labels.map((s, i) => [s.stack_label, i]))
  const allSeries = [...(timeseries.data?.series ?? [])].sort(
    (a, b) =>
      (order.get(a.stack_label) ?? 0) - (order.get(b.stack_label) ?? 0) ||
      a.stack_side.localeCompare(b.stack_side),
  )
  const labels = [...new Set(allSeries.map((s) => s.stack_label))]
  const series = allSeries.filter((s) => !hidden.has(s.stack_label))

  const toggle = (label: string) => {
    const next = new Set(hidden)
    if (!next.delete(label)) next.add(label)
    setHidden(next)
  }

  const buckets = timeseries.data?.buckets ?? []
  let cost = 0
  let revenue = 0
  let bought = 0
  let sold = 0
  for (const bucket of buckets) {
    for (const s of series) {
      const value = bucket.values[s.key] ?? 0
      if (value > 0) cost += value
      else revenue += value
      const quantity = bucket.quantities[s.key] ?? 0
      if (quantity > 0) bought += quantity
      else sold += quantity
    }
  }

  // both charts on one scale: a bar of q MWh is as high as one of
  // q x average price EUR, so the height difference shows the price level
  const showCost = charts.includes("cost")
  const showQuantity = charts.includes("quantity")
  const price = averagePrice(buckets, series)
  let costAxis: Axis | undefined
  let quantityAxis: Axis | undefined
  if (showCost && showQuantity && price !== null) {
    const [costMin, costMax] = stackExtent(buckets, series, (b) => b.values)
    const [qMin, qMax] = stackExtent(buckets, series, (b) => b.quantities)
    costAxis = niceAxis(Math.min(costMin, qMin * price), Math.max(costMax, qMax * price))
    quantityAxis = scaledAxis(costAxis, price)
  }
  const height = showCost && showQuantity ? "22rem" : "32rem"

  return (
    <>
      <CostFilterBar
        meta={meta}
        filters={filters}
        onChange={setFilters}
        charts={charts}
        onChartsChange={setCharts}
      />
      {timeseries.error && (
        <p className="text-destructive">{timeseries.error.message}</p>
      )}
      {timeseries.data && (
        <div className={timeseries.isFetching ? "opacity-60" : undefined}>
          {showCost && (
            <>
              <div className="mb-2 flex gap-6 text-sm">
                <span>Kosten: <b>{formatEur(cost)}</b></span>
                <span>Einnahmen: <b>{formatEur(Math.abs(revenue))}</b></span>
                <span>Saldo: <b>{formatEur(cost + revenue)}</b></span>
              </div>
              <CostChart
                filters={timeseries.data.filters}
                buckets={buckets}
                series={series}
                colors={colors}
                axis={costAxis}
                height={height}
                syncId={SYNC_ID}
              />
            </>
          )}
          {showQuantity && (
            <>
              <div className="mt-4 mb-2 flex flex-wrap gap-x-6 text-sm">
                <span>SystemBuy: <b>{formatMwh(bought)}</b></span>
                <span>SystemSell: <b>{formatMwh(Math.abs(sold))}</b></span>
                <span>Saldo: <b>{formatMwh(bought + sold)}</b></span>
                {price !== null && (
                  <span>Ø Preis: <b>{formatPrice(price)}</b></span>
                )}
                {quantityAxis && (
                  <span className="text-muted-foreground">
                    gleiche Balkenhöhe in beiden Diagrammen ≙ Ø Preis
                  </span>
                )}
              </div>
              <QuantityChart
                filters={timeseries.data.filters}
                buckets={buckets}
                series={series}
                colors={colors}
                axis={quantityAxis}
                height={height}
                syncId={SYNC_ID}
              />
            </>
          )}
          <CostLegend labels={labels} colors={colors} hidden={hidden} onToggle={toggle} />
        </div>
      )}
    </>
  )
}
