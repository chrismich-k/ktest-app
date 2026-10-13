import type { Bucket, CostFilters, Series } from "@/lib/api"

import { formatMwh, formatMwhCompact, formatPrice } from "./format"
import type { Axis } from "./scale"
import { StackedChart } from "./stacked-chart"
import { SIDE_NAMES, sideSums } from "./sums"

type Props = {
  filters: CostFilters
  buckets: Bucket[]
  series: Series[] // visible series, in stacking order (LTO has no quantity)
  colors: Map<string, string>
  axis?: Axis // shared scale with the cost chart, else automatic
  height: string
  syncId: string
}

const quantities = (bucket: Bucket) => bucket.quantities
const values = (bucket: Bucket) => bucket.values

// Delivered quantities of the balancing actions, stacked like the cost
// chart (buy up, sell down) with the net quantity as a line.
export function QuantityChart(props: Props) {
  return (
    <StackedChart
      {...props}
      pick={quantities}
      formatTick={formatMwhCompact}
      tooltip={(bucket, series) => <QuantityTooltip bucket={bucket} series={series} />}
    />
  )
}

function QuantityTooltip({ bucket, series }: { bucket: Bucket; series: Series[] }) {
  const quantity = sideSums(bucket, series, quantities)
  const amount = sideSums(bucket, series, values)
  return (
    <table className="tabular-nums">
      <tbody>
        {(["buy", "sell"] as const).map((side) => (
          <tr key={side}>
            <td className="pr-3">{SIDE_NAMES[side]}</td>
            <td className="pr-3 text-right">{formatMwh(Math.abs(quantity[side]))}</td>
            <td className="text-right text-muted-foreground">
              {quantity[side] !== 0 &&
                `Ø ${formatPrice(amount[side] / quantity[side])}`}
            </td>
          </tr>
        ))}
        <tr className="border-t font-medium">
          <td className="pr-3">Saldo</td>
          <td className="pr-3 text-right">{formatMwh(quantity.buy + quantity.sell)}</td>
          <td />
        </tr>
      </tbody>
    </table>
  )
}
