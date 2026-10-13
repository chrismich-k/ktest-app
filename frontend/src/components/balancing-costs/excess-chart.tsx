import type { Bucket, CostFilters, Series } from "@/lib/api"

import { formatDeviation, formatEur, formatEurCompact } from "./format"
import { priceTone } from "./price-tone"
import { StackedChart } from "./stacked-chart"
import { SIDE_NAMES, sideSums } from "./sums"

type Props = {
  filters: CostFilters
  buckets: Bucket[]
  series: Series[] // visible series, in stacking order (LTO has no reference)
  colors: Map<string, string>
  height: string
  syncId: string
}

const excess = (bucket: Bucket) => bucket.excess
const referenceValues = (bucket: Bucket) => bucket.reference_values

// Excess cost of the balancing actions over the reference price (VTP daily
// average price): > 0 bought above / sold below the reference
// (unfavourable), < 0 favourable. Hours without a reference price yet (the
// current gas day) are left out.
export function ExcessChart(props: Props) {
  return (
    <StackedChart
      {...props}
      pick={excess}
      formatTick={formatEurCompact}
      tooltip={(bucket, series) => <ExcessTooltip bucket={bucket} series={series} />}
    />
  )
}

function ExcessTooltip({ bucket, series }: { bucket: Bucket; series: Series[] }) {
  if (Object.keys(bucket.excess).length === 0) {
    return <div className="text-muted-foreground">Referenz ausstehend</div>
  }
  const amount = sideSums(bucket, series, excess)
  const reference = sideSums(bucket, series, referenceValues)
  return (
    <>
      <table className="tabular-nums">
        <tbody>
          {(["buy", "sell"] as const).map((side) => {
            // = price / reference - 1, like the deviation in the pie
            const deviation = reference[side] ? amount[side] / reference[side] : null
            const tone = priceTone(SIDE_NAMES[side], deviation)
            return (
              <tr key={side}>
                <td className="pr-3">{SIDE_NAMES[side]}</td>
                <td className="pr-3 text-right" style={{ color: tone }}>
                  {formatEur(amount[side])}
                </td>
                <td className="text-right" style={{ color: tone }}>
                  {deviation !== null && formatDeviation(deviation)}
                </td>
              </tr>
            )
          })}
          <tr className="border-t font-medium">
            <td className="pr-3">Saldo</td>
            <td className="pr-3 text-right">{formatEur(amount.buy + amount.sell)}</td>
            <td />
          </tr>
        </tbody>
      </table>
      {!bucket.reference_complete && (
        <div className="text-muted-foreground">
          ohne den laufenden Gastag (Referenz ausstehend)
        </div>
      )}
    </>
  )
}
