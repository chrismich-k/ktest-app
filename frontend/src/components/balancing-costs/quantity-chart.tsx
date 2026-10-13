import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceLine,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import { ChartContainer } from "@/components/ui/chart"
import type { Bucket, CostFilters, Series } from "@/lib/api"

import { formatBucket, formatMwh, formatMwhCompact, formatPrice } from "./format"
import { Y_AXIS_WIDTH, type Axis } from "./scale"

type Props = {
  filters: CostFilters
  buckets: Bucket[]
  series: Series[] // visible series, in stacking order (LTO has no quantity)
  colors: Map<string, string>
  axis?: Axis // shared scale with the cost chart, else automatic
  height: string
  syncId: string
}

// Delivered quantities of the balancing actions, stacked like the cost
// chart (buy up, sell down) with the net quantity as a line.
export function QuantityChart({
  filters,
  buckets,
  series,
  colors,
  axis,
  height,
  syncId,
}: Props) {
  const withQuantity = series.filter((s) =>
    buckets.some((b) => b.quantities[s.key] !== undefined),
  )
  const net = (bucket: Bucket) =>
    withQuantity.reduce((sum, s) => sum + (bucket.quantities[s.key] ?? 0), 0)

  return (
    <ChartContainer config={{}} className="aspect-auto w-full" style={{ height }}>
      <ComposedChart
        data={buckets}
        stackOffset="sign"
        syncId={syncId}
        margin={{ top: 8, right: 8, bottom: 8, left: 8 }}
      >
        <CartesianGrid vertical={false} />
        <XAxis
          dataKey="bucket"
          tickFormatter={(bucket: string) => formatBucket(bucket, filters.grain)}
          minTickGap={24}
        />
        <YAxis
          tickFormatter={(value: number) => formatMwhCompact(value)}
          width={Y_AXIS_WIDTH}
          {...(axis && { domain: axis.domain, ticks: axis.ticks, allowDataOverflow: true })}
        />
        <ReferenceLine y={0} stroke="var(--foreground)" />
        <Tooltip
          cursor={{ fill: "var(--muted)", fillOpacity: 0.5 }}
          content={({ active, label }) => {
            if (!active || typeof label !== "string") return null
            const bucket = buckets.find((b) => b.bucket === label)
            return (
              bucket && (
                <QuantityTooltip
                  title={formatBucket(label, filters.grain)}
                  bucket={bucket}
                  series={withQuantity}
                />
              )
            )
          }}
        />
        {withQuantity.map((s) => (
          <Bar
            key={s.key}
            name={s.stack_label}
            dataKey={(bucket: Bucket) => bucket.quantities[s.key] ?? 0}
            stackId="quantities"
            fill={colors.get(s.stack_label)}
            fillOpacity={s.stack_side === "sell" ? 0.7 : 1}
            isAnimationActive={false}
          />
        ))}
        <Line
          name="Saldo"
          dataKey={net}
          stroke="var(--foreground)"
          strokeWidth={2}
          dot={buckets.length <= 60}
          isAnimationActive={false}
        />
      </ComposedChart>
    </ChartContainer>
  )
}

function QuantityTooltip({
  title,
  bucket,
  series,
}: {
  title: string
  bucket: Bucket
  series: Series[]
}) {
  const sides = (["buy", "sell"] as const).map((side) => {
    let quantity = 0
    let amount = 0
    for (const s of series) {
      if (s.stack_side !== side) continue
      quantity += bucket.quantities[s.key] ?? 0
      amount += bucket.values[s.key] ?? 0
    }
    return { side, quantity, price: quantity ? amount / quantity : null }
  })
  const net = sides[0].quantity + sides[1].quantity

  return (
    <div className="rounded-lg border bg-background px-3 py-2 text-xs shadow-lg">
      <div className="font-medium">{title}</div>
      <table className="tabular-nums">
        <tbody>
          {sides.map(({ side, quantity, price }) => (
            <tr key={side}>
              <td className="pr-3">{side === "buy" ? "SystemBuy" : "SystemSell"}</td>
              <td className="pr-3 text-right">{formatMwh(Math.abs(quantity))}</td>
              <td className="text-right text-muted-foreground">
                {price !== null && `Ø ${formatPrice(price)}`}
              </td>
            </tr>
          ))}
          <tr className="border-t font-medium">
            <td className="pr-3">Saldo</td>
            <td className="pr-3 text-right">{formatMwh(net)}</td>
            <td />
          </tr>
        </tbody>
      </table>
    </div>
  )
}
