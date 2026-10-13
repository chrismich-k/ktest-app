import type { ReactNode } from "react"
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

import { formatBucket } from "./format"
import { Y_AXIS_WIDTH, type Axis } from "./scale"

type Props = {
  filters: CostFilters
  buckets: Bucket[]
  series: Series[] // visible series, in stacking order
  colors: Map<string, string>
  // values per series key of a bucket; series without any are left out
  pick: (bucket: Bucket) => Record<string, number>
  formatTick: (value: number) => string
  tooltip: (bucket: Bucket, series: Series[]) => ReactNode
  axis?: Axis // fixed scale, else automatic
  height: string
  syncId: string
}

// Sign-stacked bars per series (one colour per stack label) with their net
// as a line, aligned with the cost chart (same buckets, Y-axis width and
// sync id). Used for the quantity and excess cost charts.
export function StackedChart({
  filters,
  buckets,
  series,
  colors,
  pick,
  formatTick,
  tooltip,
  axis,
  height,
  syncId,
}: Props) {
  const shown = series.filter((s) => buckets.some((b) => pick(b)[s.key] !== undefined))
  const net = (bucket: Bucket) =>
    shown.reduce((sum, s) => sum + (pick(bucket)[s.key] ?? 0), 0)

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
          tickFormatter={formatTick}
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
                <div className="rounded-lg border bg-background px-3 py-2 text-xs shadow-lg">
                  <div className="font-medium">{formatBucket(label, filters.grain)}</div>
                  {tooltip(bucket, shown)}
                </div>
              )
            )
          }}
        />
        {shown.map((s) => (
          <Bar
            key={s.key}
            name={s.stack_label}
            dataKey={(bucket: Bucket) => pick(bucket)[s.key] ?? 0}
            stackId="stack"
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
