import { useRef, useState } from "react"
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
import { useDebouncedValue } from "@/hooks/use-debounced-value"
import type { Bucket, CostFilters, Series } from "@/lib/api"

import { BreakdownPie } from "./breakdown-pie"
import { formatBucket, formatEur, formatEurCompact } from "./format"

// pointer position relative to the chart (and its width), for placing the
// pie overlay
type Hover = {
  bucket: string
  stackLabel: string
  x: number
  y: number
  width: number
}

const PIE_WIDTH = 416 // w-[26rem] of BreakdownPie
const PIE_OFFSET = 16

type Props = {
  filters: CostFilters
  buckets: Bucket[]
  series: Series[] // visible series, in stacking order
  colors: Map<string, string>
}

// Stacked bars per product (buy up, sell down, one colour per stack label)
// with the net amount of the visible series as a line on top. Hovering a
// bar segment shows the breakdown pie of its stack label as an overlay
// (a chart nested into the Recharts tooltip does not size correctly).
export function CostChart({ filters, buckets, series, colors }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [hover, setHover] = useState<Hover | null>(null)
  // only ask the API for the pie once the pointer rests on a segment
  const debouncedHover = useDebouncedValue(hover, 200)
  const pieReady =
    hover !== null &&
    debouncedHover?.bucket === hover.bucket &&
    debouncedHover.stackLabel === hover.stackLabel

  const net = (bucket: Bucket) =>
    series.reduce((sum, s) => sum + (bucket.values[s.key] ?? 0), 0)

  const pieLeft =
    hover && hover.x + PIE_OFFSET + PIE_WIDTH > hover.width
      ? hover.x - PIE_OFFSET - PIE_WIDTH
      : (hover?.x ?? 0) + PIE_OFFSET

  return (
    <div ref={containerRef} className="relative">
      <ChartContainer config={{}} className="aspect-auto h-[32rem] w-full">
        <ComposedChart
          data={buckets}
          stackOffset="sign"
          margin={{ top: 8, right: 8, bottom: 8, left: 8 }}
        >
          <CartesianGrid vertical={false} />
          <XAxis
            dataKey="bucket"
            tickFormatter={(bucket: string) => formatBucket(bucket, filters.grain)}
            minTickGap={24}
          />
          <YAxis tickFormatter={(value: number) => formatEurCompact(value)} width={84} />
          <ReferenceLine y={0} stroke="var(--foreground)" />
          <Tooltip
            cursor={{ fill: "var(--muted)", fillOpacity: 0.5 }}
            content={({ active, label }) => {
              // over a segment the pie overlay takes over
              if (!active || hover || typeof label !== "string") return null
              const bucket = buckets.find((b) => b.bucket === label)
              return (
                <div className="rounded-lg border bg-background px-3 py-2 text-xs shadow-lg">
                  <div className="font-medium">{formatBucket(label, filters.grain)}</div>
                  {bucket && <div>Saldo: {formatEur(net(bucket))}</div>}
                  <div className="text-muted-foreground">
                    Balkensegment überfahren für die Aufschlüsselung
                  </div>
                </div>
              )
            }}
          />
          {series.map((s) => (
            <Bar
              key={s.key}
              name={s.stack_label}
              dataKey={(bucket: Bucket) => bucket.values[s.key] ?? 0}
              stackId="costs"
              fill={colors.get(s.stack_label)}
              fillOpacity={s.stack_side === "sell" ? 0.7 : 1}
              isAnimationActive={false}
              onMouseEnter={(item, _index, event) => {
                const rect = containerRef.current?.getBoundingClientRect()
                if (!rect) return
                setHover({
                  bucket: (item.payload as Bucket).bucket,
                  stackLabel: s.stack_label,
                  x: event.clientX - rect.left,
                  y: event.clientY - rect.top,
                  width: rect.width,
                })
              }}
              onMouseLeave={() => setHover(null)}
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
      {hover && pieReady && (
        <div
          className="pointer-events-none absolute z-10"
          style={{ left: Math.max(0, pieLeft), top: Math.max(0, hover.y - 80) }}
        >
          <BreakdownPie
            filters={filters}
            bucket={hover.bucket}
            stackLabel={hover.stackLabel}
            color={colors.get(hover.stackLabel) ?? "#64748b"}
          />
        </div>
      )}
    </div>
  )
}
