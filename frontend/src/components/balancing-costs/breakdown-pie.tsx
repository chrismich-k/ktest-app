import { useQuery } from "@tanstack/react-query"
import { Pie, PieChart } from "recharts"

import { fetchBreakdown, type CostFilters } from "@/lib/api"

import { DIRECTION_COLORS, SLICE_COLORS } from "./colors"
import { formatBucket, formatEur, formatPercent } from "./format"

type Props = {
  filters: CostFilters
  bucket: string
  stackLabel: string
  color: string
}

// Two-level pie for one bar element (both directions of a stack label in
// one bucket): inner ring direction, outer ring product x location.
export function BreakdownPie({ filters, bucket, stackLabel, color }: Props) {
  const { data, error, isPending } = useQuery({
    queryKey: ["balancing-costs", "breakdown", filters, bucket, stackLabel],
    queryFn: () => fetchBreakdown(filters, bucket, stackLabel),
  })

  const total = data?.inner.reduce((sum, s) => sum + s.amount_eur, 0) ?? 0
  const inner = data?.inner.map((s) => ({
    ...s,
    fill: DIRECTION_COLORS[s.direction] ?? "#64748b",
  }))
  const outer = data?.outer.map((s, i) => ({
    ...s,
    fill: SLICE_COLORS[i % SLICE_COLORS.length],
  }))

  return (
    <div className="w-[26rem] rounded-lg border bg-background p-3 text-xs text-foreground shadow-lg">
      <div className="mb-2 flex items-center gap-2 font-medium">
        <span className="size-3 shrink-0 rounded-sm" style={{ background: color }} />
        <span>{stackLabel}</span>
        <span className="ml-auto text-muted-foreground">
          {formatBucket(bucket, filters.grain)}
        </span>
      </div>
      {isPending && <div className="py-10 text-center text-muted-foreground">lädt …</div>}
      {error && <div className="text-destructive">{error.message}</div>}
      {inner && outer && (
        <div className="flex gap-3">
          <div className="shrink-0">
            <PieChart width={150} height={150}>
              <Pie
                data={inner}
                dataKey="amount_eur"
                nameKey="direction"
                outerRadius={42}
                isAnimationActive={false}
                stroke="var(--background)"
              />
              <Pie
                data={outer}
                dataKey="amount_eur"
                innerRadius={48}
                outerRadius={72}
                isAnimationActive={false}
                stroke="var(--background)"
              />
            </PieChart>
          </div>
          <table className="h-fit flex-1">
            <tbody>
              {inner.map((direction) => (
                <DirectionRows
                  key={direction.direction}
                  direction={direction}
                  slices={outer.filter((s) => s.direction === direction.direction)}
                  total={total}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

type Colored<T> = T & { fill: string }

function DirectionRows({
  direction,
  slices,
  total,
}: {
  direction: Colored<{ direction: string; amount_eur: number }>
  slices: Colored<{ product_label: string | null; location_label: string | null; amount_eur: number }>[]
  total: number
}) {
  return (
    <>
      <tr className="font-medium">
        <td className="pr-1">
          <span className="block size-2.5 rounded-sm" style={{ background: direction.fill }} />
        </td>
        <td>{direction.direction}</td>
        <td className="pl-2 text-right tabular-nums">{formatEur(direction.amount_eur)}</td>
        <td className="pl-2 text-right tabular-nums">{formatPercent(direction.amount_eur / total)}</td>
      </tr>
      {slices.map((slice) => (
        <tr key={`${slice.product_label}|${slice.location_label}`} className="text-muted-foreground">
          <td className="pr-1">
            <span className="block size-2.5 rounded-sm" style={{ background: slice.fill }} />
          </td>
          <td className="max-w-48">
            <div className="truncate text-foreground" title={slice.location_label ?? ""}>
              {slice.location_label}
            </div>
            <div className="truncate" title={slice.product_label ?? ""}>
              {slice.product_label}
            </div>
          </td>
          <td className="pl-2 text-right tabular-nums">{formatEur(slice.amount_eur)}</td>
          <td className="pl-2 text-right tabular-nums">{formatPercent(slice.amount_eur / total)}</td>
        </tr>
      ))}
    </>
  )
}
