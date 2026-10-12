import { useQuery } from "@tanstack/react-query"
import { Pie, PieChart } from "recharts"

import {
  fetchBreakdown,
  type CostFilters,
  type DirectionSlice,
  type Slice,
} from "@/lib/api"

import { DIRECTION_COLORS, SLICE_COLORS } from "./colors"
import {
  formatBucket,
  formatDeviation,
  formatEur,
  formatPercent,
  formatPrice,
} from "./format"
import { LocationIcon } from "./location-icon"
import { priceTone } from "./price-tone"

type Props = {
  filters: CostFilters
  bucket: string
  stackLabel: string
  color: string
}

type Colored<T> = T & { fill: string }

// Two-level pie for one bar element (both directions of a stack label in
// one bucket): inner ring direction, outer ring product x location. For
// balancing actions the table compares the prices paid/received with the
// reference price (VTP daily average price of the hours' gas days).
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
  // EBG: one product per stack label, already named in the header
  const showProduct = new Set(outer?.map((s) => s.product_label)).size > 1
  const referenceIncomplete = inner?.some(
    (d) => d.reference_complete === false && d.reference_price_eur_per_mwh !== null,
  )

  return (
    <div className="w-[38rem] rounded-lg border bg-background p-3 text-xs text-foreground shadow-lg">
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
          <table className="h-fit w-full">
            <tbody>
              {inner.map((direction) => (
                <DirectionRows
                  key={direction.direction}
                  direction={direction}
                  slices={outer.filter((s) => s.direction === direction.direction)}
                  total={total}
                  showProduct={showProduct}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
      {referenceIncomplete && (
        <div className="mt-2 text-muted-foreground">
          * Referenz ohne den laufenden Gastag (noch nicht veröffentlicht)
        </div>
      )}
    </div>
  )
}

function DirectionRows({
  direction,
  slices,
  total,
  showProduct,
}: {
  direction: Colored<DirectionSlice>
  slices: Colored<Slice>[]
  total: number
  showProduct: boolean
}) {
  return (
    <>
      <tr className="font-medium">
        <td className="pr-1">
          <span className="block size-2.5 rounded-sm" style={{ background: direction.fill }} />
        </td>
        <td className="whitespace-nowrap">
          <div>{direction.direction}</div>
          <Reference prices={direction} />
        </td>
        <PriceCells direction={direction.direction} prices={direction} />
        <td className="pl-2 text-right tabular-nums">{formatEur(direction.amount_eur)}</td>
        <td className="pl-2 text-right tabular-nums">{formatPercent(direction.amount_eur / total)}</td>
      </tr>
      {slices.map((slice) => (
        <tr key={`${slice.product_label}|${slice.location}`}>
          <td className="pr-1">
            <span className="block size-2.5 rounded-sm" style={{ background: slice.fill }} />
          </td>
          <td className="max-w-48">
            <div className="flex items-center gap-1">
              <LocationIcon
                locationType={slice.location_type}
                exchange={slice.exchange}
                kind={slice.location_kind}
              />
              <span className="truncate" title={slice.location}>
                {slice.location}
              </span>
            </div>
            {showProduct && (
              <div className="truncate pl-4.5 text-muted-foreground" title={slice.product_label}>
                {slice.product_label}
              </div>
            )}
          </td>
          <PriceCells direction={slice.direction} prices={slice} />
          <td className="pl-2 text-right text-muted-foreground tabular-nums">
            {formatEur(slice.amount_eur)}
          </td>
          <td className="pl-2 text-right text-muted-foreground tabular-nums">
            {formatPercent(slice.amount_eur / total)}
          </td>
        </tr>
      ))}
    </>
  )
}

// "Ø Ref 75,53 €/MWh" below the direction, or why there is none
function Reference({ prices }: { prices: DirectionSlice }) {
  if (prices.quantity_mwh === null) return null
  if (prices.reference_price_eur_per_mwh === null) {
    return <div className="font-normal text-muted-foreground">Referenz ausstehend</div>
  }
  return (
    <div
      className="font-normal text-muted-foreground"
      title="Referenzpreis: mittlerer Gaspreis (VTP) der Gastage, mengengewichtet"
    >
      Ø Ref {formatPrice(prices.reference_price_eur_per_mwh)}
      {prices.reference_complete === false && "*"}
    </div>
  )
}

// price paid/received, coloured by its deviation from the reference price
function PriceCells({
  direction,
  prices,
}: {
  direction: string
  prices: DirectionSlice | Slice
}) {
  if (prices.price_eur_per_mwh === null) {
    return <td colSpan={2} />
  }
  const tone = priceTone(direction, prices.price_deviation)
  return (
    <>
      <td className="pl-2 text-right whitespace-nowrap tabular-nums" style={{ color: tone }}>
        {formatPrice(prices.price_eur_per_mwh)}
      </td>
      <td className="w-12 pl-1 text-right tabular-nums" style={{ color: tone }}>
        {prices.price_deviation !== null && formatDeviation(prices.price_deviation)}
      </td>
    </>
  )
}
