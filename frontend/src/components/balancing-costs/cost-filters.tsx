import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import type { CostFilters, DayBasis, Grain, Meta } from "@/lib/api"

import { addDays, daysBetween } from "./format"

// keep in line with MAX_HOURLY_DAYS of the API
const MAX_HOURLY_DAYS = 62

const GRAINS: { value: Grain; label: string }[] = [
  { value: "hour", label: "Stunde" },
  { value: "day", label: "Tag" },
  { value: "week", label: "Woche" },
  { value: "month", label: "Monat" },
]

const DAY_BASES: { value: DayBasis; label: string }[] = [
  { value: "gas_day", label: "Gastag" },
  { value: "calendar_date", label: "Kalendertag" },
]

export type ChartKind = "cost" | "quantity" | "excess"

const CHARTS: { value: ChartKind; label: string }[] = [
  { value: "cost", label: "Kosten" },
  { value: "quantity", label: "Mengen" },
  { value: "excess", label: "Mehrkosten" },
]

type Props = {
  meta: Meta
  filters: CostFilters
  onChange: (filters: CostFilters) => void
  charts: ChartKind[]
  onChartsChange: (charts: ChartKind[]) => void
}

export function CostFilterBar({
  meta,
  filters,
  onChange,
  charts,
  onChartsChange,
}: Props) {
  const set = (patch: Partial<CostFilters>) => onChange({ ...filters, ...patch })

  const setGrain = (grain: Grain) => {
    // hourly bars: shorten a long range instead of failing
    const tooLong =
      grain === "hour" && daysBetween(filters.start, filters.end) >= MAX_HOURLY_DAYS
    set({ grain, ...(tooLong && { start: addDays(filters.end, -6) }) })
  }

  // empty selection means all gas qualities
  const selectedQualities =
    filters.gasQuality.length > 0 ? filters.gasQuality : meta.gas_qualities
  const toggleQuality = (quality: string, checked: boolean) => {
    const next = meta.gas_qualities.filter((q) =>
      q === quality ? checked : selectedQualities.includes(q),
    )
    if (next.length === 0) return
    set({ gasQuality: next.length === meta.gas_qualities.length ? [] : next })
  }

  return (
    <div className="flex flex-wrap items-end gap-x-6 gap-y-3">
      <Field label="Diagramme">
        <ToggleGroup
          multiple
          variant="outline"
          size="sm"
          spacing={0}
          value={charts}
          onValueChange={(values) =>
            values.length > 0 &&
            onChartsChange(
              CHARTS.map((c) => c.value).filter((c) => values.includes(c)),
            )
          }
        >
          {CHARTS.map((c) => (
            <ToggleGroupItem
              key={c.value}
              value={c.value}
              // at least one chart stays visible
              disabled={charts.length === 1 && charts[0] === c.value}
            >
              {c.label}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      </Field>

      <Field label="Auflösung">
        <ToggleGroup
          variant="outline"
          size="sm"
          spacing={0}
          value={[filters.grain]}
          onValueChange={(values) => values[0] && setGrain(values[0] as Grain)}
        >
          {GRAINS.map((g) => (
            <ToggleGroupItem key={g.value} value={g.value}>
              {g.label}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      </Field>

      <Field label="Tage nach">
        <ToggleGroup
          variant="outline"
          size="sm"
          spacing={0}
          value={[filters.dayBasis]}
          onValueChange={(values) =>
            values[0] && set({ dayBasis: values[0] as DayBasis })
          }
        >
          {DAY_BASES.map((b) => (
            <ToggleGroupItem key={b.value} value={b.value}>
              {b.label}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      </Field>

      <Field label="Von">
        <Input
          type="date"
          className="h-7 w-36"
          value={filters.start}
          min={meta.first_gas_day}
          max={filters.end}
          onChange={(e) => e.target.value && set({ start: e.target.value })}
        />
      </Field>
      <Field label="Bis">
        <Input
          type="date"
          className="h-7 w-36"
          value={filters.end}
          min={filters.start}
          max={meta.last_gas_day}
          onChange={(e) => e.target.value && set({ end: e.target.value })}
        />
      </Field>

      <Field label="Gassorte">
        <div className="flex h-7 items-center gap-4">
          {meta.gas_qualities.map((quality) => (
            <Label key={quality} className="font-normal">
              <Checkbox
                checked={selectedQualities.includes(quality)}
                onCheckedChange={(checked) => toggleQuality(quality, checked)}
              />
              {quality}
            </Label>
          ))}
        </div>
      </Field>

      <Field label="Ausschreibungen">
        <div className="flex h-7 items-center">
          <Label className="font-normal">
            <Checkbox
              checked={filters.includeLto}
              onCheckedChange={(checked) => set({ includeLto: checked })}
            />
            LTO-Leistungspreise
          </Label>
        </div>
      </Field>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-xs text-muted-foreground">{label}</span>
      {children}
    </div>
  )
}
