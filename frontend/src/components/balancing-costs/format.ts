import type { Grain } from "@/lib/api"

const TIME_ZONE = "Europe/Berlin"

export const formatEur = new Intl.NumberFormat("de-DE", {
  style: "currency",
  currency: "EUR",
  maximumFractionDigits: 0,
}).format

export const formatEurCompact = new Intl.NumberFormat("de-DE", {
  style: "currency",
  currency: "EUR",
  notation: "compact",
  maximumFractionDigits: 1,
}).format

export const formatPercent = new Intl.NumberFormat("de-DE", {
  style: "percent",
  maximumFractionDigits: 1,
}).format

// date-only buckets are calendar/gas days, not instants: format them in UTC
// so the browser's time zone cannot shift them
function dateOnly(iso: string) {
  return new Date(`${iso}T00:00:00Z`)
}

function isoWeek(day: Date) {
  const thursday = new Date(day)
  thursday.setUTCDate(day.getUTCDate() + 3 - ((day.getUTCDay() + 6) % 7))
  const firstThursday = new Date(Date.UTC(thursday.getUTCFullYear(), 0, 4))
  const week =
    1 +
    Math.round(
      ((thursday.getTime() - firstThursday.getTime()) / 86400000 -
        3 +
        ((firstThursday.getUTCDay() + 6) % 7)) /
        7,
    )
  return { year: thursday.getUTCFullYear(), week }
}

export function formatBucket(bucket: string, grain: Grain) {
  switch (grain) {
    case "hour":
      // hour buckets are UTC instants; show local (Berlin) wall-clock time
      return new Date(bucket).toLocaleString("de-DE", {
        timeZone: TIME_ZONE,
        day: "2-digit",
        month: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      })
    case "day":
      return dateOnly(bucket).toLocaleDateString("de-DE", {
        timeZone: "UTC",
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
      })
    case "week": {
      const { year, week } = isoWeek(dateOnly(bucket))
      return `KW ${week}/${year}`
    }
    case "month":
      return dateOnly(bucket).toLocaleDateString("de-DE", {
        timeZone: "UTC",
        month: "short",
        year: "numeric",
      })
  }
}

export function addDays(iso: string, days: number) {
  const day = dateOnly(iso)
  day.setUTCDate(day.getUTCDate() + days)
  return day.toISOString().slice(0, 10)
}

export function daysBetween(start: string, end: string) {
  return Math.round(
    (dateOnly(end).getTime() - dateOnly(start).getTime()) / 86400000,
  )
}
