import type { Bucket, Series } from "@/lib/api"

export type Axis = { domain: [number, number]; ticks: number[] }

const TICK_COUNT = 6

function niceStep(range: number) {
  const raw = range / (TICK_COUNT - 1)
  const magnitude = 10 ** Math.floor(Math.log10(raw))
  const step = [1, 2, 2.5, 5, 10].find((f) => f * magnitude >= raw) ?? 10
  return step * magnitude
}

// ticks at round values; the domain starts and ends on a tick
export function niceAxis(min: number, max: number): Axis {
  min = Math.min(min, 0)
  max = Math.max(max, 0)
  if (max - min <= 0) return { domain: [0, 1], ticks: [0, 1] }
  const step = niceStep(max - min)
  const lo = Math.floor(min / step) * step
  const hi = Math.ceil(max / step) * step
  const ticks = []
  for (let t = lo; t <= hi + step / 2; t += step) ticks.push(Math.round(t / step) * step)
  return { domain: [lo, hi], ticks }
}

// the axis scaled down by a factor, with round ticks inside its domain
export function scaledAxis(axis: Axis, factor: number): Axis {
  const domain: [number, number] = [axis.domain[0] / factor, axis.domain[1] / factor]
  const step = niceStep(domain[1] - domain[0])
  const ticks = []
  for (let t = Math.ceil(domain[0] / step) * step; t <= domain[1]; t += step) {
    ticks.push(Math.round(t / step) * step)
  }
  return { domain, ticks }
}

// lowest and highest point of the sign-stacked bars and the net line
export function stackExtent(
  buckets: Bucket[],
  series: Series[],
  pick: (bucket: Bucket) => Record<string, number>,
): [number, number] {
  let min = 0
  let max = 0
  for (const bucket of buckets) {
    const values = pick(bucket)
    let up = 0
    let down = 0
    for (const s of series) {
      const value = values[s.key] ?? 0
      if (value > 0) up += value
      else down += value
    }
    min = Math.min(min, down, up + down)
    max = Math.max(max, up, up + down)
  }
  return [min, max]
}

// volume-weighted price of the balancing actions (buy and sell, absolute)
export function averagePrice(buckets: Bucket[], series: Series[]) {
  let amount = 0
  let quantity = 0
  for (const bucket of buckets) {
    for (const s of series) {
      const q = bucket.quantities[s.key]
      if (q === undefined) continue
      quantity += Math.abs(q)
      amount += Math.abs(bucket.values[s.key] ?? 0)
    }
  }
  return quantity > 0 && amount > 0 ? amount / quantity : null
}

// same width in both charts, so their bars line up
export const Y_AXIS_WIDTH = 100
