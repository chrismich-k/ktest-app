import type { Bucket, Series } from "@/lib/api"

// SystemBuy and SystemSell sums of a bucket over the given series
export function sideSums(
  bucket: Bucket,
  series: Series[],
  pick: (bucket: Bucket) => Record<string, number>,
) {
  const sums = { buy: 0, sell: 0 }
  for (const s of series) sums[s.stack_side] += pick(bucket)[s.key] ?? 0
  return sums
}

export const SIDE_NAMES = { buy: "SystemBuy", sell: "SystemSell" } as const
