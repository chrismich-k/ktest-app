import type { Meta } from "@/lib/api"

// categorical palette for balancing products, greys for LTO capacity
// prices; colours follow the label's position in /meta, so they stay the
// same whatever the filters show
const PRODUCT_COLORS = [
  "#2563eb", "#f59e0b", "#10b981", "#ef4444", "#8b5cf6", "#06b6d4",
  "#ec4899", "#84cc16", "#f97316", "#14b8a6", "#6366f1", "#eab308",
  "#a855f7", "#22c55e", "#0ea5e9", "#d946ef",
]
const LTO_COLORS = ["#475569", "#94a3b8", "#1e293b", "#cbd5e1"]

export function stackColors(meta: Meta): Map<string, string> {
  const colors = new Map<string, string>()
  let product = 0
  let lto = 0
  for (const { stack_label, source } of meta.stack_labels) {
    colors.set(
      stack_label,
      source === "LTO"
        ? LTO_COLORS[lto++ % LTO_COLORS.length]
        : PRODUCT_COLORS[product++ % PRODUCT_COLORS.length],
    )
  }
  return colors
}

// pie slices (product x location) of one bar element
export const SLICE_COLORS = [
  "#0f766e", "#b45309", "#1d4ed8", "#be123c", "#6d28d9", "#15803d",
  "#c2410c", "#0369a1", "#a21caf", "#4d7c0f", "#475569", "#9f1239",
]

export const DIRECTION_COLORS: Record<string, string> = {
  SystemBuy: "#334155",
  SystemSell: "#94a3b8",
}
