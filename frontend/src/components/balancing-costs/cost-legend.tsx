import { cn } from "@/lib/utils"

type Props = {
  labels: string[]
  colors: Map<string, string>
  hidden: Set<string>
  onToggle: (label: string) => void
}

// One entry per stack label (buy and sell share colour and entry);
// clicking hides or shows both.
export function CostLegend({ labels, colors, hidden, onToggle }: Props) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
      {labels.map((label) => (
        <button
          key={label}
          type="button"
          onClick={() => onToggle(label)}
          className={cn(
            "flex items-center gap-1.5 hover:underline",
            hidden.has(label) && "opacity-40 line-through",
          )}
        >
          <span className="size-3 rounded-sm" style={{ background: colors.get(label) }} />
          {label}
        </button>
      ))}
      <span className="flex items-center gap-1.5">
        <span className="h-0.5 w-4 bg-foreground" />
        Saldo (Kosten − Einnahmen)
      </span>
    </div>
  )
}
