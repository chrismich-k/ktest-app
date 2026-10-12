import { CircleHelp, Handshake, Landmark, MapPin, MapPinHouse } from "lucide-react"

import type { LocationKind } from "@/lib/api"

// how it is traded wins (exchange / OTC); otherwise what it is
const TRADED = {
  Exchange: { Icon: Landmark, title: "Börse" },
  OTC: { Icon: Handshake, title: "OTC" },
} as const

const KINDS = {
  storage: { Icon: MapPinHouse, title: "Speicher" },
  zone: { Icon: MapPin, title: "Region/Zone" },
  unknown: { Icon: CircleHelp, title: "unbekannt" },
} satisfies Record<LocationKind, { Icon: typeof Landmark; title: string }>

export function LocationIcon({
  locationType,
  exchange,
  kind,
}: {
  locationType: string | null
  exchange: string | null
  kind: LocationKind
}) {
  const traded = locationType ? TRADED[locationType as keyof typeof TRADED] : undefined
  const what = KINDS[kind] ?? KINDS.unknown
  const { Icon, title } = traded ?? what
  // e.g. "Börse (EEX) · Region/Zone"
  const label = traded
    ? `${title}${exchange && exchange !== locationType ? ` (${exchange})` : ""} · ${what.title}`
    : title
  return (
    <Icon className="size-3.5 shrink-0 text-muted-foreground" aria-label={label}>
      <title>{label}</title>
    </Icon>
  )
}
