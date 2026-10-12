// Colour of a price paid/received compared to the reference price (VTP
// daily average price): green = favourable for THE, red = unfavourable.
// Within THE's own +-2 % "small adjustment" band the price is neutral.
const NEUTRAL_BAND = 0.02
const STEPS = [0.05, 0.15] // light up to 5 %, medium up to 15 %, strong above

const FAVOURABLE = ["#5f9e70", "#1e9a4a", "#0b7a34"]
const UNFAVOURABLE = ["#c26b6b", "#dc2626", "#a30f0f"]

export function priceTone(
  direction: string,
  deviation: number | null,
): string | undefined {
  if (deviation === null) return undefined
  const size = Math.abs(deviation)
  if (size <= NEUTRAL_BAND) return "var(--foreground)"
  const level = STEPS.findIndex((step) => size <= step)
  const index = level === -1 ? STEPS.length : level
  // buying below / selling above the reference is favourable
  const favourable = direction === "SystemSell" ? deviation > 0 : deviation < 0
  return (favourable ? FAVOURABLE : UNFAVOURABLE)[index]
}
