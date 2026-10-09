import { createFileRoute } from "@tanstack/react-router"

import { BalancingCostsPage } from "@/components/balancing-costs/balancing-costs-page"

export const Route = createFileRoute("/regelenergie")({
  component: BalancingCostsPage,
})
