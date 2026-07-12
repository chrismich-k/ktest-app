import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/about')({
  component: () => (
    <div>
      <h1 className="text-2xl font-bold mb-2">Über uns</h1>
      <p>Kubernetes Test Projekt</p>
    </div>
  ),
})
