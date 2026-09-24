import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/about')({
  component: () => (
    <div>
      <h1 className="text-2xl font-bold mb-2">About this</h1>
      <p>Kubernetes Test Project</p>
    </div>
  ),
})
