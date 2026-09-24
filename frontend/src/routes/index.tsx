import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/')({
  component: () => (
    <div>
      <h1 className="text-2xl font-bold mb-2">Welcome</h1>
      <p>This is how it starts.</p>
    </div>
  ),
})
