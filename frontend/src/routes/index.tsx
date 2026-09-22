import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/')({
  component: () => <div className="p-4">This is how it starts.</div>,
})
