import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/')({
  component: () => <div className="p-4">Nur eine Startseite.</div>,
})
