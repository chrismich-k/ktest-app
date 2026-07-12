import { createRootRoute, Link, Outlet } from '@tanstack/react-router'

export const Route = createRootRoute({
  component: () => (
    <div className="min-h-screen bg-slate-50 text-slate-950 dark:bg-slate-950 dark:text-slate-50">
      {/* Eine simple, mit Tailwind gestylte Navigationsleiste */}
      <nav className="flex gap-4 p-4 bg-white border-b border-slate-200 dark:bg-slate-900 dark:border-slate-800">
        <Link to="/" className="font-medium hover:underline [&.active]:text-blue-600">
          Startseite
        </Link>
        <Link to="/about" className="font-medium hover:underline [&.active]:text-blue-600">
          Über uns
        </Link>
      </nav>

      {/* Hier landen die Inhalte der jeweiligen Unterseite */}
      <main className="p-4">
        <Outlet />
      </main>
    </div>
  ),
})
