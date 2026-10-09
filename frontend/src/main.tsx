import React from 'react'
import ReactDOM from 'react-dom/client'
import { RouterProvider, createRouter } from '@tanstack/react-router'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { routeTree } from './routeTree.gen'
import './index.css' // get tailwind

// auto-generate router tree
const router = createRouter({ routeTree })

const queryClient = new QueryClient({
  defaultOptions: {
    // the data changes at most a few times a day
    queries: { staleTime: 5 * 60 * 1000, refetchOnWindowFocus: false },
  },
})

// register types for type safety
declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </React.StrictMode>,
)
