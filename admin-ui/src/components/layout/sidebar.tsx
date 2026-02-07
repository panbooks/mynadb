import { NavLink } from 'react-router-dom'
import {
  LayoutDashboard,
  Server,
  Database,
  Table2,
  ArrowDownToLine,
  Activity,
  Settings,
  Network,
  HardDrive,
  Terminal,
  FolderTree,
  Bug,
} from 'lucide-react'
import { cn } from '@/lib/utils'

const navigation = [
  { name: 'Dashboard', href: '/', icon: LayoutDashboard },
  { name: 'Query Console', href: '/query', icon: Terminal },
  { name: 'Storage Browser', href: '/storage', icon: FolderTree },
  { name: 'Nodes', href: '/nodes', icon: Server },
  { name: 'Tables', href: '/tables', icon: Table2 },
  { name: 'Scale-In', href: '/scale-in', icon: ArrowDownToLine },
  { name: 'Events', href: '/events', icon: Activity },
  { name: 'Debug Tools', href: '/debug', icon: Bug },
]

const operations = [
  { name: 'Meta Operations', href: '/meta-operations', icon: Database },
  { name: 'Data Operations', href: '/data-operations', icon: HardDrive },
  { name: 'Router Operations', href: '/router-operations', icon: Network },
]

export function Sidebar() {
  return (
    <div className="flex h-full w-64 flex-col border-r bg-card">
      {/* Logo */}
      <div className="flex h-16 items-center gap-2 border-b px-6">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary text-primary-foreground font-bold">
          M
        </div>
        <div>
          <h1 className="text-lg font-semibold">MynaDB</h1>
          <p className="text-xs text-muted-foreground">Admin Dashboard</p>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-1 px-3 py-4">
        <div className="space-y-1">
          {navigation.map((item) => (
            <NavLink
              key={item.name}
              to={item.href}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-primary text-primary-foreground'
                    : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
                )
              }
            >
              <item.icon className="h-4 w-4" />
              {item.name}
            </NavLink>
          ))}
        </div>

        <div className="pt-4">
          <p className="px-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Operations
          </p>
          <div className="mt-2 space-y-1">
            {operations.map((item) => (
              <NavLink
                key={item.name}
                to={item.href}
                className={({ isActive }) =>
                  cn(
                    'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                    isActive
                      ? 'bg-primary text-primary-foreground'
                      : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
                  )
                }
              >
                <item.icon className="h-4 w-4" />
                {item.name}
              </NavLink>
            ))}
          </div>
        </div>
      </nav>

      {/* Footer */}
      <div className="border-t p-4">
        <NavLink
          to="/settings"
          className={({ isActive }) =>
            cn(
              'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
              isActive
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
            )
          }
        >
          <Settings className="h-4 w-4" />
          Settings
        </NavLink>
      </div>
    </div>
  )
}
