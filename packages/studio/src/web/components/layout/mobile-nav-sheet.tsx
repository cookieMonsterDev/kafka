import { Sheet, SheetContent, SheetHeader, SheetTitle } from '../ui/sheet';
import { useAppShell } from './app-shell';
import { SidebarContent } from './sidebar';

/**
 * The off-canvas counterpart to `Sidebar`, sharing `SidebarContent` so the two chrome states can
 * never drift apart — the mobile drawer carries the same nav, profile switcher, and connection
 * pill as the fixed sidebar, just inside a dialog instead of a persistent panel. Opened from the
 * top bar's menu button (`useAppShell().openMobileNav`); always mounted — Radix only portals the
 * dialog content while `open` is true, so this is inert whenever the fixed sidebar is showing.
 */
export function MobileNavSheet() {
  const { mobileNavOpen, closeMobileNav } = useAppShell();

  return (
    <Sheet open={mobileNavOpen} onOpenChange={(open) => !open && closeMobileNav()}>
      <SheetContent side="left" className="flex h-full flex-col gap-0 p-0">
        <SheetHeader className="border-b border-border px-4 py-3">
          <SheetTitle className="text-sm">Kafka Studio</SheetTitle>
        </SheetHeader>
        <nav aria-label="Primary" className="flex flex-1 flex-col overflow-hidden">
          <SidebarContent collapsed={false} onNavigate={closeMobileNav} />
        </nav>
      </SheetContent>
    </Sheet>
  );
}
