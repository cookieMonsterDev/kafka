import { Sheet, SheetContent, SheetHeader, SheetTitle } from '../ui/sheet';
import { useAppShell } from './app-shell';
import { SidebarNav } from './sidebar-nav';

/**
 * The off-canvas counterpart to `Sidebar`, sharing `SidebarNav` so the two chrome states can never
 * drift apart. Opened from the top bar's menu button (`useAppShell().openMobileNav`); always
 * mounted — Radix only portals the dialog content while `open` is true, so this is inert (and
 * inaccessible to assistive tech) whenever the fixed sidebar is showing instead.
 */
export function MobileNavSheet() {
  const { mobileNavOpen, closeMobileNav } = useAppShell();

  return (
    <Sheet open={mobileNavOpen} onOpenChange={(open) => !open && closeMobileNav()}>
      <SheetContent side="left">
        <SheetHeader>
          <SheetTitle>Kafka Studio</SheetTitle>
        </SheetHeader>
        <nav aria-label="Primary" className="flex-1 overflow-y-auto">
          <SidebarNav collapsed={false} onNavigate={closeMobileNav} />
        </nav>
      </SheetContent>
    </Sheet>
  );
}
