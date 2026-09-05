import { useId } from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger } from '@/components/ui/select';
import { cn } from '@/lib/utils';

export type PackageOption = {
  id: string;
  label: string;
  blurb: string;
  href: string;
};

type PackageSwitcherSelectProps = {
  packages: PackageOption[];
  value: string;
  className?: string;
  /** When set, the select only reports the choice — the parent updates menu content. */
  onPackageChange?: (id: string) => void;
};

export function PackageSwitcherSelect({ packages, value, className, onPackageChange }: PackageSwitcherSelectProps) {
  const labelId = useId();
  const active = packages.find((pkg) => pkg.id === value) ?? packages[0];
  if (active == null) {
    return null;
  }

  function onValueChange(next: string) {
    const pkg = packages.find((item) => item.id === next);
    if (pkg == null || pkg.id === value) {
      return;
    }
    if (onPackageChange != null) {
      onPackageChange(pkg.id);
      return;
    }
    window.location.assign(pkg.href);
  }

  return (
    <div className={cn(className)}>
      <p id={labelId} className="text-muted-foreground mb-2 px-1 text-xs font-medium tracking-wide uppercase">
        Package
      </p>
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger
          aria-labelledby={labelId}
          className="border-border hover:bg-sidebar-accent h-auto min-h-11 w-full items-center gap-3 rounded-lg bg-transparent px-3.5 py-3 whitespace-normal data-[size=default]:h-auto [&[data-state=open]_svg]:rotate-180 [&_svg]:shrink-0 [&_svg]:transition-transform"
        >
          <span className="flex min-w-0 flex-1 flex-col items-start gap-0.5 text-left">
            <span className="text-foreground text-sm leading-snug font-medium" translate="no">
              {active.label}
            </span>
            <span className="text-muted-foreground text-xs leading-snug">{active.blurb}</span>
          </span>
        </SelectTrigger>
        <SelectContent position="popper" align="start" className="w-(--radix-select-trigger-width)">
          {packages.map((pkg) => (
            <SelectItem
              key={pkg.id}
              value={pkg.id}
              className="data-[state=checked]:bg-sidebar-accent data-[state=checked]:text-sidebar-accent-foreground items-start py-2 pr-3 pl-2.5 *:[span]:last:flex-col *:[span]:last:items-start *:[span]:last:gap-0.5 [&>span:first-child]:hidden"
            >
              <span className="leading-tight font-medium" translate="no">
                {pkg.label}
              </span>
              <span className="text-muted-foreground text-xs leading-tight">{pkg.blurb}</span>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
