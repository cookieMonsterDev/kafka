import type { ComponentProps } from 'react';
import { Select as SelectPrimitive } from '@base-ui/react/select';
import { CheckIcon, ChevronDownIcon, ChevronUpIcon } from 'lucide-react';
import { cn } from '../../lib/utils';

export interface SelectOption {
  readonly value: string;
  readonly label: string;
}

/** Shadcn's Base UI Select composition, with an explicit label/value list for controlled fields. */
export function SelectInput({
  value,
  onValueChange,
  items,
  placeholder,
  className,
  ...props
}: {
  readonly value: string;
  readonly onValueChange: (value: string) => void;
  readonly items: readonly SelectOption[];
  readonly placeholder?: string;
} & Omit<ComponentProps<'button'>, 'value' | 'onChange'>) {
  return (
    <SelectPrimitive.Root<string>
      value={value}
      items={items}
      disabled={props.disabled}
      onValueChange={(next) => {
        if (next !== null) onValueChange(next);
      }}
    >
      <SelectPrimitive.Trigger
        type="button"
        {...props}
        data-slot="select-trigger"
        className={cn(
          'flex h-10 w-full min-w-0 items-center gap-2 rounded-lg border border-input bg-input/30 px-3 text-sm outline-none hover:bg-input/50 focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50 data-placeholder:text-muted-foreground',
          className,
        )}
      >
        <SelectPrimitive.Value placeholder={placeholder} className="min-w-0 flex-1 truncate text-left" />
        <SelectPrimitive.Icon
          render={
            <ChevronDownIcon aria-hidden="true" className="pointer-events-none size-4 shrink-0 text-muted-foreground" />
          }
        />
      </SelectPrimitive.Trigger>
      <SelectPrimitive.Portal>
        <SelectPrimitive.Positioner align="start" sideOffset={4} alignItemWithTrigger={false} className="isolate z-50">
          <SelectPrimitive.Popup
            data-slot="select-content"
            className="max-h-[min(18rem,var(--available-height))] w-(--anchor-width) min-w-36 overflow-y-auto rounded-lg bg-popover p-1 text-popover-foreground shadow-md ring-1 ring-foreground/10 outline-none"
          >
            <SelectPrimitive.ScrollUpArrow className="flex h-6 items-center justify-center">
              <ChevronUpIcon aria-hidden="true" className="size-4" />
            </SelectPrimitive.ScrollUpArrow>
            <SelectPrimitive.List>
              {items.map((item) => (
                <SelectPrimitive.Item
                  key={item.value}
                  value={item.value}
                  className="relative flex min-h-8 cursor-default items-center rounded-md py-1.5 pr-8 pl-2 text-sm outline-none select-none data-highlighted:bg-accent data-highlighted:text-accent-foreground data-disabled:pointer-events-none data-disabled:opacity-50"
                >
                  <SelectPrimitive.ItemText className="min-w-0 flex-1 break-words">
                    {item.label}
                  </SelectPrimitive.ItemText>
                  <SelectPrimitive.ItemIndicator className="absolute right-2 flex size-4 items-center justify-center">
                    <CheckIcon aria-hidden="true" className="size-4" />
                  </SelectPrimitive.ItemIndicator>
                </SelectPrimitive.Item>
              ))}
            </SelectPrimitive.List>
            <SelectPrimitive.ScrollDownArrow className="flex h-6 items-center justify-center">
              <ChevronDownIcon aria-hidden="true" className="size-4" />
            </SelectPrimitive.ScrollDownArrow>
          </SelectPrimitive.Popup>
        </SelectPrimitive.Positioner>
      </SelectPrimitive.Portal>
    </SelectPrimitive.Root>
  );
}
