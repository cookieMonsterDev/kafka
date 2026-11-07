import { useId, useRef, useState } from 'react';
import { CalendarIcon } from 'lucide-react';
import { Button } from './button';
import { Calendar } from './calendar';
import { Popover, PopoverContent, PopoverTrigger } from './popover';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './select';
import { formatLocalDateTime, parseLocalDateTime } from '../../lib/local-date-time';
import { cn } from '../../lib/utils';

const HOURS = Array.from({ length: 24 }, (_, index) => String(index).padStart(2, '0'));
const MINUTES = Array.from({ length: 60 }, (_, index) => String(index).padStart(2, '0'));

export function DateTimePicker({
  value,
  onChange,
  id,
  label,
  disabled = false,
  className,
}: {
  readonly value: string;
  readonly onChange: (value: string) => void;
  readonly label: string;
  readonly id?: string;
  readonly disabled?: boolean;
  readonly className?: string;
}) {
  const [open, setOpen] = useState(false);
  const contentRef = useRef<HTMLDivElement>(null);
  const generatedId = useId();
  const valueId = `${generatedId}-value`;
  const hourId = `${generatedId}-hour`;
  const minuteId = `${generatedId}-minute`;
  const selected = parseLocalDateTime(value);
  const hour = String(selected?.getHours() ?? 0).padStart(2, '0');
  const minute = String(selected?.getMinutes() ?? 0).padStart(2, '0');
  function selectDay(date: Date | undefined) {
    if (!date) return;
    const next = new Date(date);
    next.setHours(Number(hour), Number(minute), 0, 0);
    onChange(formatLocalDateTime(next));
  }
  function selectTime(hours: string, minutes: string) {
    const next = selected ? new Date(selected) : new Date();
    next.setHours(Number(hours), Number(minutes), 0, 0);
    onChange(formatLocalDateTime(next));
  }
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          id={id}
          type="button"
          variant="outline"
          disabled={disabled}
          aria-label={label}
          aria-describedby={valueId}
          className={cn(
            'h-10 w-full min-w-0 justify-start gap-2 px-3 text-left font-normal',
            !selected && 'text-muted-foreground',
            className,
          )}
        >
          <CalendarIcon aria-hidden="true" className="size-4 shrink-0" />
          <span id={valueId} className="truncate">
            {selected
              ? selected.toLocaleString(undefined, {
                  year: 'numeric',
                  month: 'short',
                  day: 'numeric',
                  hour: '2-digit',
                  minute: '2-digit',
                  hourCycle: 'h23',
                })
              : 'Pick date and time'}
          </span>
        </Button>
      </PopoverTrigger>
      <PopoverContent
        ref={contentRef}
        onOpenAutoFocus={(event) => {
          const day = contentRef.current?.querySelector<HTMLButtonElement>('[data-day][tabindex="0"]');
          if (day) {
            event.preventDefault();
            day.focus();
          }
        }}
        align="start"
        className="max-h-(--radix-popover-content-available-height) w-auto max-w-[calc(100vw-2rem)] overflow-y-auto p-0"
        aria-label={label}
      >
        <Calendar mode="single" selected={selected} defaultMonth={selected} onSelect={selectDay} autoFocus />
        <div className="grid grid-cols-2 gap-3 border-t border-border p-3">
          <div className="flex flex-col gap-1.5">
            <label htmlFor={hourId} className="text-xs font-medium">
              Hour (24h)
            </label>
            <Select value={hour} onValueChange={(next) => selectTime(next, minute)}>
              <SelectTrigger id={hourId} aria-label={`${label}: hour`} className="h-9 w-full px-3">
                <SelectValue />
              </SelectTrigger>
              <SelectContent position="popper">
                {HOURS.map((item) => (
                  <SelectItem key={item} value={item}>
                    {item}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor={minuteId} className="text-xs font-medium">
              Minute
            </label>
            <Select value={minute} onValueChange={(next) => selectTime(hour, next)}>
              <SelectTrigger id={minuteId} aria-label={`${label}: minute`} className="h-9 w-full px-3">
                <SelectValue />
              </SelectTrigger>
              <SelectContent position="popper">
                {MINUTES.map((item) => (
                  <SelectItem key={item} value={item}>
                    {item}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
        <div className="flex items-center justify-between gap-2 border-t border-border p-3">
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => {
              onChange('');
              setOpen(false);
            }}
          >
            Clear
          </Button>
          <Button type="button" variant="outline" size="sm" onClick={() => onChange(formatLocalDateTime(new Date()))}>
            Now
          </Button>
          <Button type="button" size="sm" onClick={() => setOpen(false)}>
            Done
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  );
}
