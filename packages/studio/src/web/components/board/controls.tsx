import { useEffect, useState } from 'react';
import { Gauge, Pause, Play } from 'lucide-react';
import { Button } from '../ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';

const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

/** True whenever the OS/browser setting is on — the board never animates particles against it, only a manual "play" click does. */
export function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(() => window.matchMedia(REDUCED_MOTION_QUERY).matches);

  useEffect(() => {
    const list = window.matchMedia(REDUCED_MOTION_QUERY);
    setReduced(list.matches);
    const handleChange = (event: MediaQueryListEvent): void => setReduced(event.matches);
    list.addEventListener('change', handleChange);
    return () => list.removeEventListener('change', handleChange);
  }, []);

  return reduced;
}

export const SPEED_OPTIONS = [
  { value: '0.5', label: '0.5×' },
  { value: '1', label: '1×' },
  { value: '2', label: '2×' },
  { value: '4', label: '4×' },
] as const;

export interface BoardControlsProps {
  readonly paused: boolean;
  readonly onTogglePaused: () => void;
  readonly speed: string;
  readonly onSpeedChange: (speed: string) => void;
  readonly reducedMotion: boolean;
}

/** Play/pause and speed for the particle layer. Honors `prefers-reduced-motion` by starting paused and saying so, but a viewer who wants the motion can still press play. */
export function BoardControls({ paused, onTogglePaused, speed, onSpeedChange, reducedMotion }: BoardControlsProps) {
  return (
    <div className="flex items-center gap-2">
      {reducedMotion && (
        <span className="text-xs text-muted-foreground">Reduced motion is on — animation starts paused</span>
      )}
      <Button
        type="button"
        variant="outline"
        size="sm"
        onClick={onTogglePaused}
        aria-pressed={!paused}
        aria-label={paused ? 'Play the flow animation' : 'Pause the flow animation'}
      >
        {paused ? <Play className="size-4" aria-hidden="true" /> : <Pause className="size-4" aria-hidden="true" />}
        {paused ? 'Play' : 'Pause'}
      </Button>
      <Select value={speed} onValueChange={onSpeedChange} disabled={paused}>
        <SelectTrigger size="sm" className="w-24" aria-label="Animation speed">
          <Gauge className="size-3.5" aria-hidden="true" />
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {SPEED_OPTIONS.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
