import { createContext, useCallback, useContext, useState, type ReactNode } from 'react';

export type TerminalMode = 'hidden' | 'docked' | 'expanded';
interface TerminalContextValue {
  readonly mode: TerminalMode;
  readonly mounted: boolean;
  readonly open: (mode: Exclude<TerminalMode, 'hidden'>) => void;
  readonly minimize: () => void;
  readonly terminate: () => void;
  readonly toggle: () => void;
}
const TerminalContext = createContext<TerminalContextValue | null>(null);
export function TerminalProvider({ children }: { readonly children: ReactNode }) {
  const [state, setMode] = useState<TerminalMode | 'unopened'>('unopened');
  const mounted = state !== 'unopened';
  const mode = state === 'unopened' ? 'hidden' : state;
  const minimize = useCallback(() => {
    setMode('hidden');
    document.getElementById('terminal-toggle')?.focus();
  }, []);
  const terminate = useCallback(() => {
    setMode('unopened');
    document.getElementById('terminal-toggle')?.focus();
  }, []);
  const toggle = useCallback(() => {
    setMode((current) => (current === 'hidden' || current === 'unopened' ? 'docked' : 'hidden'));
  }, []);
  return (
    <TerminalContext.Provider value={{ mode, mounted, open: setMode, minimize, terminate, toggle }}>
      {children}
    </TerminalContext.Provider>
  );
}
export function useTerminal() {
  const context = useContext(TerminalContext);
  if (context === null) throw new Error('Terminal provider missing');
  return context;
}
