import { useEffect } from 'react';
import { createLazyRoute, useNavigate } from '@tanstack/react-router';
import { useTerminal } from '../components/terminal/terminal-context';

export const Route = createLazyRoute('/console')({ component: OpenTerminal });

function OpenTerminal() {
  const { open } = useTerminal();
  const navigate = useNavigate();
  useEffect(() => {
    open('expanded');
    void navigate({ to: '/' });
  }, [open, navigate]);
  return null;
}
