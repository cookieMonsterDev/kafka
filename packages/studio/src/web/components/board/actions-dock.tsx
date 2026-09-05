import { ExternalLink, Radio, Send } from 'lucide-react';
import { Link } from '@tanstack/react-router';
import { Button } from '../ui/button';
import { Popover, PopoverAnchor, PopoverContent } from '../ui/popover';
import type { BoardNode } from './layout';

export interface ActionsDockProps {
  readonly node: BoardNode | null;
  /** The clicked node's own screen rect, captured at click time — already accounts for the board's current pan and zoom, since it comes straight from `getBoundingClientRect()`. */
  readonly anchorRect: DOMRect | null;
  readonly onClose: () => void;
}

/** A contextual popover of next steps for the clicked node — what this repo's actual routes can do with it, not a generic context menu. */
export function ActionsDock({ node, anchorRect, onClose }: ActionsDockProps) {
  if (node === null || anchorRect === null) return null;

  return (
    <Popover open onOpenChange={(open) => !open && onClose()}>
      <PopoverAnchor asChild>
        <span
          style={{
            position: 'fixed',
            top: anchorRect.top,
            left: anchorRect.left,
            width: anchorRect.width,
            height: anchorRect.height,
            pointerEvents: 'none',
          }}
        />
      </PopoverAnchor>
      <PopoverContent className="w-56">
        <p className="mb-2 truncate text-sm font-semibold">{node.label}</p>
        <div className="flex flex-col gap-1">
          {node.kind === 'topic' && (
            <>
              <Button asChild variant="ghost" size="sm" className="justify-start" onClick={onClose}>
                <Link to="/topics/$name" params={{ name: node.label }}>
                  <ExternalLink className="size-4" aria-hidden="true" />
                  View topic
                </Link>
              </Button>
              <Button asChild variant="ghost" size="sm" className="justify-start" onClick={onClose}>
                <Link to="/producer" search={{ topic: node.label }}>
                  <Send className="size-4" aria-hidden="true" />
                  Produce here
                </Link>
              </Button>
              <Button asChild variant="ghost" size="sm" className="justify-start" onClick={onClose}>
                <Link to="/messages" search={{ topic: node.label }}>
                  <Radio className="size-4" aria-hidden="true" />
                  Tail this topic
                </Link>
              </Button>
            </>
          )}
          {node.kind === 'group' && (
            <Button asChild variant="ghost" size="sm" className="justify-start" onClick={onClose}>
              <Link to="/groups/$groupId" params={{ groupId: node.label }}>
                <ExternalLink className="size-4" aria-hidden="true" />
                View group
              </Link>
            </Button>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}
