import { z } from 'zod';

/** `POST /api/terminal/sessions/:id/input`: keystrokes for the session's line editor. */
export const terminalInputSchema = z.object({ data: z.string().max(8192) });
