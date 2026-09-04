import type { IncomingMessage, ServerResponse } from 'node:http';

export type RouteParams = Readonly<Record<string, string>>;

export type Handler = (
  req: IncomingMessage,
  res: ServerResponse,
  params: RouteParams,
  url: URL,
) => void | Promise<void>;

interface Route {
  readonly method: string;
  readonly segments: readonly string[];
  readonly handler: Handler;
}

export interface RouteMatch {
  readonly handler: Handler;
  readonly params: RouteParams;
}

/**
 * Reads a route param the pattern guarantees is present (e.g. `:name` in `/api/topics/:name`) —
 * `RouteParams` is indexed, so `noUncheckedIndexedAccess` would otherwise widen every access to
 * `string | undefined` even though the router can't call the handler without it.
 */
export function requireParam(params: RouteParams, name: string): string {
  const value = params[name];
  if (value === undefined) throw new Error(`missing required route param "${name}"`);
  return value;
}

function splitPath(pathname: string): string[] {
  return pathname.split('/').filter((segment) => segment.length > 0);
}

/**
 * A tiny method + path matcher — enough for `/api/topics/:name`-shaped routes without pulling in
 * a routing library. Segments are matched positionally; a leading `:` marks a param.
 */
export class Router {
  private readonly routes: Route[] = [];

  add(method: string, pattern: string, handler: Handler): this {
    this.routes.push({ method: method.toUpperCase(), segments: splitPath(pattern), handler });
    return this;
  }

  get(pattern: string, handler: Handler): this {
    return this.add('GET', pattern, handler);
  }

  post(pattern: string, handler: Handler): this {
    return this.add('POST', pattern, handler);
  }

  patch(pattern: string, handler: Handler): this {
    return this.add('PATCH', pattern, handler);
  }

  delete(pattern: string, handler: Handler): this {
    return this.add('DELETE', pattern, handler);
  }

  match(method: string, pathname: string): RouteMatch | undefined {
    const segments = splitPath(pathname);
    const upperMethod = method.toUpperCase();

    for (const route of this.routes) {
      if (route.method !== upperMethod) continue;
      if (route.segments.length !== segments.length) continue;

      const params: Record<string, string> = {};
      let matched = true;
      for (const [index, routeSegment] of route.segments.entries()) {
        const actual = segments[index] ?? '';
        if (routeSegment.startsWith(':')) {
          params[routeSegment.slice(1)] = decodeURIComponent(actual);
        } else if (routeSegment !== actual) {
          matched = false;
          break;
        }
      }

      if (matched) return { handler: route.handler, params };
    }

    return undefined;
  }
}
