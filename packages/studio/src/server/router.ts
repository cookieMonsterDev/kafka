import type { IncomingMessage, ServerResponse } from 'node:http';

export type RouteParams = Readonly<Record<string, string>>;

export type Handler = (
  req: IncomingMessage,
  res: ServerResponse,
  params: RouteParams,
  url: URL,
) => void | Promise<void>;

export interface RouteOptions {
  /**
   * Exempts a mutating route from `--read-only` rejection. For local UI/session state that isn't
   * a Kafka cluster mutation at all — e.g. switching which configured profile is active — not for
   * anything that writes to the cluster.
   */
  readonly allowInReadOnly?: boolean;
}

interface Route {
  readonly method: string;
  readonly segments: readonly string[];
  readonly handler: Handler;
  readonly allowInReadOnly: boolean;
}

export interface RouteMatch {
  readonly handler: Handler;
  readonly params: RouteParams;
  /** `true` for any method other than `GET`/`HEAD` — the signal `--read-only` enforcement acts on. */
  readonly mutating: boolean;
  readonly allowInReadOnly: boolean;
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

  add(method: string, pattern: string, handler: Handler, options: RouteOptions = {}): this {
    this.routes.push({
      method: method.toUpperCase(),
      segments: splitPath(pattern),
      handler,
      allowInReadOnly: options.allowInReadOnly ?? false,
    });
    return this;
  }

  get(pattern: string, handler: Handler, options?: RouteOptions): this {
    return this.add('GET', pattern, handler, options);
  }

  post(pattern: string, handler: Handler, options?: RouteOptions): this {
    return this.add('POST', pattern, handler, options);
  }

  patch(pattern: string, handler: Handler, options?: RouteOptions): this {
    return this.add('PATCH', pattern, handler, options);
  }

  delete(pattern: string, handler: Handler, options?: RouteOptions): this {
    return this.add('DELETE', pattern, handler, options);
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

      if (matched) {
        return {
          handler: route.handler,
          params,
          mutating: route.method !== 'GET' && route.method !== 'HEAD',
          allowInReadOnly: route.allowInReadOnly,
        };
      }
    }

    return undefined;
  }
}
