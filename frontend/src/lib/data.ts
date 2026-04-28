// Data-fetching helpers + a small useFetch hook. Every endpoint maps to
// a static JSON file under /static/data/, so "fetching" is basically a
// CDN read. The hook caches by URL for the lifetime of the page so
// repeat navigations don't re-download.
import { useEffect, useState } from 'react';
import type { Window } from '../types';

const cache = new Map<string, unknown>();

export function dataUrl(endpoint: string, window?: Window): string {
  return window
    ? `/static/data/${endpoint}-${window}.json`
    : `/static/data/${endpoint}.json`;
}

export async function fetchData<T>(url: string): Promise<T> {
  if (cache.has(url)) return cache.get(url) as T;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`fetch ${url}: ${resp.status}`);
  const data = (await resp.json()) as T;
  cache.set(url, data);
  return data;
}

export type FetchState<T> =
  | { status: 'loading' }
  | { status: 'ready'; data: T }
  | { status: 'error'; error: Error };

export function useData<T>(url: string): FetchState<T> {
  const [state, setState] = useState<FetchState<T>>(
    cache.has(url)
      ? { status: 'ready', data: cache.get(url) as T }
      : { status: 'loading' }
  );

  useEffect(() => {
    let cancelled = false;
    setState(
      cache.has(url)
        ? { status: 'ready', data: cache.get(url) as T }
        : { status: 'loading' }
    );
    fetchData<T>(url)
      .then((data) => {
        if (!cancelled) setState({ status: 'ready', data });
      })
      .catch((error: Error) => {
        if (!cancelled) setState({ status: 'error', error });
      });
    return () => {
      cancelled = true;
    };
  }, [url]);

  return state;
}
