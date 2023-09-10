import { Signal, signal, useSignalEffect } from "@preact/signals";
import { ContentSecurityPolicy, IS_BROWSER, useCSP } from "$fresh/runtime.ts";
import { RouteConfig } from "$fresh/server.ts";
import * as search from "../search/index.ts";

const shouldStoreSymbol = Symbol("should store signal");
const keySymbol = Symbol("should store signal");
export type StorageSignal<T> = Signal<
  { [shouldStoreSymbol]: boolean; [keySymbol]: string; data: T }
>;
export const updateStorageSignal = <T>(
  signal: StorageSignal<T>,
  f: (data: T) => T,
) => {
  signal.value = {
    [shouldStoreSymbol]: true,
    [keySymbol]: signal.value[keySymbol],
    data: f(signal.value.data),
  };
};
export const storageSignal = <T>(key: string, data: T): StorageSignal<T> => {
  const fromStorage = IS_BROWSER ? localStorage.getItem(key) : void 0;
  const s = signal({
    [shouldStoreSymbol]: false,
    [keySymbol]: key,
    data: typeof fromStorage == "string" ? JSON.parse(fromStorage) : data,
  });
  return s;
};
export const useSyncSignalWithLocalStorage = <T>(
  signal: StorageSignal<T>,
) =>
  useSignalEffect(() => {
    if (!signal.value[shouldStoreSymbol]) {
      const fromStorage = localStorage.getItem(signal.value[keySymbol]);
      if (typeof fromStorage == "string") {
        signal.value = {
          [shouldStoreSymbol]: true,
          [keySymbol]: signal.value[keySymbol],
          data: JSON.parse(fromStorage),
        };
      }
    }
    localStorage.setItem(
      signal.value[keySymbol],
      JSON.stringify(signal.value.data),
    );
  });

export const DEFAULT_ROUTE_CONFIG: RouteConfig = {
  csp: true,
};

export const useDefaultCSP = (
  mutator?: (csp: ContentSecurityPolicy) => void,
) => {
  useCSP((csp) => {
    csp.directives.defaultSrc = ["'self'"];
    (csp.directives.scriptSrc ??= []).push("'self'");
    (csp.directives.connectSrc ??= []).push(search.API_BASE.value);
    (csp.directives.imgSrc ??= []).push("'self'", "data:");

    mutator?.(csp);
  });
};

/**
 * A component for setting the default CSP especially useful in async routes
 * where {@link useDefaultCSP} doesn't work.
 */
export const DefaultCSP = (
  { mutator }: { mutator?: (csp: ContentSecurityPolicy) => void },
) => {
  useDefaultCSP(mutator);
  return null;
};
