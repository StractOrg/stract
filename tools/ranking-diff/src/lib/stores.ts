import { browser } from '$app/environment';
import { writable } from 'svelte/store';

const parseJSONWithFallback = <T>(json: string, fallback: T, message = '') => {
  try {
    return JSON.parse(json);
  } catch (e) {
    if (message) console.warn(message, { json });
    return fallback;
  }
};

const writableLocalStorage = <T>(
  key: string,
  defaultValue: T,
  storage = browser && localStorage,
) => {
  const storedValue = storage && storage.getItem(key);
  const store = writable<T>(
    typeof storedValue == 'string'
      ? parseJSONWithFallback(storedValue, defaultValue, `Failed to parse value stored in '${key}'`)
      : defaultValue,
  );

  store.subscribe(($value) => {
    if (browser && storage) {
      if (typeof $value == 'undefined') {
        storage.removeItem(key);
      } else {
        storage.setItem(key, JSON.stringify($value));
      }
    }
  });

  const { set } = store;
  store.set = (value: T) => {
    if (storage) {
      set(value);
    }
  };

  return store;
};

const SHUFFLE_EXPERIMENTS = 'shuffleExperiments';
export const shuffleExperimentsStore = writableLocalStorage<boolean>(SHUFFLE_EXPERIMENTS, true);

const SHOW_SIGNALS = 'showSignals';
export const showSignalsStore = writableLocalStorage<boolean>(SHOW_SIGNALS, false);

const SEARCH_API = 'searchApi';
export const searchApiStore = writableLocalStorage<string>(
  SEARCH_API,
  'http://localhost:3000/beta/api/search',
);
