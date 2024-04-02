import { match } from 'ts-pattern';
import Result from '../routes/search/Result.svelte';
import type Searchbar from './components/Searchbar.svelte';
import type SpellCorrection from '../routes/search/SpellCorrection.svelte';

export const Keys = [
  'j',
  'd',
  'k',
  'h',
  'v',
  't',
  'm',
  'l',
  'o',
  's',
  "'",
  '/',
  'Control',
  'Enter',
  'ArrowUp',
  'ArrowDown',
  'Escape',
];
export type Key = (typeof Keys)[number];

// The type used to declare the key downs that will trigger the callback
export interface KeybindCallback {
  key: Key;
  callback: (context: Refs) => void;
  shift?: boolean;
  ctrl?: boolean;
  alt?: boolean;
}

/**
 * Used to create and interface with key bindings
 */
export class Keybind {
  // All of the keybindings to their callbacks
  public bindings: KeybindCallback[];
  private bindingEntries: Key[];

  /**
   * Constructor
   *
   * @param bindings An array of {@link KeybindCallback}s to be evaluated and utilized with {@link Keybind:onKeyDown}.
   */
  constructor(bindings: KeybindCallback[]) {
    this.bindings = bindings;
    this.bindingEntries = this.bindings.map((x) => x.key);
  }

  /**
   * Attempt to convert a string to a {@link Key}
   *
   * @param str - The string to attempt to convert
   * @returns undefined if unable to convert, otherwise corresponding {@link Key}
   */
  private keyFromString(str: string): Key | undefined {
    return Keys.find((key) => key === str);
  }

  /**
   * Handler for `keydown` events
   *
   * @remarks
   * Requires a wrapper function to pass in `useKeyboardShortcuts` boolean store.
   *
   * Will run the given callback if all requirements are met from an item in the
   * previously given {@link KeybindCallback} array.
   *
   * @param e - The `keydown` event of `KeyboardEvent` type
   * @param useKeyboardShortcuts - A boolean of the user's `useKeyboardShortcuts` preference
   */
  onKeyDown(e: KeyboardEvent, useKeyboardShortcuts: boolean, context: Refs) {
    if (!useKeyboardShortcuts) return;

    const key = this.keyFromString(e.key);

    if (!(key && this.bindingEntries.includes(key))) return;

    // Conditionals to be able to later compare to a potentially undefined binding.shift/.al/.ctrl
    const shift = e.shiftKey ? true : undefined;
    const ctrl = e.ctrlKey ? true : undefined;
    const alt = e.altKey ? true : undefined;

    const binding = this.bindings.find((binding) => binding.key === key);
    if (binding && binding.alt == alt && binding.shift == shift && binding.ctrl == ctrl) {
      e.preventDefault();
      binding.callback(context);
    }
  }
}

/*
Search callbacks
*/

export type Refs = {
  results?: Result[];
  searchbar?: Searchbar;
  spellCorrection?: SpellCorrection;
};

type Direction = 'up' | 'down';

/**
 * Utilized to bring the next/previous result into focus
 *
 * @remarks
 * Utilizes the element that is currently in focus and the given
 * direction to determine where and how to traverse the array of
 * results. Moves a single result per call.
 *
 * @param direction - How to maneuver through the result list (up/down), indicated by {@link Direction}
 */
const navigateResults = (direction: Direction, context: Refs) => {
  if (!context.results || context.results.length === 0) return;
  const results = context.results;
  console.log(results);

  let newIndex = 0;
  const currentResultIndex = results.findIndex((result) => result?.hasFocus());

  if (currentResultIndex > -1) {
    newIndex =
      currentResultIndex +
      match(direction)
        .with('up', () => -1)
        .with('down', () => 1)
        .exhaustive();

    if (newIndex < 0 || newIndex >= results.length) {
      newIndex = currentResultIndex;
    }
  }

  if (newIndex < results.length && results[newIndex]) {
    results[newIndex].getMainResultLink()?.focus();
  }
};

const focusNextResult = (context: Refs) => navigateResults('down', context);
const focusPrevResult = (context: Refs) => navigateResults('up', context);
const focusMainResult = (context: Refs) => {
  if (!context.results || context.results.length == 0) return;
  if (context.results[0]) context.results[0].getMainResultLink()?.focus();
};

const selectSearchBar = (context: Refs) => {
  scrollToTop(context);

  if (context.searchbar) {
    context.searchbar.select();
  }
};

/**
 * Scroll to the top of the window and reset focus
 */
const scrollToTop = (context: Refs) => {
  window.scrollTo({ top: 0, behavior: 'smooth' });

  if (!context.results) return;

  const focusedResult: Result | undefined = context.results.find((result) => result.hasFocus());
  focusedResult?.clearFocus();
};

/**
 * Redirect to the currently focused result
 */
const openResult = (context: Refs) => {
  if (!context.results) return;

  const focusedResult: Result | undefined = context.results.find((result) => result.hasFocus());
  focusedResult?.getMainResultLink()?.open();
};

/**
 * Open the currently focused result in a new tab
 *
 * @remarks
 * Requires pop-ups to be allowed for the window
 */
const openResultInNewTab = (context: Refs) => {
  if (!context.results) return;
  const focusedResult: Result | undefined = context.results.find((result) => result.hasFocus());
  focusedResult?.getMainResultLink()?.openInNewTab();
};

/**
 * Do a domain search using the domain of the currently focused result
 */
const domainSearch = (context: Refs) => {
  if (!context.results || !context.searchbar) return;

  const focusedResult: Result | undefined = context.results.find((result) => result.hasFocus());
  if (!focusedResult) return;

  const query = context.searchbar.userQuery();
  const domain = focusedResult.getMainResultLink()?.getUrl().hostname;
  const domainQuery = `site:${domain}`;

  // Only run the domain query if it isn't already in the query
  if (!query.includes(domainQuery)) context.searchbar.search(`${query} ${domainQuery}`);
};

const openSpellCorrection = (context: Refs) => {
  if (context.spellCorrection) context.spellCorrection.open();
};

const clearFocus = (context: Refs) => {
  const focusedResult: Result | undefined = context.results?.find((result) => result.hasFocus());
  focusedResult?.clearFocus();
};

// Packaged callbacks to keep imports clean
export const searchCb = {
  focusNextResult,
  focusPrevResult,
  focusMainResult,
  selectSearchBar,
  scrollToTop,
  openResult,
  openResultInNewTab,
  domainSearch,
  openSpellCorrection,
  clearFocus,
};
