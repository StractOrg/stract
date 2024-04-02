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
];
export type Key = (typeof Keys)[number];

// The type used to declare the key downs that will trigger the callback
export interface KeybindCallback {
  key: Key;
  callback: (e: KeyboardEvent, context: Refs) => void;
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
   * Attempt to convert a string to {@link Keys} enum
   *
   * @param str - The string to attempt to convert
   * @returns undefined if unable to convert, otherwise {@link Keys}
   */
  private keyEnumFromString(str: string): Key | undefined {
    for (const key of Keys) {
      if (key == str) return key;
    }

    return undefined;
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
    if (!useKeyboardShortcuts || e.repeat) return;

    const enum_key = this.keyEnumFromString(e.key);

    if (!(enum_key && this.bindingEntries.includes(enum_key))) return;

    // Conditionals to be able to later compare to a potentially undefined binding.shift/.al/.ctrl
    const shift = e.shiftKey ? true : undefined;
    const ctrl = e.ctrlKey ? true : undefined;
    const alt = e.altKey ? true : undefined;

    const binding = this.bindings.find((binding) => binding.key === enum_key);
    if (binding && binding.alt == alt && binding.shift == shift && binding.ctrl == ctrl) {
      e.preventDefault();
      binding.callback(e, context);
    }
  }
}

/*
Search callbacks
*/

export type Refs = {
  results: Result[];
  searchbar: Searchbar;
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
  const currentActive = document.activeElement;
  const results = context.results;

  let newIndex = 0;
  const currentResultIndex = results.findIndex((el) => el.getMainDiv().contains(currentActive));

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

  results[newIndex].getMainResultLink().focus();
};

const focusNextResult = (_e: KeyboardEvent, context: Refs) => navigateResults('down', context);

const focusPrevResult = (_e: KeyboardEvent, context: Refs) => navigateResults('up', context);

const focusMainResult = (_e: KeyboardEvent, context: Refs) => {
  if (context.results[0]) context.results[0].getMainResultLink().focus();
};

const selectSearchBar = (_e: KeyboardEvent, context: Refs) => {
  context.searchbar.select();
};

/**
 * Scroll to the top of the window and reset focus
 */
const scrollToTop = () => {
  window.scrollTo({ top: 0, behavior: 'smooth' });
  if (document.activeElement instanceof HTMLElement) {
    document.activeElement.blur(); // reset focus
  }
};

/**
 * Redirect to the currently focused result
 *
 * @param _e - The triggering {@link KeyboardEvent}
 */
const openResult = (e: KeyboardEvent, context: Refs) => {
  if (e.target && e.target instanceof HTMLElement) {
    const activeElement = e.target as HTMLElement;
    const currentResultIndex = context.results.findIndex((el) =>
      el.getMainDiv().contains(activeElement),
    );
    if (currentResultIndex > -1) context.results[currentResultIndex].getMainResultLink().open();
  }
};

/**
 * Open the currently focused result in a new tab
 *
 * @remarks
 * Requires pop-ups to be allowed for the window
 *
 * @param e - The triggering {@link KeyboardEvent}
 */
const openResultInNewTab = (e: KeyboardEvent, context: Refs) => {
  if (e.target && e.target instanceof HTMLElement) {
    const activeElement = e.target as HTMLElement;
    const currentResultIndex = context.results.findIndex((el) =>
      el.getMainDiv().contains(activeElement),
    );
    if (currentResultIndex > -1)
      context.results[currentResultIndex].getMainResultLink().openInNewTab();
  }
};

/**
 * Do a domain search using the domain of the currently focused result
 *
 * @param e - The triggering {@link KeyboardEvent}
 */
const domainSearch = (e: KeyboardEvent, context: Refs) => {
  if (e.target && e.target instanceof HTMLElement) {
    const activeElement = e.target as HTMLElement;
    const currentResultIndex = context.results.findIndex((el) =>
      el.getMainDiv().contains(activeElement),
    );
    if (currentResultIndex > -1) {
      const focusedResult = context.results[currentResultIndex];
      const query = context.searchbar.userQuery();
      const domain = focusedResult.getMainResultLink().getUrl().hostname;
      const domainQuery = `site:${domain}`;

      // Only run the domain query if it isn't already in the query
      if (!query.includes(domainQuery)) context.searchbar.search(`${query} ${domainQuery}`);
    }
  }
};

const openSpellCorrection = (_e: KeyboardEvent, context: Refs) => {
  if (context.spellCorrection) context.spellCorrection.open();
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
};
