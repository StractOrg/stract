/**
 * Enum to represent the key strings provided by {@link KeyboardEvent}
 */
export enum Keys {
  J = 'j',
  D = 'd',
  K = 'k',
  H = 'h',
  V = 'v',
  T = 't',
  M = 'm',
  L = 'l',
  O = 'o',
  S = 's',
  SINGLE_QUOTE = "'",
  FORWARD_SLASH = '/',
  CTRL = 'Control',
  ENTER = 'Enter',
  ARROW_UP = 'ArrowUp',
  ARROW_DOWN = 'ArrowDown',
}

// The type used to declare the key downs that will trigger the callback
export interface KeybindCallback {
  key: Keys;
  callback: (e: KeyboardEvent) => void;
  shift?: boolean;
  ctrl?: boolean;
  alt?: boolean;
}

/**
 * Used to create and interface with key bindings
 */
export class Keybind {
  // All of the keybindings to their callbacks
  private bindings: KeybindCallback[];
  private bindingEntries: Keys[];

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
  private keyEnumFromString(str: string): Keys | undefined {
    for (const [_, key] of Object.entries(Keys)) {
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
  onKeyDown(e: KeyboardEvent, useKeyboardShortcuts: boolean) {
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
      binding.callback(e);
    }
  }
}

/*
Search callbacks
*/

/**
 * Used to indicate what direction {@link navigateResults} should go (up/down)
 */
type Direction = 1 | -1;

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
const navigateResults = (direction: Direction) => {
  const currentActive = document.activeElement;

  const results = [...document.getElementsByClassName('result')] as HTMLElement[];
  let currentResultIndex: number | null = null;
  let newIndex = 0;

  results.forEach((el, index) => {
    if (el.contains(currentActive)) currentResultIndex = index;
  });

  if (currentResultIndex != null) {
    newIndex = currentResultIndex + direction;
    if (0 > newIndex || newIndex >= results.length) {
      newIndex = currentResultIndex;
    }
  }

  const nextResult = results[newIndex];

  // Get the anchor element and focus it
  (nextResult.getElementsByClassName('result-main-link')[0] as HTMLElement)?.focus();
};

/**
 * Wrapper for {@link navigateResults} that will focus on the next result
 */
const focusNextResult = () => navigateResults(1);

/**
 * Wrapper for {@link navigateResults} that will focus on the previous result
 */
const focusPrevResult = () => navigateResults(-1);

/**
 * Focus the first result (if any)
 */
const focusMainResult = () => {
  const results = document.getElementsByClassName('result-main-link');
  if (results.length > 0) {
    (results[0] as HTMLElement).focus();
  }
};

/**
 * Focus the search bar
 */
const focusSearchBar = () => {
  (document.getElementById('searchbar') as HTMLInputElement)?.select();
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
 * @param e - The triggering {@link KeyboardEvent}
 */
const openResult = (e: KeyboardEvent) => {
  if ((e.target as HTMLElement).className.includes('result-main-link')) {
    const resultLink = e.target as HTMLAnchorElement;
    window.location.href = resultLink.href;
  } else {
    const results = document.getElementsByClassName('result-main-link');
    if (results.length > 0) {
      window.location.href = (results[0] as HTMLAnchorElement).href;
    }
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
const openResultInNewTab = (e: KeyboardEvent) => {
  // Ensure target event is a result url
  if ((e.target as HTMLElement).className.includes('result-main-link')) {
    const resultLink = e.target as HTMLAnchorElement;
    window.open(resultLink.href, '_blank', 'noopener');
  }
};

/**
 * Do a domain search using the domain of the currently focused result
 *
 * @param e - The triggering {@link KeyboardEvent}
 */
const domainSearch = (e: KeyboardEvent) => {
  // Ensure target event is a result url
  if ((e.target as HTMLElement).className.includes('result-main-link')) {
    const searchBar = document.getElementById('searchbar') as HTMLInputElement;
    if (searchBar) {
      // Parse the result url, pull out the hostname then resubmit search
      const resultLink = new URL((e.target as HTMLAnchorElement)?.href);
      searchBar.value += ` site:${resultLink.hostname}`;
      searchBar.form?.submit();
    }
  }
};

/**
 * Redirect to the `Did you mean:` link (if exists)
 */
const goToMisspellLink = () => {
  const msLink = document.getElementById('misspell-link');
  if (msLink) window.location.href = (msLink as HTMLAnchorElement).href;
};

// Packaged callbacks to keep imports clean
export const searchCb = {
  focusNextResult,
  focusPrevResult,
  focusMainResult,
  focusSearchBar,
  scrollToTop,
  openResult,
  openResultInNewTab,
  domainSearch,
  goToMisspellLink,
};
