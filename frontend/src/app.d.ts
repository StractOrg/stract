import 'unplugin-icons/types/svelte';

// See https://kit.svelte.dev/docs/types#app
// for information about these interfaces
declare global {
  namespace App {
    // interface Error {}
    interface Locals {
      form?: FormData;
    }
    interface PageData {
      globals?: {
        title?: string;
        header?: {
          /** Hide tne logo from the header */
          hideLogo?: boolean;
          /** Show the divider between the header and the content */
          divider?: boolean;
        };
        highlightjs?: { HighlightAuto: typeof import('svelte-highlight').HighlightAuto };
      };
    }
    // interface Platform {}
  }
}

export {};
