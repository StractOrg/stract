import { browser } from '$app/environment';

export const globals = async (
  globals?: App.PageData['globals'],
): Promise<App.PageData['globals']> => ({
  highlightjs: browser
    ? void 0
    : { HighlightAuto: (await import('svelte-highlight')).HighlightAuto },
  ...globals,
});
