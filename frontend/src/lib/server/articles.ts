import { readFile } from 'node:fs/promises';
import { Marked } from 'marked';

export const loadMarkdown = async (path: string) => {
  const m = new Marked();
  const md = await m.parse(await readFile(path, 'utf-8'));
  return { md };
};
