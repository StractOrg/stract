export type OpticOption = {
  name: string;
  url: string;
  description: string;
};

export type DefaultOpticOption = OpticOption & {
  shown: boolean;
};

export const opticKey = (optic: OpticOption) => optic.name + "_" + optic.url;


export const DEFAULT_OPTICS = [
  {
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/copycats_removal.optic',
    description: 'Remove common copycat websites from search results.',
    name: 'Copycats removal',
    shown: true,
  },
  {
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/hacker_news.optic',
    description: 'Only return results from websites that are popular on Hacker News.',
    name: 'Hacker News',
    shown: false,
  },
  {
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/discussions.optic',
    description: 'Only return results from forums or similar types of QA pages.',
    name: 'Discussions',
    shown: true,
  },
  {
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/10k_short.optic',
    description: 'Remove the top 10,000 most popular websites from search results.',
    name: '10K Short',
    shown: false,
  },
  {
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/indieweb_blogroll.optic',
    description:
      'Search only in the indieweb + a list of blogs from blogroll.org and some hand-picked blogs from hackernews.',
    name: 'Indieweb & blogroll',
    shown: true,
  },
  {
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/devdocs.optic',
    description:
      'Only return results from some of the developer documentation sites listed on devdocs.io. This is a non-exhaustive list.',
    name: 'Devdocs',
    shown: false,
  },
  {
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/academic.optic',
    description:
      'Search exclusively in academic sites (.edu, .ac.uk, arxiv.org etc.). This is a non-exhaustive list.',
    name: 'Academic',
    shown: true,
  },
] satisfies DefaultOpticOption[];

/**
 * Fetces the given `opticUrl` if allowed. The rules for which are allowed
 * should consider potentially malicious URLs such as `file://` or
 * internal/local IP addresses.
 */
export const fetchRemoteOptic = async (opts: { opticUrl: string; fetch?: typeof fetch }) => {
  if (opts.opticUrl.startsWith('file://')) return void 0;
  const response = await (opts.fetch ?? fetch)(opts.opticUrl);
  return await response.text();
};
