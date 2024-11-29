export type OpticOption = {
  name: string;
  url: string;
  description: string;
};

export type DefaultOpticOption = OpticOption & {
  shown: boolean;
};

export const opticKey = (optic: OpticOption) => optic.name + '_' + optic.url;

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
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/fediverse.optic',
    description: 'Only search in fediverse sites.',
    name: 'Fediverse',
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
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/10k_short.optic',
    description: 'Remove the top 10,000 most popular websites from search results.',
    name: '10K Short',
    shown: false,
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
  {
    url: 'https://raw.githubusercontent.com/StractOrg/sample-optics/main/tilde.optic',
    description: 'Only search in urls that contain a tilde (~).',
    name: 'Tilde',
    shown: false,
  },
] satisfies DefaultOpticOption[];

const isPrivateIp4 = (url: string) => {
  const parts = url.split('://');
  const ip =
    parts.length > 0
      ? parts[parts.length - 1].replace(/\/$/, '').split(':')[0]
      : url.replace(/\/$/, '').split(':')[0];

  if (/^(10)\.(.*)\.(.*)\.(.*)$/.test(ip)) return true;
  if (/^(172)\.(1[6-9]|2[0-9]|3[0-1])\.(.*)\.(.*)$/.test(ip)) return true;
  if (/^(192)\.168\.(.*)\.(.*)$/.test(ip)) return true;
  if (/^(127)\.(0)\.(0)\.(1)$/.test(ip)) return true;
  if (/^(100)\.(6[4-9]|[7-9][0-9]|1[0-1][0-9]|12[0-7])\.(.*)\.(.*)$/.test(ip)) return true;

  return false;
};

const isPrivateIp6 = (url: string) => {
  const parts = url.split('://');
  const ip =
    parts.length > 0
      ? parts[parts.length - 1].replace(/\/$/, '').split(':')[0]
      : url.replace(/\/$/, '');

  if (/^fe80::/i.test(ip)) return true;
  if (/^fd[0-9a-f]{2}:/i.test(ip)) return true;

  return false;
};

const isPrivateIp = (url: string) => isPrivateIp4(url) || isPrivateIp6(url);

/**
 * Fetces the given `opticUrl` if allowed. The rules for which are allowed
 * should consider potentially malicious URLs such as `file://` or
 * internal/local IP addresses.
 */
export const fetchRemoteOptic = async (opts: { opticUrl: string; fetch?: typeof fetch }) => {
  if (opts.opticUrl.startsWith('file://')) return void 0;
  if (isPrivateIp(opts.opticUrl)) return void 0;

  const response = await (opts.fetch ?? fetch)(opts.opticUrl);

  if (isPrivateIp(response.url)) return void 0;

  return await response.text();
};
