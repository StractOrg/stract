import { storageSignal } from "./utils.ts";

export const DEFAULT_OPTICS = [
  {
    url:
      "https://raw.githubusercontent.com/StractOrg/sample-optics/main/copycats_removal.optic",
    description: "Remove common copycat websites from search results.",
    name: "Copycats removal",
  },
  {
    url:
      "https://raw.githubusercontent.com/StractOrg/sample-optics/main/hacker_news.optic",
    description:
      "Only return results from websites that are popular on Hacker News.",
    name: "Hacker News",
  },
  {
    url:
      "https://raw.githubusercontent.com/StractOrg/sample-optics/main/discussions.optic",
    description:
      "Only return results from forums or similar types of QA pages.",
    name: "Discussions",
  },
  {
    url:
      "https://raw.githubusercontent.com/StractOrg/sample-optics/main/10k_short.optic",
    description:
      "Remove the top 10,000 most popular websites from search results.",
    name: "10K Short",
  },
  {
    url:
      "https://raw.githubusercontent.com/StractOrg/sample-optics/main/indieweb_blogroll.optic",
    description:
      "Search only in the indieweb + a list of blogs from blogroll.org and some hand-picked blogs from hackernews.",
    name: "Indieweb & blogroll",
  },
  {
    url:
      "https://raw.githubusercontent.com/StractOrg/sample-optics/main/devdocs.optic",
    description:
      "Only return results from some of the developer documentation sites listed on devdocs.io. This is a non-exhaustive list.",
    name: "Devdocs",
  },
  {
    url:
      "https://raw.githubusercontent.com/StractOrg/sample-optics/main/academic.optic",
    description:
      "Search exclusively in academic sites (.edu, .ac.uk, arxiv.org etc.). This is a non-exhaustive list.",
    name: "Academic",
  },
] satisfies OpticOption[];

export type OpticOption = { name: string; url: string; description: string };

const OPTICS_LOCAL_STORAGE_KEY = "optics";
export const opticsSignal = storageSignal<OpticOption[]>(
  OPTICS_LOCAL_STORAGE_KEY,
  [],
);
