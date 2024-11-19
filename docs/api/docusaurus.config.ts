import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import { ScalarOptions } from '@scalar/docusaurus';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Stract',
  tagline: 'Web search done right',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://docs.stract.com',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'stractorg', // Usually your GitHub org/user name.
  projectName: 'stract', // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          // Useful options to enforce blogging best practices
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/logo.svg',
    navbar: {
      // title: '',
      logo: {
        alt: 'Stract Logo',
        src: 'img/logo.svg',
      },
      items: [
        // {
        //   type: 'docSidebar',
        //   sidebarId: 'tutorialSidebar',
        //   position: 'left',
        //   label: 'Tutorial',
        // },
        // {to: '/blog', label: 'Blog', position: 'left'},
      ],
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
    colorMode: {
      defaultMode: 'dark',
    },
  } satisfies Preset.ThemeConfig,

  plugins: [
    [
      '@scalar/docusaurus',
      {
        label: 'API Reference',
        route: '/api',
        configuration: {
          spec: {
            url: 'https://stract.com/beta/api/docs/openapi.json',
            // url: 'http://localhost:3000/beta/api/docs/openapi.json',
          },
          layout: 'modern',
          hideModels: true,
          baseServerURL: 'https://stract.com',
          servers: [{ url: 'https://stract.com', description: 'Production server' }],
          theme: 'kepler',
          defaultOpenAllTags: true,
        },
      } as ScalarOptions,
    ],
  ],
};

export default config;
