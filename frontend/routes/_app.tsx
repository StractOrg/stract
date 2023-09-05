import { AppProps } from "$fresh/server.ts";
import { injectGlobal } from "https://esm.sh/@twind/core@1.1.3";
import { DefaultCSP } from "../search/utils.ts";
import { ApiClient } from "../islands/ApiClient.tsx";
import { apiBaseFromEnv } from "../search/index.ts";

export default function App({ Component }: AppProps) {
  injectColorScheme();

  injectGlobal`
    html,
    body {
      position: relative;
      width: 100%;
      height: 100%;
    }

    a:hover {
      text-decoration: underline;
    }

    label {
      display: block;
    }

    pre > code {
      font-size: 0.8rem;
    }

    @media (scripting: none) {
      .script-none:hidden {
        @apply hidden;
      }
    }

    @media (scripting: initial-only) {
    }

    @media (scripting: enabled) {
    }
  `;

  return (
    <>
      <DefaultCSP />
      <ApiClient apiBase={apiBaseFromEnv()} />
      <html lang="en" class="h-full">
        <head>
          <meta charSet="UTF-8" />
          <meta
            name="viewport"
            content="width=device-width, initial-scale=1.0"
          />
          <meta name="referrer" content="strict-origin" />
          {/* <link rel="icon" type="image/svg+xml" href="/favicon.svg" />  */}
          <link rel="icon" type="image/x-icon" href="/favicon.ico" />
          <link
            rel="search"
            type="application/opensearchdescription+xml"
            title="Stract Search"
            href="/opensearch.xml"
          />
          <noscript
            dangerouslySetInnerHTML={{
              __html:
                `<style>.noscript\\:hidden{display:none!important;}</style>`,
            }}
          >
          </noscript>
          <title>Stract</title>
        </head>
        <body class="font-light h-full antialiased dark:bg-stone-900 dark:text-white">
          <Component />
        </body>
      </html>
    </>
  );
}

const injectColorScheme = () => {
  return injectGlobal`
  :root {
    --brand-50: 237 249 255;
    --brand-100: 215 239 255;
    --brand-200: 185 228 255;
    --brand-300: 136 213 255;
    --brand-400: 80 189 255;
    --brand-500: 40 156 255;
    --brand-600: 11 123 255;
    --brand-700: 10 102 235;
    --brand-800: 15 82 190;
    --brand-900: 19 72 149;
    --brand-950: 17 45 90;

    --contrast-50: 255 247 237;
    --contrast-100: 255 236 212;
    --contrast-200: 255 213 168;
    --contrast-300: 255 183 112;
    --contrast-400: 255 140 55;
    --contrast-500: 255 105 11;
    --contrast-600: 240 80 6;
    --contrast-700: 199 58 7;
    --contrast-800: 158 46 14;
    --contrast-900: 127 41 15;
    --contrast-950: 69 17 5;
  }
  `;
};
