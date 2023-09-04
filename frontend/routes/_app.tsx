import { AppProps } from "$fresh/server.ts";
import { injectGlobal } from "https://esm.sh/@twind/core@1.1.3";
import { DefaultCSP } from "../search/utils.ts";

export default function App({ Component }: AppProps) {
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

    input:disabled {
      color: #ccc;
    }

    button {
      color: #333;
      background-color: #f4f4f4;
      outline: none;
    }

    button:disabled {
      color: #999;
    }

    button:not(:disabled):active {
      background-color: #ddd;
    }

    button:focus {
      border-color: #666;
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
        <body class="font-light h-full antialiased">
          <Component />
        </body>
      </html>
    </>
  );
}
