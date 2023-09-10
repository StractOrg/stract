import { AppProps } from "$fresh/server.ts";
import { DefaultCSP } from "../search/utils.ts";
import { ApiClient } from "../islands/ApiClient.tsx";
import { apiBaseFromEnv } from "../search/index.ts";

export default function App({ Component }: AppProps) {
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
        <body style={{ height: "100%" }}>
          <Component />
        </body>
      </html>
    </>
  );
}
