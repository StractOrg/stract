# Supported search operators

* `A B C` - Searches for pages containing *all* of the terms A, B, or C. The terms are allowed to appear in different fields (url, title, body, etc.).

* `"A B C"` - Searches for pages containing the exact phrase "A B C" with the words in that specific order. The terms must appear in the same field.

* `site:example.com` - Restricts search results to only pages from the specified site example.com. It can also match subdomains `site:sub.example.com` or page prefixes `site:sub.example.com/page` which will both match `sub.example.com/page`, `sub.example.com/page/subpage`, `extra.sub.example.com/page` etc.

* `linkto:example.com` - Finds pages that contain links pointing to the specified domain example.com. Uses the same matching logic as `site:` queries.

* `intitle:A B` - Searches for pages where term A must appear in the title, while B can appear anywhere on the page.

* `inbody:A B` - Searches for pages where term A must appear in the main body text, while B can appear anywhere.

* `inurl:A B` - Searches for pages where term A must appear in the URL, while B can appear anywhere on the page.

* `exacturl:https://example.com` - Finds the page with exactly the specified URL. This is mostly used for debugging to see if a specific page is indexed.

* `A -B` - Searches for pages containing term A but not term B.
