# Stract Crawler

Stract is an [open source](https://github.com/StractOrg/stract/) web search engine. StractBot is the name of our crawler that collects pages from the web in order to build the index.
It is written in Rust and the source code can be found [here](https://github.com/StractOrg/stract/tree/main/crates/core/src/crawler).
The crawler uses the user agent `Mozilla/5.0 (compatible; StractBot/0.2; open source search engine; +https://stract.com/webmasters)`.

## Politeness

StractBot is a polite crawler. It respects the [robots.txt](https://en.wikipedia.org/wiki/Robots.txt) file of the website it is crawling and tries to not overload the server.

### Waiting Time Calculation

The crawler waits a certain amount of time between requests to the same domain. The waiting time is calculated by:
_min((2^politeness) \* max(fetchtime, 5 sec), 180 sec)_

Where:

- _politeness_ is the politeness factor (starting at 2)
- _fetchtime_ is the time it took to fetch the previous page

The crawler will wait at least 5 seconds between requests and at most 180 seconds.

### 429 Response Handling

If the crawler receives a 429 (Too Many Requests) response from the server:

1. The politeness factor is increased, doubling the time between each request.
2. This increased politeness factor persists for the duration of the crawl for that specific domain.
3. The crawler will never decrease the politeness factor for a domain that has returned a 429 response.

If the crawler hasn't received any 429 responses from a server, it may gradually decrease the politeness factor over time, but never below 0.

### Robots.txt

The crawler looks for the token StractBot in the [robots.txt](https://www.robotstxt.org/about.html) file to determine which pages (if any) it is allowed to crawl.

If you want to restrict access to part of your site, add the following to your robots.txt file

```
User-agent: StractBot
Disallow: /private
```

This will ensure that StractBot doesn't access any page with the '/private' prefix.
You can also restrict StractBot from accessing any page on your site

```
User-agent: StractBot
Disallow: /
```

The robots.txt file is cached for 1 hour, so changes to the file should be respected fairly quickly.

## Contact us

If you have any concerns or bad experiences with our crawler, please don't hesitate to reach out to us at [crawler@stract.com](mailto:crawler@stract.com). Chances are that others experience the same problems and we would love to fix them.
