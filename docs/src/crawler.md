# Crawler
[Information for webmasters here](https://stract.com/webmasters)

![Overview of Crawler Architecture](assets/images/crawler_overview.svg)
The crawler is a distributed system that scours the web. It has a coordinator process that determines which URLs to crawl and a set of worker processes that fetch the content of those URLs. Each worker receives a batch of crawl jobs to process, stores the fetched contents in an S3 bucket and sends newly discovered URLs back to the coordinator. This continues until the coordinator has determined that the crawl is complete.

## Coordinator
This is the brains of the crawl operation. The coordinator is responsible for determining which URLs to crawl and distributing them to the workers.

### URL Frontier
The coordinator starts with a list of seed urls, schedules these to the available workers and receives a list of newly discovered urls from each worker. These newly discovered urls are added to the url frontier, which is a list of urls to crawl.

You can imagine that the url frontier can grow very large, very quickly. This begs the question: How does the coordinator determine which urls to crawl next? We could just crawl the urls in the order they were discovered, but this might not lead to the most interesting results.

Instead, the coordinator assigns a score to each url and performs a weighted random selection of the next url to crawl.
Each domain starts with a score of 1.0 and is summed with the weight of all the ingoing links. Whenever a domain is sampled, it spreads its score to all urls that are linked to from the domain. Thus if a domain has many outgoing links, the weight for each of those links will be relatively low compared to if the domain only has a few outgoing links.

This prioritizes urls and domains that has many high-valued incoming links and are therefore more likely to be interesting for the user. The sampled urls are then scheduled to the available workers and the process repeats.

### Respectfullness
It is of utmost importance that we are respectful of the websites we crawl. We do not want to overload a website with requests and we do not want to crawl pages from the website that the website owner does not want us to crawl.

When a domain has been sampled it is therefore marked as `CrawlInProgress` until the worker sends results back to the coordinator for the job it was assigned. This ensures that each domain is only scheduled to a single worker at a time. It is then the responsibility of the worker to respect the `robots.txt` file of the domain and to not overload the domain with requests.

## Worker
The worker is quite simple and is responsible for fetching data from urls scheduled by the coordinator. It is completely stateless and stores the fetched data directly to an S3 bucket while sending newly discovered urls back to the coordinator.

When a worker is tasked to crawl a new site, it first checks the `robots.txt` file for the site to see which urls (if any) it is allowed to crawl.
If the worker receives a `429 Too Many Requests` response from the site, it backs off for a while before trying again. The specific backoff time depends on how fast the server responds. Further details can be found [here](https://stract.com/webmasters).
