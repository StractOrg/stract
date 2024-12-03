# Crawler
[Information for webmasters here](https://stract.com/webmasters)

The crawler is a distributed system that scours the web. It has a coordinator process that determines which URLs to crawl and a set of worker processes that fetch the content of those URLs. Each worker receives a batch of crawl jobs to process, stores the fetched contents in an S3 bucket and retrieves a new batch of jobs to process. This continues until the coordinator has determined that the crawl is complete.

Each crawl job contains a site, a crawl budget and a list of some known high-authority urls for that site. The crawl budget is used to determine how many pages to fetch from the site. Each site is only allowed to be crawled by a single worker at a time to ensure that we don't overload a website.

## Coordinator
The coordinator is responsible for planning and orchestrating the crawl process. It analyzes data from previous crawls to determine an appropriate crawl budget for each website. This budget helps ensure fair resource allocation and prevents overloading any single site.

Based on this analysis, the coordinator creates a crawl plan that takes the form of a queue of jobs to be processed. This approach allows for efficient distribution to worker nodes while ensuring the coordinator does not become a bottleneck.

### Respectfullness
It is of utmost importance that we are respectful of the websites we crawl. We do not want to overload a website with requests and we do not want to crawl pages from the website that the website owner does not want us to crawl.

To ensure this, the jobs are oriented by site so each site is only included in a single job. When a site gets scheduled to a worker it is then the responsibility of the worker to respect the `robots.txt` file of the domain and to not overload the domain with requests. For more details see the [webmasters](https://stract.com/webmasters) documentation.

## Worker
The worker is responsible for crawling the sites scheduled by the coordinator. It is completely stateless and stores the fetched data directly to an S3 bucket. It recursively discovers new urls on the assigned site and crawls them until the crawl budget is exhausted.

When a worker is tasked to crawl a new site, it first checks the `robots.txt` file for the site to see which urls (if any) it is allowed to crawl.
If the worker receives a `429 Too Many Requests` response from the site, it backs off for a while before trying again. The specific backoff time depends on how fast the server responds.
