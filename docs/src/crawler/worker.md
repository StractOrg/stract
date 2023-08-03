# Crawl Worker
The worker is quite simple and is responsible for fetching data from urls scheduled by the coordinator. It is completely stateless and stores the fetched data directly to an S3 bucket while sending newly discovered urls back to the coordinator.

When a worker is tasked to crawl a new site, it first checks the `robots.txt` file for the site to see which urls (if any) it is allowed to crawl.
If the worker receives a `429 Too Many Requests` response from the site, it backs off for a while before trying again. The specific backoff time depends on how fast the server responds. Further details can be found [here](https://trystract.com/webmasters).