<p align="center">
  <img src="git-assets/big-logo.svg" height="120" />
</p>
<br />

Stract is an open source web search engine hosted at [trystract.com](https://trystract.com/) targeted towards tinkerers and developers.

<br />
<p align="center">
  <img src="git-assets/screenshot.png" width="80%" />
</p>
<br />
<br />

# 💡 Features
* Keyword search that respects your search query.
* Advanced query syntax (`site:`, `intitle:` etc.).
* Rank webpages based on their [harmonic centrality](https://en.wikipedia.org/wiki/Centrality#Harmonic_centrality)
* DDG-style [!bang syntax](https://duckduckgo.com/bang)
* Entity sidebar
* De-rank websites with third-party trackers
* Prioritize fresh content
* Regional search
* Use [optics](https://github.com/StractOrg/sample-optics/blob/main/quickstart.optic) to almost endlessly customize your search results.
  * Customize how signals are combined during search for the final search result
* Prioritize links (centrality) from the sites you trust.

# 👩‍💻 Setup
We recommend everyone to use the hosted version at [trystract.com](https://trystract.com/), but you can also follow the steps outlined in [CONTRIBUTING.md](CONTRIBUTING.md) to setup the engine locally.

# ‍💼 License
Stract is offered under the terms defined under the [LICENSE.md](LICENSE.md) file.

# 📬 Contact
You can contact us at [hello@trystract.com](mailto:hello@trystract.com) or in our [Discord server](https://discord.gg/BmzKHffWJM).

# 🏆 Thank you!
We truly stand on the shoulders of giants and this project would not have been even remotely feasible without them. An especially huge thank you to
* The authors and contributors of Tantivy for providing the inverted index library on which Stract is built.
* The commoncrawl organization for crawling the web and making the dataset readily available.