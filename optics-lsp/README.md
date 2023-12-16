# Optics VS Code Extension
This extension makes it easier to edit `.optic` files. An optic is used by [Stract](https://stract.com) and gives you complete control over which search results gets returned and how they are ranked. This extension includes syntax highlighting for the optics language and an LSP to detect syntax errors.

## Sample `.optic` syntax
This small sample hopefully gives you a quick introduction to the optics language.

```
// Most optics contains a sequence of rules that applies an action to matching search results
// This rule mathes all urls that contains the term "/forum" and boosts their score by 3
Rule {
    Matches {
        Url("/forum")
    },
    Action(Boost(3))
};

// you can also downrank results
Rule {
    Matches {
        Site("w3schools.com")
    },
    Action(Downrank(3))
};

// or completely discard the result from your search results
Rule {
    Matches {
        Site("gitmemory.com")
    },
    Action(Discard)
};
```

See our [quickstart](https://github.com/StractOrg/sample-optics/blob/main/quickstart.optic) for a more thorough and up-to-date walk through.