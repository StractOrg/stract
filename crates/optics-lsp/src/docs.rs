pub fn token_docs(token: &optics::Token) -> Option<&'static str> {
    Some(match token {
        optics::Token::DiscardNonMatching => "All results that does not match any of the rules in the optic will be discarded.",

        optics::Token::Rule => "A rule specifies how a particular search result should be treated. \
        It consists of a `Matches` block and an optional `Action`. Any search result that matches the `Matches` block \
        will have the `Action` applied to it. The action can either `Boost`, `Downrank` or `Discard` a result. An empty `Action` is \
        equivalent to a `Boost` of 0.",

        optics::Token::RankingPipeline => "The final ranking consists of multiple stages in a pipeline. Each stage receives the
        best scoring webpages from the stage before it, and uses more accurate yet computationally expensive algorithms to rank
        the pages for the next stage.",

        optics::Token::Stage => "The final ranking consists of multiple stages in a pipeline. Each stage has different fields and
        signals available to it.",

        optics::Token::Matches => "`Matches` dictates the set of criteria a search result should match in order to have the action applied to it. \
        A search result must match all the parts of the `Matches` block in order to match the specific rule.",

        optics::Token::Site => "`Site(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the site of the result. \
        Note that when `Site` is used inside `Like` or `Dislike`, the pattern can only contain simple terms (no `*` and `|`). \n\n\
        When the site is used in a `Matches` block, you can use `*` as a wildcard term and `|` to indicate either the end or start of a string. \n\
        Consider the pattern `\"|sub.*.com|\"`. This will ensure that the terms `sub` and `.` must appear at the beggining of the site, then followed by any \
        domain-name that ends in `.` and `com`. Note that `|` can only be used in the beggining or end of a pattern and the pattern will only match full terms (no substring matching). \n\n\
        This example illustrates the difference between `Domain`, `Site` and `Url`:\n\
        Assume a search result has the url `https://sub.example.org/page`. the domain here is `example.org`, the site is `sub.example.org` and the url is the entire url (with protocol).\
        ",

        optics::Token::Url => "`Url(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the url of the result. \
        You can use `*` as a wildcard term and `|` to indicate either the end or start of a url. \n\
        Consider the pattern `\"https://sub.*.com|\"`. This will ensure that the terms `https`, `:`, `/`, `/`, `sub` and `.` must appear before any term that ends with `.` and `com` \
        in the url. Note that `|` can only be used in the beggining or end of a pattern and the pattern will only match full terms (no substring matching). \n\n\
        This example illustrates the difference between `Domain`, `Site` and `Url`:\n\
        Assume a search result has the url `https://sub.example.org/page`. the domain here is `example.org`, the site is `sub.example.org` and the url is the entire url (with protocol).\
        ",

        optics::Token::Domain => "`Domain(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the domain of the result. \
        You can use `*` as a wildcard term and `|` to indicate either the end or start of a domain. \n\
        Consider the pattern `\"example.org\"`. This is equivalent to doing a phrase search for `\"example.org\"` in the domain. Note that the pattern will only match full terms (no substring matching). \n\n\
        This example illustrates the difference between `Domain`, `Site` and `Url`:\n\
        Assume a search result has the url `https://sub.example.org/page`. the domain here is `example.org`, the site is `sub.example.org` and the url is the entire url (with protocol).\
        ",

        optics::Token::Title => "`Title(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the title of the web page. \
        You can use `*` as a wildcard term and `|` to indicate either the end or start of a title. \n\
        Consider the pattern `\"|Best * ever\"`. This will match any result where the title starts with `Best` followed by any term(s) and then followed by the term `ever`. \
        Note that the pattern will only match full terms (no substring matching) and the modifier `|` can only be used at the end or beggining of the pattern.",

        optics::Token::Description => "`Description(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the description of the web page. \
        You can use `*` as a wildcard term and `|` to indicate either the end or start of a description. \n\
        Consider the pattern `\"|Best * ever\"`. This will match any result where the description starts with `Best` followed by any term(s) and then followed by the term `ever`. \
        Note that the pattern will only match full terms (no substring matching) and the modifier `|` can only be used at the end or beggining of the pattern.",

        optics::Token::Content => "`Content(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the content of the web page. \
        The content of a webpage is all the text that is not part of navigational menues, footers etc. \n\
        You can use `*` as a wildcard term and `|` to indicate either the end or start of the content. \n\
        Consider the pattern `\"Best * ever\"`. This will match any result where the description starts with `Best` followed by any term(s) and then followed by the term `ever`. \
        Note that the pattern will only match full terms (no substring matching) and the modifier `|` can only be used at the end or beggining of the pattern.",

        optics::Token::MicroformatTag => "`MicroformatTag(\"...\")` matches any search result that contains the microformat tag defined in `\"...\"`. \
        This is useful when looking for indieweb pages.",

        optics::Token::Schema => "`Schema(\"...\")` matches any search result that contains the https://schema.org entity defined in `\"...\"`. \
        As an example, `Schema(\"BlogPosting\")` matches all pages that contains the https://schema.org/BlogPosting entity. Note that `Schema` \
        does not support the pattern syntax, but only simple strings.",

        optics::Token::Ranking => "When results are ranked we take a weighted sum of various signals to give each webpage a score for the specific query. \
        The top scored results are then presented to the user. `Ranking` allows you to alter the weight of all the `Signal`s and text `Field`s.",

        optics::Token::Signal => "During ranking of the search results, a number of signals are combined in a weighted sum to create the final score for each search result. \
        `Signal` allows you to change the coefficient used for each signal, and thereby alter the search result ranking. Some supported signals are e.g. \"host_centrality\", \"bm25\" and \"tracker_score\". \
        A complete list of the available signals can be found in the code (https://github.com/StractOrg/Stract/blob/main/src/ranking/signal.rs)",

        optics::Token::Field => "`Field` lets you change how the various text fields are prioritized during ranking (e.g. a search result matching text in the title is probably more relevant than a result where only the body matches). \
        Some supported fields are e.g. \"title\", \"body\", \"backlink_text\" and \"site\". \
        A complete list of available fields can be seen in the code (https://github.com/StractOrg/Stract/blob/main/src/schema.rs)",

        optics::Token::Action => "`Action` defines which action should be applied to the matching search result. The result can either be boosted, downranked or discarded.",

        optics::Token::Boost => "`Boost(...)` boosts the search result by the number specified in `...`.",

        optics::Token::Downrank => "`Downrank(...)` downranks the search result by the number specified in `...`. A higher number further downranks the search result.",

        optics::Token::Discard => "`Discard` discards the matching search result completely from the results page.",

        optics::Token::Like => "`Like(Site(...))` lets you like specific hosts. During ranking, we will calculate a centrality meassure from all you liked sites \
        so results that are heavily linked to from your liked sites will be favored. Note therefore, that `Like` not only alters the ranking of the specifc site, \
        but also sites that are heavily linked to from the liked site.",

        optics::Token::Dislike => "`Dislike(Site(...))` lets you dislike specifc hosts. During ranking, we will calculate a centrality meassure from all you dislike sites \
        so results that are heavily linked to from your disliked sites will be downranked. Note therefore, that `Dislike` not only alters the ranking of the specifc site, \
        but also sites that are heavily linked to from the disliked site.",

        _ => return None,
    })
}
