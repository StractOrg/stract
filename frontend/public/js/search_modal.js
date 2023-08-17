var rankingModal = document.getElementById("modal");
var siteLabel = document.getElementById("modal-site-label");
var btnPreferMore = document.getElementById("prefer-more");
var btnPreferLess = document.getElementById("prefer-less");
var btnPreferBlock = document.getElementById("prefer-block");
var btnPreferBlock = document.getElementById("prefer-block");
var btnSummarize = document.getElementById("summarize-btn");

btnPreferMore.addEventListener("click", (event) => {
    preferMore();
});

btnPreferLess.addEventListener("click", (event) => {
    preferLess();
});

btnPreferBlock.addEventListener("click", (event) => {
    block();
});

btnSummarize.addEventListener("click", (event) => {
    summarizeBtn();
});

var currentSite = "";
var currentUrl = "";
var currentIdx = null;
var currentHasCleanBody = false;

const BLOCKED = 0;
const LESS = 1;
const MORE = 2;

let rankings = JSON.parse(
    window.localStorage.getItem("sites-ranking-adjustment") || "{}"
);

rankingModal.addEventListener("click", (event) => {
    event.stopPropagation();
});

// This is called from search.astro
// Do not delete!
function updateModal(idx, site, url, hasCleanBody) {
    siteLabel.innerHTML = site;
    currentSite = site;
    currentUrl = url;
    currentIdx = idx;
    currentHasCleanBody = hasCleanBody;

    btnSummarize.disabled = !currentHasCleanBody;

    updateSelection();
    updateSummaryBtn();
}

function updateSummaryBtn() {
    var idx = parseInt(currentIdx);
    var snip = snippets[idx];

    if (snip["summaryShown"]) {
        if (!btnSummarize.classList.contains("active")) {
            btnSummarize.classList.add("active");
        }
    } else {
        if (btnSummarize.classList.contains("active")) {
            btnSummarize.classList.remove("active");
        }
    }
}

function updateSelection() {
    let rankings = JSON.parse(
        window.localStorage.getItem("sites-ranking-adjustment") || "{}"
    );

    btnPreferMore.classList.remove("selected");
    btnPreferLess.classList.remove("selected");
    btnPreferBlock.classList.remove("selected");

    if (rankings[currentSite] != undefined) {
        let pref = rankings[currentSite];

        if (pref == MORE) {
            btnPreferMore.classList.add("selected");
        } else if (pref == LESS) {
            btnPreferLess.classList.add("selected");
        } else if (pref == BLOCKED) {
            btnPreferBlock.classList.add("selected");
        }
    }
}

function setSitePreference(pref) {
    if (rankings[currentSite] == pref) {
        delete rankings[currentSite];
    } else {
        rankings[currentSite] = pref;
    }

    window.localStorage.setItem(
        "sites-ranking-adjustment",
        JSON.stringify(rankings)
    );

    updateSelection();
    document.getElementById("searchbar-form").submit();
}

// called from button
function preferMore() {
    setSitePreference(MORE);
}

// called from button
function preferLess() {
    setSitePreference(LESS);
}

// called from button
function block() {
    setSitePreference(BLOCKED);
}

const allSnippetElements = document.getElementsByClassName("snippet");
var snippets = {};

for (let i = 0; i < allSnippetElements.length; i++) {
    const snip = allSnippetElements[i];
    const snipText = snip.querySelector(".snippet-text");
    if (snipText) {
        snippets[i] = {
            "origSnippet": snipText.innerHTML,
            "summaryShown": false,
            "summary": {
                "text": "",
                "hasStarted": false,
                "hasFinished": false,
            }
        };
    }
}

// called from button
function summarizeBtn() {
    if (currentIdx === null) {
        return;
    }

    var snippetElem = document.getElementById("snippet-" + currentIdx);

    if (snippetElem == null) {
        return;
    }
    var snippetText = snippetElem.querySelector(".snippet-text");

    var idx = parseInt(currentIdx);
    var snip = snippets[idx];

    snip["summaryShown"] = !snip["summaryShown"];
    if (snip["summaryShown"]) {
        btnSummarize.classList.add("active");
        summarize(idx, snippetText);
    } else {
        btnSummarize.classList.remove("active");

        snippetText.innerHTML = snip["origSnippet"];
        snippetText.classList.remove("blinking-cursor");
    }

}


function summarize(idx, snippetText) {
    var snip = snippets[idx];
    snippetText.innerHTML = snip['summary']["text"];

    var searchParams = new URLSearchParams(window.location.search);
    var query = searchParams.get("q");

    var reqData = {
        'query': query,
        'url': currentUrl,
    };

    var queryData = new URLSearchParams(reqData).toString();

    if (!snip["summary"]["hasFinished"]) {
        snippetText.classList.add("blinking-cursor");
    }

    if (!snip["summary"]["hasStarted"]) {
        snip["summary"]["hasStarted"] = true;

        var source = new EventSource("/beta/api/summarize?" + queryData);

        source.onmessage = function (event) {
            console.log(event);
            snip["summary"]["text"] += event.data;

            if (snip["summaryShown"]) {
                snippetText.innerHTML = snip["summary"]["text"];
            }

            if (event.data == "") {
                source.close();
                snip["summary"]["hasFinished"] = true;
                snippetText.classList.remove("blinking-cursor");
                return;
            }
        };

        // this is called when the connection is closed by the server
        source.onerror = function (event) {
            snip["summary"]["hasFinished"] = true;
            snippetText.classList.remove("blinking-cursor");
            source.close();
        };
    }
}