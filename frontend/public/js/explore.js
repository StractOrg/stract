const siteInput = document.querySelector("#site-input");
const siteInputContainer = document.querySelector("#site-input-container");
const siteInputError = document.querySelector("#site-input-error");
const origSiteInputBorderColor = siteInputContainer.style.borderColor;
const sitesList = document.querySelector("#sites-list");
const result = document.querySelector("#result");
const limit = document.querySelector("#limit");
const submitButton = document.querySelector("#add-site-btn");
const resultContainer = document.querySelector("#result-container");
const moreButton = document.querySelector("#more-btn");
const exportButton = document.querySelector("#export-optic");

const origMoreButtonColor = moreButton.style.color;

const API = "/beta/api/webgraph/similar_sites";
const KNOWS_SITE_API = "/beta/api/webgraph/knows_site";

let sites = [];
let similarSites = [];

siteInput.addEventListener("keyup", function (event) {
    if (event.key === "Enter") {
        if (siteInput.value === "") {
            updateSimilarSites();
            return;
        }

        addSiteIfKnown();
    }

    siteInputError.classList.add("hidden");
    siteInputContainer.style.borderColor = origSiteInputBorderColor;
});

limit.addEventListener("change", function () {
    if (sites.length > 0) updateSimilarSites();
    updateMoreButton();
});

moreButton.addEventListener("click", function () {
    if (limit.selectedIndex === limit.options.length - 1) return;

    limit.selectedIndex++;
    limit.value = limit.options[limit.selectedIndex].value;
    updateSimilarSites();
    updateMoreButton();
});

function updateMoreButton() {
    if (limit.selectedIndex === limit.options.length - 1) {
        moreButton.style.color = "rgba(0, 0, 0, 0.25)";
        moreButton.style.cursor = "default";
    } else {
        moreButton.style.color = origMoreButtonColor;
        moreButton.style.cursor = "pointer";
    }
}

document.addEventListener("click", function (event) {
    if (event.target instanceof HTMLElement) {
        if (event.target.closest(".remove-site") && event.target.dataset.index) {
            removeSite(parseInt(event.target.dataset.index));
        }
    }
});

function addSiteIfKnown() {
    const site = siteInput.value;

    fetch(KNOWS_SITE_API + "?site=" + site)
        .then(response => response.json())
        .then(data => {
            if (data["type"] === "known") {
                addSite(data["site"]);
                siteInput.value = "";
            } else {
                siteInputError.classList.remove("hidden");
                siteInputContainer.style.borderColor = "red";
            }
        });
}

function addSite(site) {
    sites.push(site);
    displayChosenSites();
    updateSimilarSites();
}

function removeSite(index) {
    sites.splice(index, 1);
    displayChosenSites();
    updateSimilarSites();
}

function displayChosenSites() {
    sitesList.innerHTML = "";

    sites.forEach((site, index) => {
        const siteDiv = document.createElement("div");
        siteDiv.classList.add("site");
        siteDiv.innerHTML = "<a target=\"_blank\" href=\"https://" + site + "\">" + site + "</a><span class=\"remove-site\" style=\"margin-left: 0.5rem; cursor: pointer;\" data-index=\"" + index + "\">×</span>";
        sitesList.appendChild(siteDiv);
    });
}

submitButton.addEventListener("click", function () {
    if (siteInput.value != "") {
        addSiteIfKnown();
    } else {
        updateSimilarSites();
    }
});

function updateSimilarSites() {
    if (sites.length === 0) {
        resultContainer.classList.add("hidden");
        result.innerHTML = "";
        return;
    };

    fetch(API, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            sites: sites,
            topN: parseInt(limit.value)
        })
    })
        .then(response => response.json())
        .then(data => {
            resultContainer.classList.remove("hidden");
            result.innerHTML = "";


            similarSites = data.map((res) => res.site);


            data.forEach((res) => {
                const addSiteDiv = document.createElement("div");
                addSiteDiv.style.width = "1rem";

                if (sites.includes(res.site)) {
                    addSiteDiv.innerHTML = "<img src=\"/images/disabled-add.svg\" />";
                } else {
                    addSiteDiv.innerHTML = "<img src=\"/images/add.svg\" />";
                    addSiteDiv.style.cursor = "pointer";

                    addSiteDiv.addEventListener("click", function () {
                        addSite(res.site);
                    });
                }

                result.appendChild(addSiteDiv);

                const scoreDiv = document.createElement("div");
                scoreDiv.innerHTML = res.score.toFixed(2);
                result.appendChild(scoreDiv);

                const desc = res.description === null ? "" : res.description;

                const siteDiv = document.createElement("div");
                siteDiv.innerHTML = "<a target=\"_blank\" class=\"underline\" href=\"http://" + res.site + "\">" + res.site + "</a>";
                siteDiv.innerHTML += "<p class=\"text-sm\">" + desc + "</p>";
                result.appendChild(siteDiv);
            });

            updateExportLink();
        });
}

function updateExportLink() {
    const data = {
        chosen_sites: sites,
        similar_sites: similarSites,
    };

    // @ts-ignore
    const compressed = LZString.compressToBase64(JSON.stringify(data));
    const url = window.location.origin + "/explore/export?data=" + compressed;
    exportButton.href = url;
}