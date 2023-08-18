var rankingAdjustButtons = document.querySelectorAll(".adjust-btn");
var titleLinks = document.querySelectorAll(".sr-title-link");
var modal = document.getElementById("modal");
var alertCrosses = document.querySelectorAll(".alert-cross");
var improvementButtons = document.querySelectorAll(".improvement-on-click");

document.querySelector("#region-selector").addEventListener("change", (_) => {
    document.getElementById('searchbar-form').submit();
});

improvementButtons.forEach((btn) => {
    btn.addEventListener("click", (event) => {
        improvement(btn.dataset.idx);
    });
});

alertCrosses.forEach((cross) => {
    cross.addEventListener("click", (event) => {
        this.parentElement.style.display = 'none';
    });
});

var searchParams = new URLSearchParams(window.location.search);
var query = searchParams.get("q");

let allowStats = window.localStorage.getItem("allowStats") === "true";

if (allowStats === null) {
    allowStats = "true";
}


var qid = null;

var improvementSent = new Set();

var urls = [];

for (const elem of titleLinks) {
    urls.push(elem.href);
}

if (allowStats) {
    fetch("/improvement/store", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            "query": query,
            "urls": urls
        })
    }).then((response) => response.text()).then((data) => {
        qid = data;
    });
}

document.addEventListener("click", (elem) => {
    modal.classList.remove("modal-open");
    modal.classList.add("modal-closed");
});

var rankingModalHeight = modal.clientHeight;

var prevButtonClickIdx = -1;

rankingAdjustButtons.forEach((btn) => {
    btn.addEventListener("click", (event) => {
        event.stopPropagation();

        const elem = event.target;
        const hasCleanBody = (elem.dataset.hasCleanBody === "true");

        updateModal(elem.dataset.idx, elem.dataset.site, elem.dataset.url, hasCleanBody);

        const rect = elem.getBoundingClientRect();
        modal.style.left = rect.left + rect.width + 5 + "px";
        modal.style.top =
            rect.top +
            document.documentElement.scrollTop -
            rankingModalHeight / 2 +
            "px";

        setTimeout(() => {
            if (prevButtonClickIdx == elem.dataset.idx && modal.classList.contains("modal-open")) {
                modal.classList.add("modal-closed");
                modal.classList.remove("modal-open");
            } else {
                modal.classList.remove("modal-closed");
                modal.classList.add("modal-open");
            }

            prevButtonClickIdx = elem.dataset.idx;
        }, 0);
    });
});

function improvement(idx) {
    if (allowStats && qid && !improvementSent.has(idx)) {
        window.navigator.sendBeacon("/improvement/click?qid=" + qid + "&click=" + idx);
        improvementSent.add(idx);
    }
}