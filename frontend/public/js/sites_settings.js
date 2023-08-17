let sites = JSON.parse(
    window.localStorage.getItem("sites-ranking-adjustment") || "{}"
);

const likedList = document.getElementById("liked-list");
const dislikedList = document.getElementById("disliked-list");
const blockedList = document.getElementById("blocked-list");
const exportButton = document.getElementById("export-optic");
var deleteSiteButtons = document.querySelectorAll(".delete-site");

deleteSiteButtons.forEach((btn) => {
    btn.addEventListener("click", (event) => {
        deleteRow(btn);
    });
});

const BLOCKED = 0;
const LESS = 1;
const MORE = 2;

function addMoreRow(site) {
    addRow(site, likedList);
}

function addLessRow(site) {
    addRow(site, dislikedList);
}

function addBlockedRow(site) {
    addRow(site, blockedList);
}

function addRow(site, list) {
    let img = document.createElement("div");
    img.classList.add("site");

    img.innerHTML =
        '<img src="/images/delete.svg" class="w-5 h-5 hover:cursor-pointer delete-site" id="' +
        site +
        '"/>';

    let name = document.createElement("div");
    name.innerHTML = site;
    name.classList.add("site");
    name.classList.add("text-sm");

    list.appendChild(img);
    list.appendChild(name);


    deleteSiteButtons = document.querySelectorAll(".delete-site");

    deleteSiteButtons.forEach((btn) => {
        btn.addEventListener("click", (event) => {
            deleteRow(btn);
        });
    });
}

function updateList() {
    sites = JSON.parse(
        window.localStorage.getItem("sites-ranking-adjustment") || "{}"
    );

    clearList();

    for (const [site, ranking] of Object.entries(sites)) {
        if (ranking == MORE) {
            addMoreRow(site);
        } else if (ranking == LESS) {
            addLessRow(site);
        } else if (ranking == BLOCKED) {
            addBlockedRow(site);
        }
    }
}

function clearList() {
    document.querySelectorAll(".site").forEach((site) => site.remove());
}

function deleteRow(deleteBtn) {
    let site = deleteBtn.id;
    sites = JSON.parse(
        window.localStorage.getItem("sites-ranking-adjustment") || "{}"
    );

    if (sites[site] != undefined) {
        delete sites[site];
        window.localStorage.setItem(
            "sites-ranking-adjustment",
            JSON.stringify(sites)
        );
    }

    updateList();
}

exportButton.addEventListener("click", () => {
    sites = {}

    window.localStorage.setItem(
        "sites-ranking-adjustment",
        JSON.stringify(sites)
    );

    updateList();
});

function updateExportLink() {
    console.log(sites);
    if (sites.length == 0) {
        exportButton.href = "#";
        return;
    }

    let data = {
        liked: [],
        disliked: [],
        blocked: [],
    };

    for (const site in sites) {
        if (sites[site] == MORE) {
            data.liked.push(site);
        } else if (sites[site] == LESS) {
            data.disliked.push(site);
        } else if (sites[site] == BLOCKED) {
            data.blocked.push(site);
        }
    }

    // @ts-ignore
    const compressed = LZString.compressToBase64(JSON.stringify(data));
    const url = window.location + "/export?data=" + compressed;
    exportButton.href = url;
}

updateList();
updateExportLink();
