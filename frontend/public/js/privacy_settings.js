const allowStatsBox = document.getElementById("allowStats");
let allowStats = window.localStorage.getItem("allowStats");

if (allowStats === null) {
    allowStats = "true";
}

allowStatsBox.checked = allowStats === "true";

allowStatsBox.addEventListener("change", () => {
    if (allowStatsBox.checked) {
        window.localStorage.setItem("allowStats", "true");
    } else {
        window.localStorage.setItem("allowStats", "false");
    }
});