
const optics = JSON.parse(window.localStorage.getItem("optics") || "[]");

for (const optic of optics) {
    const option = document.createElement("option");

    option.value = optic.url;
    option.text = optic.name;
    option.setAttribute("title", optic.description);

    document.getElementById("optics-selector").appendChild(option);
}

var searchParams = new URLSearchParams(window.location.search);
const currentOpticUrl = searchParams.get("optic");

if (currentOpticUrl) {
    Array.from(document.getElementById("optics-selector").getElementsByTagName("option")).forEach((option) => {
        if (option.value === currentOpticUrl) {
            option.selected = true;
        }
    });
}