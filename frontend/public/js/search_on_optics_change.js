if (search_on_change) {
    document.querySelector("#optics-selector").addEventListener("change", (_) => {
        document.getElementById("searchbar-form").submit();
    });
}