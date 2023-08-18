const nameInput = document.getElementById("name");
const description = document.getElementById("description");
const url = document.getElementById("url");
const addBtn = document.getElementById("add-btn");
let deleteBtn = document.querySelectorAll(".delete-btn");
const originalOpticsList = document.getElementById("optics-list").cloneNode(true);

function updateDeleteButtons() {
    deleteBtn = document.querySelectorAll(".delete-btn");

    for (const btn of deleteBtn) {
        btn.addEventListener("click", (e) => {
            const index = e.target.dataset.opticIndex;
            optics.splice(index, 1);
            saveOptics();
            loadOptics();
        });
    }
}


addBtn.addEventListener('click', (_) => {
    if (nameInput.value != '' && url.value != '') {
        optics.push({ name: nameInput.value, description: description.value, url: url.value });
        nameInput.value = url.value = description.value = '';

        saveOptics();
        loadOptics();
    }
});

// {"name":string, "description":string, "url":string}[]
let optics = [];

loadOptics();

function loadOptics() {
    optics = JSON.parse(
        window.localStorage.getItem("optics") || "[]"
    );

    const opticsList = originalOpticsList.cloneNode(true);
    for (let i = 0; i < optics.length; i++) {
        const item = optics[i];
        const div = document.createElement("div");
        div.classList.add("flex");
        div.innerHTML = `
        <div style="width: 2.5rem;">
          <img
            src="/images/delete.svg"
            class="delete-btn"
            data-optic-index="${i}"
          />
        </div>
        <div class="col-data mr-5">${item.name}</div>
        <div class="col-data mr-5">${item.description}</div>
        <div class="col-data">
          <a href="${item.url}">Source</a>
        </div>
      `;
        opticsList.appendChild(div);
    }

    document.getElementById("optics-list").replaceWith(opticsList);

    updateDeleteButtons();
}

function saveOptics() {
    window.localStorage.setItem("optics", JSON.stringify(optics));
}