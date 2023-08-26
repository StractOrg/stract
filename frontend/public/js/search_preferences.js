const safeSearchOn = document.getElementById('safe-search-on');
const safeSearchOff = document.getElementById('safe-search-off');

let safeSearch = window.localStorage.getItem('safeSearch') === 'true';

safeSearchOn.checked = safeSearch;
safeSearchOff.checked = !safeSearch;

safeSearchOn.addEventListener('change', () => {
    safeSearch = true;
    window.localStorage.setItem('safeSearch', safeSearch);
});

safeSearchOff.addEventListener('change', () => {
    safeSearch = false;
    window.localStorage.setItem('safeSearch', safeSearch);
});

