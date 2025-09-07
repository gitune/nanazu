// ui.js

import { performSearch, getIndexData, loadIndexFiles } from './searchLogic.js';

// UI elements
export const SEARCH_FORM = document.getElementById('searchForm');
export const SEARCH_INPUT = document.getElementById('searchInput');
export const SEARCH_BUTTON = document.getElementById('searchButton');
export const POPUP_DIALOG = document.getElementById('popupDialog');
export const BACKDROP = document.getElementById('backdrop');
export const DIALOG_CONTENT = document.getElementById('dialogContent');
export const CLOSE_BUTTON = document.getElementById('closeDialogButton');

// Placeholder and status text
const INITIAL_PLACEHOLDER = '検索語を入力...';
const LOADING_PLACEHOLDER = 'インデックスを読み込み中...';
const NOT_FOUND_MESSAGE = `<h3 class="result-status">該当する文書は見つかりませんでした。</h3>
    <p>長い単語は分割して検索すると見つかるかも？ (「既読点復帰」→「既読 点 復帰」など)</p>`;

let abortController = null;

// --- UI Functions ---

export function showPopup() {
    POPUP_DIALOG.classList.remove('hidden');
    BACKDROP.classList.remove('hidden');
}

export function closePopup() {
    POPUP_DIALOG.classList.add('hidden');
    BACKDROP.classList.add('hidden');
    if (abortController) {
        abortController.abort();
        abortController = null;
    }
}

export function displaySearching() {
    DIALOG_CONTENT.innerHTML = '<h3>検索中...</h3>';
    showPopup();
}

export function displayNotFound() {
    DIALOG_CONTENT.innerHTML = NOT_FOUND_MESSAGE;
    showPopup();
}

export function displayInitialMessage() {
    DIALOG_CONTENT.innerHTML = '<h3>検索したい単語を入力してください。</h3>';
    showPopup();
}

export function updateSearchResults(resultsHtml) {
    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) {
        resultsContainer.insertAdjacentHTML('beforeend', resultsHtml);
    }
}

export function displayResultCount(hitCount) {
    let headingHtml = '';
    if (hitCount > 0) {
        headingHtml = `<h3 class="result-status">${hitCount}件見つかりました。</h3><div id="resultsContainer"></div>`;
    } else {
        headingHtml = NOT_FOUND_MESSAGE;
    }
    DIALOG_CONTENT.innerHTML = headingHtml;
    showPopup();
}

// --- Event Listeners ---

document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && !POPUP_DIALOG.classList.contains('hidden')) {
        closePopup();
    }
});

BACKDROP.addEventListener('click', () => closePopup());

CLOSE_BUTTON.addEventListener('click', (event) => {
    event.stopPropagation();
    closePopup();
});

SEARCH_INPUT.addEventListener('focus', () => {
    if (!getIndexData().isLoaded && !getIndexData().loadPromise) {
        SEARCH_INPUT.placeholder = LOADING_PLACEHOLDER;
        getIndexData().loadPromise = loadIndexFiles();
    }
    SEARCH_INPUT.select();
});

SEARCH_FORM.addEventListener('submit', async (e) => {
    e.preventDefault();
    const query = SEARCH_INPUT.value.trim();
    if (!query) {
        displayInitialMessage();
        return;
    }

    if (!getIndexData().isLoaded) {
        SEARCH_INPUT.placeholder = LOADING_PLACEHOLDER;
        await getIndexData().loadPromise;
    }

    displaySearching();
    abortController = new AbortController();
    await performSearch(query, abortController.signal);
    SEARCH_INPUT.blur();
});
