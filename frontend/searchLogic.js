// searchLogic.js

import { displaySearching, displayNotFound, updateSearchResults, displayResultCount, closePopup, DIALOG_CONTENT, SEARCH_INPUT, SEARCH_BUTTON } from './ui.js';

// Index file path
const INDEX_FILE = './nnz.all.idx';
const HEADER_SIZE = 4 + 4 * 6; // Magic Number + 6 file offsets
const POSTING_LIST_RECORD_SIZE = 8;
const DOC_META_RECORD_SIZE = 26;

// Global data store for the index
const indexData = {
    dictKeys: [],
    dictOffsets: [],
    docMeta: null,
    idxOffset: null,
    docDataOffset: null,
    totalDocs: 0,
    isLoaded: false,
    loadPromise: null,
};

// Gzip decompression utility
async function decompress(buffer) {
    const ds = new DecompressionStream('gzip');
    const stream = new ReadableStream({
        start(controller) {
            controller.enqueue(new Uint8Array(buffer));
            controller.close();
        }
    }).pipeThrough(ds);
    
    const chunks = [];
    let length = 0;
    const reader = stream.getReader();
    
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        length += value.length;
    }

    const result = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
        result.set(chunk, offset);
        offset += chunk.length;
    }
    return result.buffer;
}

const buildDictionaryArrays = (buffer) => {
    const view = new DataView(buffer);
    let offset = 8;
    const numRecords = view.getUint32(4, false);
    indexData.dictKeys = [];
    indexData.dictOffsets = [];
    for (let i = 0; i < numRecords; i++) {
        const biGramLen = view.getUint8(offset, false);
        const biGramBytes = new Uint8Array(buffer, offset + 1, biGramLen);
        const biGram = new TextDecoder('utf-8').decode(biGramBytes);
        const postingListOffset = view.getUint32(offset + 1 + biGramLen, false);
        indexData.dictKeys.push(biGram);
        indexData.dictOffsets.push(postingListOffset);
        offset += 1 + biGramLen + 4;
    }
};

const findBiGramIndex = (biGram) => {
    let low = 0;
    let high = indexData.dictKeys.length - 1;
    let index = -1;
    while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        const currentBiGram = indexData.dictKeys[mid];
        if (currentBiGram === biGram) {
            index = mid;
            break;
        } else if (currentBiGram < biGram) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    return index;
};

const isAscii = (str) => /^[\x00-\x7F]*$/.test(str);

const getBiGramsFromQuery = (query) => {
    const biGrams = new Set();
    const words = query.split(/\s+/).filter(word => word.length > 0);
    for (const word of words) {
        if (word.length === 1 || isAscii(word)) {
            biGrams.add(word.toLowerCase());
        } else {
            for (let i = 0; i < word.length - 1; i++) {
                biGrams.add(word.substring(i, i + 2));
            }
        }
    }
    return Array.from(biGrams);
};

const fetchPostingLists = async (biGrams) => {
    const postingLists = {};
    const promises = [];
    const biGramOffsets = biGrams.map(biGram => {
        const index = findBiGramIndex(biGram);
        if (index !== -1) {
            const offset = indexData.dictOffsets[index];
            const endOffset = index + 1 < indexData.dictOffsets.length ?
                indexData.dictOffsets[index + 1] - 1 :
                null;
            return { biGram, index, offset, endOffset };
        }
        return { biGram, index: -1, offset: -1, endOffset: -1 };
    });
    
    const idxFileStart = indexData.idxOffset;

    for (const item of biGramOffsets) {
        if (item.index === -1) continue;
        promises.push((async () => {
            const start = idxFileStart + item.offset;
            const end = item.endOffset ? idxFileStart + item.endOffset : null;
            const headers = {};
            if (end) {
                headers['Range'] = `bytes=${start}-${end}`;
            } else {
                headers['Range'] = `bytes=${start}-`;
            }

            try {
                const response = await fetch(INDEX_FILE, { headers });
                if (!response.ok) {
                    console.error(`Failed to fetch posting list for ${item.biGram}:`, response.statusText);
                    return;
                }
                const buffer = await response.arrayBuffer();
                const view = new DataView(buffer);
                const list = [];
                for (let i = 0; i < view.byteLength; i += POSTING_LIST_RECORD_SIZE) {
                    const docId = view.getUint32(i, false);
                    const count = view.getUint32(i + 4, false);
                    list.push({ docId, count });
                }
                postingLists[item.biGram] = { df: list.length, list: list };
            } catch (error) {
                console.error(`Error fetching posting list for ${item.biGram}:`, error);
            }
        })());
    }
    await Promise.all(promises);
    return postingLists;
};

const findCommonDocuments = (postingLists) => {
    const biGrams = Object.keys(postingLists);
    if (biGrams.length === 0) return {};
    const sortedLists = biGrams.map(bg => postingLists[bg].list.sort((a, b) => a.docId - b.docId));
    if (sortedLists.some(list => list.length === 0)) return {};
    const matchedDocs = {};
    const pointers = new Array(sortedLists.length).fill(0);
    while (true) {
        const currentDocIds = pointers.map((p, i) => sortedLists[i][p].docId);
        const allSame = currentDocIds.every(id => id === currentDocIds[0]);
        if (allSame) {
            const docId = currentDocIds[0];
            const counts = {};
            for (let i = 0; i < biGrams.length; i++) {
                counts[biGrams[i]] = sortedLists[i][pointers[i]].count;
            }
            matchedDocs[docId] = { counts };
            for (let i = 0; i < pointers.length; i++) pointers[i]++;
            if (pointers.some((p, i) => p >= sortedLists[i].length)) break;
        } else {
            const minId = Math.min(...currentDocIds);
            for (let i = 0; i < pointers.length; i++) {
                if (currentDocIds[i] === minId) pointers[i]++;
            }
            if (pointers.some((p, i) => p >= sortedLists[i].length)) break;
        }
    }
    return matchedDocs;
};

const calculateAndSortResults = (matchedDocs, postingLists) => {
    const results = [];
    const biGrams = Object.keys(postingLists);
    const docMetaView = new DataView(indexData.docMeta);

    for (const docId in matchedDocs) {
        const doc = matchedDocs[docId];
        let score = 0;
        let queryVectorSum = 0;
        
        const docMetaOffset = 4 + parseInt(docId) * DOC_META_RECORD_SIZE;
        const norm = docMetaView.getFloat32(docMetaOffset + 22, false);

        if (norm === 0) continue;
        for (const biGram of biGrams) {
            const tf = doc.counts[biGram];
            const df = postingLists[biGram].df;
            const idf = Math.log(indexData.totalDocs / df);
            const tfidf = tf * idf;
            score += tfidf;
            queryVectorSum += idf * idf;
        }
        const queryNorm = Math.sqrt(queryVectorSum);
        const finalScore = score / (norm * queryNorm);
        results.push({ docId: parseInt(docId), score: finalScore });
    }
    results.sort((a, b) => b.score - a.score);
    return results;
};

export async function performSearch(query, signal) {
    try {
        if (!indexData.isLoaded) {
            await loadIndexFiles();
        }

        const biGrams = getBiGramsFromQuery(query);
        if (biGrams.length === 0) {
            displayInitialMessage();
            return;
        }

        const postingLists = await fetchPostingLists(biGrams);
        if (Object.keys(postingLists).length !== biGrams.length) {
            displayNotFound();
            return;
        }

        const matchedDocs = findCommonDocuments(postingLists);
        if (Object.keys(matchedDocs).length === 0) {
            displayNotFound();
            return;
        }

        const sortedResults = calculateAndSortResults(matchedDocs, postingLists);
        displayResultCount(sortedResults.length);

        const words = query.split(/\s+/).filter(word => word.length > 0);
        const textFragments = words.map(word => `text=${encodeURIComponent(word)}`).join('&');

        const docDataFileStart = indexData.docDataOffset;
        const docMetaView = new DataView(indexData.docMeta);

        for (const item of sortedResults) {
            if (signal.aborted) {
                console.log("Search aborted by user.");
                break;
            }
            const docId = item.docId;
            const docMetaOffset = 4 + docId * DOC_META_RECORD_SIZE;
            const urlOffset = docMetaView.getUint32(docMetaOffset + 4, false);
            const urlLen = docMetaView.getUint16(docMetaOffset + 8, false);
            const titleOffset = docMetaView.getUint32(docMetaOffset + 10, false);
            const titleLen = docMetaView.getUint16(docMetaOffset + 14, false);
            const descOffset = docMetaView.getUint32(docMetaOffset + 16, false);
            const descLen = docMetaView.getUint16(docMetaOffset + 20, false);
            
            const startByte = docDataFileStart + Math.min(urlOffset, titleOffset, descOffset);
            const endByte = docDataFileStart + Math.max(urlOffset + urlLen, titleOffset + titleLen, descOffset + descLen) - 1;

            try {
                const response = await fetch(INDEX_FILE, { headers: { 'Range': `bytes=${startByte}-${endByte}` }, signal });
                if (signal.aborted) break;
                const buffer = await response.arrayBuffer();
                const dataView = new DataView(buffer);
                const baseOffset = startByte - docDataFileStart;
                
                const url = new TextDecoder().decode(dataView.buffer.slice(urlOffset - baseOffset, urlOffset - baseOffset + urlLen));
                const title = new TextDecoder().decode(dataView.buffer.slice(titleOffset - baseOffset, titleOffset - baseOffset + titleLen));
                const description = new TextDecoder().decode(dataView.buffer.slice(descOffset - baseOffset, descOffset - baseOffset + descLen));
                
                const urlWithHighlight = `${url}#:~:${textFragments}`;
                const resultHtml = `
                    <div class="result-item">
                        <h2 class="result-title"><a href="${urlWithHighlight}" target="_blank">${title || 'タイトルなし'}</a></h2>
                        <p class="result-desc">${description || 'ディスクリプションなし'}</p>
                        <p class="result-score">スコア: ${item.score.toFixed(4)}</p>
                        <p class="result-url"><a href="${urlWithHighlight}" target="_blank">${url}</a></p>
                    </div>
                    <hr/>
                `;
                updateSearchResults(resultHtml);
            } catch (error) {
                if (error.name === 'AbortError') {
                    console.log('Fetch aborted.');
                } else {
                    console.error(`Error fetching data for docId ${docId}:`, error);
                }
                break;
            }
        }
    } catch (error) {
        if (error.name === 'AbortError') {
            console.log('Search process aborted.');
        } else {
            console.error('An error occurred during search:', error);
            closePopup();
        }
    }
}

export async function loadIndexFiles() {
    if (indexData.isLoaded) {
        return;
    }

    try {
        const headerResponse = await fetch(INDEX_FILE, { headers: { 'Range': `bytes=0-${HEADER_SIZE - 1}` } });
        if (!headerResponse.ok) {
            throw new Error(`Failed to fetch index header: ${headerResponse.statusText}`);
        }
        const headerBuffer = await headerResponse.arrayBuffer();
        const headerView = new DataView(headerBuffer);
        
        const magicNumber = headerView.getUint32(0, false);
        if (magicNumber !== 0xDA7C) {
            throw new Error('Invalid magic number in index file header.');
        }
        
        const offsets = [];
        for (let i = 0; i < 6; i++) {
            offsets.push(headerView.getUint32(4 + i * 4, false));
        }
        
        const [
            dictGzOffset,
            dociGzOffset,
            dictOffset,
            dociOffset,
            idxOffset,
            docDataOffset
        ] = offsets;

        indexData.idxOffset = idxOffset;
        indexData.docDataOffset = docDataOffset;

        let dictBuffer, dociBuffer;
        if (typeof DecompressionStream === 'function') {
            const dictGzEnd = dociGzOffset > 0 ? dociGzOffset - 1 : undefined;
            const dociGzEnd = dictOffset > 0 ? dictOffset - 1 : undefined;
            
            const dictGzRange = dictGzEnd ? `bytes=${dictGzOffset}-${dictGzEnd}` : `bytes=${dictGzOffset}-`;
            const dociGzRange = dociGzEnd ? `bytes=${dociGzOffset}-${dociGzEnd}` : `bytes=${dociGzOffset}-`;

            const [dictGzRes, dociGzRes] = await Promise.all([
                fetch(INDEX_FILE, { headers: { 'Range': dictGzRange } }),
                fetch(INDEX_FILE, { headers: { 'Range': dociGzRange } })
            ]);

            if (!dictGzRes.ok || !dociGzRes.ok) throw new Error('Failed to fetch compressed files.');
            
            const [dictGzBuf, dociGzBuf] = await Promise.all([
                dictGzRes.arrayBuffer(),
                dociGzRes.arrayBuffer()
            ]);

            dictBuffer = await decompress(dictGzBuf);
            dociBuffer = await decompress(dociGzBuf);
            console.log("Using compressed index files.");
        } else {
            const dictEnd = dociOffset > 0 ? dociOffset - 1 : undefined;
            const dociEnd = idxOffset > 0 ? idxOffset - 1 : undefined;

            const dictRange = dictEnd ? `bytes=${dictOffset}-${dictEnd}` : `bytes=${dictOffset}-`;
            const dociRange = dociEnd ? `bytes=${dociOffset}-${dociEnd}` : `bytes=${dociOffset}-`;

            const [dictRes, dociRes] = await Promise.all([
                fetch(INDEX_FILE, { headers: { 'Range': dictRange } }),
                fetch(INDEX_FILE, { headers: { 'Range': dociRange } })
            ]);
            
            if (!dictRes.ok || !dociRes.ok) throw new Error('Failed to fetch uncompressed files.');

            dictBuffer = await dictRes.arrayBuffer();
            dociBuffer = await dictRes.arrayBuffer();
            console.log("Using uncompressed index files.");
        }

        buildDictionaryArrays(dictBuffer);
        indexData.docMeta = dociBuffer;
        indexData.totalDocs = new DataView(indexData.docMeta).getUint32(0, false);
        indexData.isLoaded = true;
        SEARCH_INPUT.placeholder = '検索語を入力...';
        SEARCH_BUTTON.disabled = false;
    } catch (error) {
        console.error('インデックスファイルの読み込みに失敗しました:', error);
        SEARCH_INPUT.placeholder = 'エラー: 読み込み失敗。リロードしてください。';
    }
}

export function getIndexData() {
    return indexData;
}
