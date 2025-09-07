import os
import glob
import json
import MeCab
import struct
from collections import defaultdict
from bs4 import BeautifulSoup
import math
import gzip
import io

# 設定
HTML_DOCS_DIR = 'public'
OUTPUT_DIR = 'index'
DICT_FILE = os.path.join(OUTPUT_DIR, 'nnz.dict')
INDEX_FILE = os.path.join(OUTPUT_DIR, 'nnz.idx')
DOC_META_FILE = os.path.join(OUTPUT_DIR, 'nnz.doci')
DOC_DATA_FILE = os.path.join(OUTPUT_DIR, 'nnz.docd')
ALL_INDEX_FILE = os.path.join(OUTPUT_DIR, 'nnz.all.idx')

# バイナリデータフォーマット
DOC_ID_SIZE = 4
COUNT_SIZE = 4
# docid(4)+url_offset(4)+url_len(2)+title_offset(4)+title_len(2)+desc_offset(4)+desc_len(2)+norm(4) = 26 bytes
DOC_META_RECORD_SIZE = 26
URL_OFFSET_SIZE = 4
URL_LEN_SIZE = 2
TITLE_OFFSET_SIZE = 4
TITLE_LEN_SIZE = 2
DESC_OFFSET_SIZE = 4
DESC_LEN_SIZE = 2
NORM_SIZE = 4

# 除外したいパスのプレフィックス
SKIPPED_PATH_PREFIXES = [
    'auth/',
]

# MeCabの初期化
MECAB_DIC_PATH = '/usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd'
mecab = MeCab.Tagger(f'-Ochasen -d {MECAB_DIC_PATH}')

# 許可する品詞のリスト
ALLOWED_POS = ['名詞', '動詞', '形容詞', '副詞', '感動詞']

# URLプレフィックスのマッピング
URL_PREFIX_MAP = {
    'www': 'https://www.digitune.org/',
    'memo': 'https://memo.digitune.org/',
    'other': 'https://www.digitune.org/',
}
DEFAULT_URL_PREFIX = URL_PREFIX_MAP['other']

def extract_text_and_title_and_description(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # タイトルを抽出
    title = soup.title.string if soup.title else None
    
    # インデックス用: 一部のタグを削除
    index_text_soup = soup
    for tag in index_text_soup(['script', 'style', 'head', 'title', 'meta', 'link', 'nav', 'header', 'footer', 'input', 'button']):
        tag.decompose()
    index_text = index_text_soup.get_text()

    # ディスクリプション用: H1-H6なども削除
    desc_text_soup = soup
    for tag in desc_text_soup(['script', 'style', 'head', 'title', 'meta', 'link', 'nav', 'header', 'footer', 'input', 'button', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'img']):
        tag.decompose()
    desc_text = desc_text_soup.get_text()
    
    # ディスクリプションを生成（140文字で切り捨て）
    cleaned_desc_text = ' '.join(desc_text.split()).strip()
    description = cleaned_desc_text[:140]
    if len(cleaned_desc_text) > 140:
        description += '...'

    return index_text, title, description

def is_ascii(s):
    try:
        s.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False

def get_bi_grams(text):
    bi_grams = []
    
    node = mecab.parseToNode(text)
    
    while node:
        features = node.feature.split(',')
        pos = features[0]
        
        if pos in ALLOWED_POS:
            surface_form = node.surface
            # 1文字もしくはASCII文字のみを含む場合はそのまま利用
            if len(surface_form) == 1 or is_ascii(surface_form):
                bi_grams.append(surface_form.lower())
            else:
                for i in range(len(surface_form) - 1):
                    bi_grams.append(surface_form[i:i+2])
            # 表層形と基本形が異なる場合は基本形も対象に 
            if len(features) > 6 and features[6] != '*' and features[6] != surface_form:
                base_form = features[6]
                # 1文字もしくはASCII文字のみを含む場合はそのまま利用
                if len(base_form) == 1 or is_ascii(base_form):
                    bi_grams.append(base_form.lower())
                else:
                    for i in range(len(base_form) - 1):
                        bi_grams.append(base_form[i:i+2])
        
        node = node.next
    
    return bi_grams

def write_posting_list(posting_lists):
    offsets = {}
    with open(INDEX_FILE, 'wb') as f:
        for bi_gram, entries in sorted(posting_lists.items()):
            offsets[bi_gram] = f.tell()
            for doc_id, count in entries:
                f.write(struct.pack('>I', doc_id))
                f.write(struct.pack('>I', count))
    return offsets

def write_dict_file(posting_list_offsets):
    sorted_bi_grams = sorted(posting_list_offsets.keys())
    
    buffer = io.BytesIO()
    MAGIC_NUMBER = 0xDA7A
    buffer.write(struct.pack('>I', MAGIC_NUMBER))
    buffer.write(struct.pack('>I', len(sorted_bi_grams)))
    
    for bi_gram in sorted_bi_grams:
        encoded_bi_gram = bi_gram.encode('utf-8')
        buffer.write(struct.pack('>B', len(encoded_bi_gram)))
        buffer.write(encoded_bi_gram)
        buffer.write(struct.pack('>I', posting_list_offsets[bi_gram]))

    # 非圧縮版を書き込み
    with open(DICT_FILE, 'wb') as f:
        f.write(buffer.getvalue())

    # 圧縮版を書き込み
    with gzip.open(DICT_FILE + '.gz', 'wb') as f:
        f.write(buffer.getvalue())

def write_document_db(document_db):
    total_docs = len(document_db)
    
    meta_buffer = io.BytesIO()
    meta_buffer.write(struct.pack('>I', total_docs))
    
    with open(DOC_DATA_FILE, 'wb') as data_f:
        current_data_offset = 0
        for doc in document_db:
            url_bytes = doc['url'].encode('utf-8')
            title_bytes = doc['title'].encode('utf-8') if doc['title'] else b''
            desc_bytes = doc['description'].encode('utf-8') if doc['description'] else b''
            
            url_offset = current_data_offset
            url_len = len(url_bytes)
            
            title_offset = current_data_offset + url_len
            title_len = len(title_bytes)
            
            desc_offset = current_data_offset + url_len + title_len
            desc_len = len(desc_bytes)

            data_f.write(url_bytes)
            data_f.write(title_bytes)
            data_f.write(desc_bytes)

            current_data_offset += url_len + title_len + desc_len
            
            meta_buffer.write(struct.pack('>I', doc['doc_id']))
            meta_buffer.write(struct.pack('>I', url_offset))
            meta_buffer.write(struct.pack('>H', url_len))
            meta_buffer.write(struct.pack('>I', title_offset))
            meta_buffer.write(struct.pack('>H', title_len))
            meta_buffer.write(struct.pack('>I', desc_offset))
            meta_buffer.write(struct.pack('>H', desc_len))
            meta_buffer.write(struct.pack('>f', doc['norm']))

    # 非圧縮版を書き込み
    with open(DOC_META_FILE, 'wb') as f:
        f.write(meta_buffer.getvalue())

    # 圧縮版を書き込み
    with gzip.open(DOC_META_FILE + '.gz', 'wb') as f:
        f.write(meta_buffer.getvalue())

def write_all_index_combined():
    # 指定されたファイルと順序
    source_files = [
        os.path.join(OUTPUT_DIR, 'nnz.dict.gz'),
        os.path.join(OUTPUT_DIR, 'nnz.doci.gz'),
        os.path.join(OUTPUT_DIR, 'nnz.dict'),
        os.path.join(OUTPUT_DIR, 'nnz.doci'),
        os.path.join(OUTPUT_DIR, 'nnz.idx'),
        os.path.join(OUTPUT_DIR, 'nnz.docd')
    ]

    # マジックナンバーと各ファイルのオフセットを格納するためのヘッダー
    HEADER_MAGIC_NUMBER = 0xDA7C  # 新しいマジックナンバー
    HEADER_SIZE = 4 + 4 * len(source_files)
    offsets = []

    with open(ALL_INDEX_FILE, 'wb') as output_f:
        # ヘッダー用のプレースホルダーを確保
        output_f.write(b'\x00' * HEADER_SIZE)

        for file_path in source_files:
            if not os.path.exists(file_path):
                print(f"警告: {file_path} が見つかりませんでした。スキップします。")
                offsets.append(0) # 存在しない場合はオフセットを0にする
                continue
            
            # 各ファイルの開始オフセットを記録
            offsets.append(output_f.tell())
            
            # ファイルの内容を読み込んで、出力ファイルに書き込む
            with open(file_path, 'rb') as input_f:
                output_f.write(input_f.read())
        
        # ヘッダーを更新
        output_f.seek(0)
        output_f.write(struct.pack('>I', HEADER_MAGIC_NUMBER))
        for offset in offsets:
            output_f.write(struct.pack('>I', offset))

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    posting_lists = defaultdict(list)
    document_db = []
    doc_id_counter = 0

    print("ドキュメントを読み込み、ポスティングリストを作成しています...")
    file_paths = glob.glob(os.path.join(HTML_DOCS_DIR, '**/*.html'), recursive=True)
    total_files = len(file_paths)
    for i, file_path in enumerate(file_paths):
        relative_path = os.path.relpath(file_path, HTML_DOCS_DIR)
        
        if any(relative_path.startswith(p) for p in SKIPPED_PATH_PREFIXES):
            print(f"Skipping: {file_path} (Excluded by path)")
            continue
            
        if not os.path.basename(file_path).startswith('.'):
            print(f"Processing: {file_path} ({i+1}/{total_files})")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
            except UnicodeDecodeError:
                print(f"警告: {file_path} の文字コードエラーによりスキップされました。")
                continue
                
            index_text, title, description = extract_text_and_title_and_description(html_content)
            bi_grams = get_bi_grams(index_text)
            
            bi_gram_counts = defaultdict(int)
            for bg in bi_grams:
                bi_gram_counts[bg] += 1

            for bg, count in bi_gram_counts.items():
                posting_lists[bg].append((doc_id_counter, count))

            parts = relative_path.split(os.sep)
            sub_dir = parts[0] if len(parts) > 1 else None
            
            url_prefix = DEFAULT_URL_PREFIX
            path_to_use = relative_path

            if sub_dir in URL_PREFIX_MAP:
                url_prefix = URL_PREFIX_MAP[sub_dir]
                path_to_use = os.path.join(*parts[1:])

            url = f"{url_prefix}{path_to_use.replace(os.sep, '/')}"

            document_db.append({
                'doc_id': doc_id_counter,
                'url': url,
                'title': title,
                'description': description,
                'bi_gram_counts': bi_gram_counts
            })
            doc_id_counter += 1

    unique_bi_grams = len(posting_lists)
    print(f"ユニークなbi-gramの数: {unique_bi_grams}")
    
    term_dfs = {bi_gram: len(entries) for bi_gram, entries in posting_lists.items()}
    
    total_docs_processed = len(document_db)
    for doc in document_db:
        norm_squared = 0
        for bi_gram, count in doc['bi_gram_counts'].items():
            if bi_gram in term_dfs and term_dfs[bi_gram] > 0:
                tf = count
                idf = math.log(total_docs_processed / term_dfs[bi_gram])
                norm_squared += (tf * idf) ** 2
        doc['norm'] = math.sqrt(norm_squared)

    print("ポスティングリストをファイルに書き込んでいます...")
    posting_list_offsets = write_posting_list(posting_lists)
    
    print("辞書ファイルを構築しています...")
    write_dict_file(posting_list_offsets)

    print("文書DBをファイルに書き込んでいます...")
    write_document_db(document_db)
            
    print("すべてのデータを一つのファイルに結合しています...")
    write_all_index_combined()
    
    print("インデックス作成が完了しました。")

if __name__ == '__main__':
    main()
