# Python 3.9をベースイメージとして使用
FROM python:3.9-slim

# 作業ディレクトリを設定
WORKDIR /app

# ロケールを設定
ENV LANG=C.UTF-8
ENV MECAB_DIC_DIR=/usr/local/lib/mecab/dic/mecab-ipadic-neologd

# 必要なパッケージをインストール
RUN apt-get update && apt-get install -y \
    build-essential \
    mecab \
    libmecab-dev \
    mecab-ipadic-utf8 \
    unzip \
    git \
    curl \
    file \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# mecab-ipadic-neologdをインストール
RUN git clone https://github.com/neologd/mecab-ipadic-neologd.git /tmp/mecab-ipadic-neologd \
    && /tmp/mecab-ipadic-neologd/bin/install-mecab-ipadic-neologd -n -y -u ${MECAB_DIC_DIR} \
    && rm -rf /tmp/mecab-ipadic-neologd \
    && ln -s /etc/mecabrc /usr/local/etc/mecabrc

# Pythonの依存関係をインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# プログラムコードをコンテナにコピー
COPY create_index.py .

# マウントポイント＆ユーザ・グループ作成
RUN mkdir public index \
    && groupadd -g 1000 indexer \
    && useradd -u 1000 -g 1000 -m indexer

USER indexer

# スクリプトを実行
CMD ["python", "create_index.py"]
