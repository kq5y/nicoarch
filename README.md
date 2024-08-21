# nicoarch

ニコニコ動画の動画ファイル・メタデータ・コメントデータをアーカイブするツール

## 使い方

1. `.env.sample`を`.env`にコピーし、編集する。
    `NICONICO_MAIL`: ニコニコ動画のメールアドレス
    `NICONICO_PASSWORD`: ニコニコ動画のパスワード
    `WORKER_COUNT`: workerの数 (コメント取得に難があるため1推奨)
    `NGINX_PORT`: ホストするポート番号(デフォルト: 8080)

2. 下記コマンドを実行し、localhost:8080にアクセスすると使用できる。

    ```sh
    docker compose up -d --build
    ```

## ライセンス
[MIT License](LICENSE)
