# elastickv Admin Dashboard Design

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-04-24

---

## 1. Background and Motivation

elastickv は現在、DynamoDB 互換 (`adapter/dynamodb.go`) と S3 互換 (`adapter/s3.go`) の両アダプタを提供している。しかし、テーブル／バケットの作成・一覧・削除といった管理操作は、以下のいずれかでしか行えない。

- AWS CLI または AWS SDK (DynamoDB: `CreateTable` / `ListTables` / `DescribeTable`、S3: `CreateBucket` / `ListBuckets` / `HeadBucket`)
- 直接 HTTP で `X-Amz-Target` ヘッダを叩く

運用者・開発者が「どんなテーブル／バケットが存在しているか」「行レコード数・ストレージサイズはどの程度か」を確認するための視覚的なインターフェイスは存在しない。Grafana ダッシュボード (`monitoring/grafana/dashboards/`) はクラスタ指標は見られるが、データモデル的なメタデータ (テーブル・バケット一覧) には触れない。

本ドキュメントでは、**elastickv に組み込む管理ダッシュボード (Admin Dashboard)** を設計する。ユーザはブラウザで以下を行える。

- DynamoDB テーブルの一覧・作成・削除・スキーマ確認
- S3 バケットの一覧・作成・削除・ACL 変更

この管理 UI は既存アダプタの公開 API を経由し、**新しい永続化パスや Raft 経路を一切追加しない**ことを要件とする。

---

## 2. Goals and Non-goals

### 2.1 Goals

1. 管理者が GUI からテーブル／バケットの CRUD 操作を行えるようにする。
2. 既存の DynamoDB / S3 互換 API を唯一の書き込み経路として再利用する (データパスを増やさない)。
3. クラスタノード内にバンドルされた単一バイナリで配信する (追加の外部サービス不要)。
4. 認証・認可を持ち、既存の AWS SigV4 資格情報 (access key / secret key) と統合する。
5. 読み取りのみのモード (view-only) をサポートし、本番クラスタに安全に常駐できる。

### 2.2 Non-goals

1. マルチテナント / ユーザー管理 UI (資格情報の CRUD) — 本 PR では資格情報は既存の設定ファイルに委ねる。
2. DynamoDB 項目エディタ (PutItem / GetItem の GUI) — 次フェーズ。
3. S3 オブジェクトブラウザ (アップロード・ダウンロード GUI) — 次フェーズ。
4. Raft / シャード運用 (leader transfer、reshard 等) — オブザーバビリティは Grafana に任せる。
5. メトリクス可視化 — Prometheus/Grafana スタックの置き換えではない。
6. モバイルレイアウト最適化。

---

## 3. Architecture Overview

```
  ┌─────────────────────────────────────────────────────────────┐
  │                       elastickv node                         │
  │                                                              │
  │   ┌──────────────┐   ┌──────────────┐   ┌────────────────┐ │
  │   │  DynamoDB    │   │     S3        │   │  Admin HTTP    │ │
  │   │  adapter     │   │   adapter     │   │  handler (new) │ │
  │   │ (:8000)      │   │  (:9000)      │   │  (:8080/admin) │ │
  │   └──────┬───────┘   └──────┬────────┘   └───────┬────────┘ │
  │          │                  │                    │          │
  │          ▼                  ▼                    ▼          │
  │   ┌────────────────────────────────────────────────────┐  │
  │   │              kv.OperationGroup / Store              │  │
  │   └────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────┘
                               ▲
                               │  (HTTP + JSON)
                               │
                        ┌──────┴──────┐
                        │  Browser    │
                        │  SPA (new)  │
                        └─────────────┘
```

- **Admin HTTP handler** (新規): 既存の `main.go` が起動する HTTP listener に `/admin/*` を追加する。
- **SPA** (新規): React + Vite でビルドした静的ファイルを `go:embed` し、admin handler が配信。
- Admin handler は内部的に **DynamoDB / S3 adapter と同じハンドラ関数を呼び出す**。独自のストレージ書き込みは実装しない。

### 3.1 Why re-use existing adapters instead of going direct to `store/`

- DynamoDB の `CreateTable` は `makeCreateTableRequest` (`adapter/dynamodb.go:926`) でテーブルスキーマを OperationGroup にエンコードする非自明なロジックを含む。S3 の `CreateBucket` も `s3BucketMeta` のメタキー書き込みを含む。これらを再実装すると整合性バグを招く。
- 既存アダプタ経路を通すことで SigV4 検証・権限モデル・ACL チェックもそのまま再利用できる。
- 将来アダプタ側が変更されても UI 経路に追従の必要がない。

---

## 4. Admin HTTP API

Admin API は internal な JSON API であり、AWS API プロトコルには準拠しない。フロントが使いやすい形を優先する。

### 4.1 Endpoint Layout

| Method | Path | 概要 |
|---|---|---|
| `GET`    | `/admin/api/v1/cluster` | ノード ID, Raft リーダー, バージョンを返す |
| `GET`    | `/admin/api/v1/dynamo/tables` | テーブル一覧 (`ListTables` を内部で呼ぶ) |
| `POST`   | `/admin/api/v1/dynamo/tables` | テーブル作成 (`CreateTable` 相当) |
| `GET`    | `/admin/api/v1/dynamo/tables/{name}` | テーブルスキーマ・統計 (`DescribeTable`) |
| `DELETE` | `/admin/api/v1/dynamo/tables/{name}` | テーブル削除 (`DeleteTable`) |
| `GET`    | `/admin/api/v1/s3/buckets` | バケット一覧 |
| `POST`   | `/admin/api/v1/s3/buckets` | バケット作成 |
| `GET`    | `/admin/api/v1/s3/buckets/{name}` | バケットメタ + ACL |
| `PUT`    | `/admin/api/v1/s3/buckets/{name}/acl` | ACL 変更 (`PutBucketAcl`) |
| `DELETE` | `/admin/api/v1/s3/buckets/{name}` | バケット削除 |
| `GET`    | `/admin/healthz` | 配信用ヘルスチェック |

### 4.2 Request / Response Examples

**POST /admin/api/v1/dynamo/tables**

```json
{
  "table_name": "users",
  "partition_key": { "name": "id", "type": "S" },
  "sort_key":      { "name": "created_at", "type": "N" },
  "gsi": [
    {
      "name": "by-email",
      "partition_key": { "name": "email", "type": "S" },
      "projection": { "type": "ALL" }
    }
  ]
}
```

Admin handler は上記ボディを `adapter/dynamodb.go` の `createTableInput` (AWS DynamoDB ワイヤフォーマット) に変換し、**同じプロセス内で** 既存ハンドラを呼び出す (HTTP round-trip ではなく Go 関数呼び出し)。

**POST /admin/api/v1/s3/buckets**

```json
{
  "bucket_name": "public-assets",
  "acl": "public-read"
}
```

同様に `adapter/s3.go` の `handleCreateBucket` を呼び、続けて ACL 指定があれば `PutBucketAcl` 相当の処理を行う。

### 4.3 Pagination

- `ListTables` / `ListBuckets` は内部で既にページネーションをサポートしているが、管理 UI 側は単一リスト表示で十分なため、admin handler 側で全ページを結合して返す (最大件数 10,000 件でハードキャップ)。

---

## 5. Frontend (SPA)

### 5.1 Technology

- **React 18 + TypeScript**, ビルドは **Vite**。
- UI フレームワークは軽量さを優先し **Tailwind CSS** + **shadcn/ui** (ヘッドレスコンポーネント) のみ。
- チャート系ライブラリは入れない (非ゴール)。
- 成果物 (`dist/`) を `internal/admin/dist` に配置し、Go の `embed.FS` で単一バイナリに含める。

### 5.2 Pages

1. **/admin** (dashboard top): クラスタサマリ (ノード数、テーブル数、バケット数)
2. **/admin/dynamo**: テーブル一覧 + 「Create Table」ダイアログ
3. **/admin/dynamo/:name**: テーブル詳細 (key schema、GSI、生成番号、推定行数)
4. **/admin/s3**: バケット一覧 + 「Create Bucket」ダイアログ
5. **/admin/s3/:name**: バケットメタ + ACL 切り替えトグル

### 5.3 Static asset embedding

```go
// internal/admin/embed.go
//go:embed dist
var distFS embed.FS
```

Admin handler は `dist/` を `/admin/assets/*` で配信し、すべての `/admin/*` パスは `index.html` にフォールバックする (SPA ルーティング)。

---

## 6. Authentication and Authorization

### 6.1 Credentials

- 既存の SigV4 資格情報テーブル (config に静的定義) を再利用する。
- 管理 UI は **ブラウザに SigV4 署名ロジックを実装しない**。代わりに以下のフローを採る。

```
1. POST /admin/api/v1/auth/login
     body: { "access_key": "...", "secret_key": "..." }
   server: signs a short-lived (1h) JWT with HS256 using a server-side
           admin signing key. Returns JWT in an httpOnly cookie.

2. 以降の /admin/api/v1/* は Cookie で認証。
```

- 秘密鍵はサーバー側でのみ検証し、以降のリクエストに渡らない。
- JWT シグナルキーはノードごとにランダム生成 (プロセスライフタイム)。ノード再起動で全セッションが失効するのは許容する (長期セッションは非ゴール)。

### 6.2 Role Model

設定ファイル (`config.toml` 相当) に以下の項目を追加する。

```toml
[admin]
enabled = true
listen  = ":8080"
read_only_access_keys = ["AKIA_READONLY_1"]
full_access_keys      = ["AKIA_ADMIN_1"]
```

- `read_only_access_keys` でログインしたセッションは `GET` のみ許可。
- `full_access_keys` はすべての CRUD を許可。
- どちらにも含まれない access key でのログインは 403。

### 6.3 CSRF

- 変更系 (`POST`/`PUT`/`DELETE`) は `X-Admin-CSRF` ヘッダを要求する。
- ログインレスポンスで `csrf_token` をボディで返し、SPA が `localStorage` に保管、全書き込みリクエストに付与する。

---

## 7. Configuration and Deployment

### 7.1 New flags / config

| 設定キー | デフォルト | 説明 |
|---|---|---|
| `admin.enabled` | `false` | Admin handler を有効化するか |
| `admin.listen` | `:8080` | 管理 UI の listen address |
| `admin.tls.cert_file` / `admin.tls.key_file` | 空 | TLS 有効化 |
| `admin.read_only_access_keys` | `[]` | 読み取り専用キー |
| `admin.full_access_keys` | `[]` | 管理権限キー |

### 7.2 Binary size

React バンドルは gzip 後 ~150KB を目標。`go:embed` 経由で elastickv バイナリが +1〜2MB 程度膨らむ想定。`admin.enabled = false` でも静的資産はバイナリに含まれる (ビルドフラグで削る案は初版では採らない)。

---

## 8. Implementation Plan

| Phase | 内容 | 目安 |
|---|---|---|
| **P1** | `internal/admin/` 新規作成。Go 側 API スケルトン、auth、DynamoDB テーブル一覧/作成/削除の 3 エンドポイント | ~1 週 |
| **P2** | S3 バケット一覧/作成/削除/ACL、`DescribeTable` 対応 | ~1 週 |
| **P3** | React SPA 実装 + embed | ~1.5 週 |
| **P4** | TLS、read-only ロール、CSRF、ドキュメント (`docs/admin.md`) | ~3 日 |

各フェーズ末にユニット + 統合テストを必ず通す。

---

## 9. Testing Strategy

1. **Go ユニットテスト** (`internal/admin/handlers_test.go`)
   - 各エンドポイントに対する happy path / 不正 body / 未認証 / 権限不足 / CSRF 欠落。
2. **ブラックボックス統合テスト**
   - `main.go` を in-process で起動し、`net/http` クライアントからログイン → テーブル作成 → 既存 DynamoDB API (`:8000`) で `DescribeTable` が成功することを確認。
   - S3 についても同様に `aws s3api head-bucket` 相当 HTTP 呼び出しで確認。
3. **フロントエンド**: Vitest + React Testing Library で主要 3 ページのスモーク。E2E (Playwright) は将来検討。
4. **Lint**: 既存 golangci-lint に加え、`dist/` は `.golangciignore` 対象。

---

## 10. Security Considerations

- Admin listener をパブリック IP にさらさないことを README で強く推奨。`admin.enabled` のデフォルトは `false`。
- ログインエンドポイントにレート制限 (per-IP、5 req/min)。
- Secret key は JWT payload には入れず、サーバーでのみ照合する。
- TLS 無効で non-loopback にバインドしようとした場合、起動時に警告ログを出す。
- すべての書き込み系は監査ログ (`slog` 構造化ログ、key: `admin_audit`, `actor`, `action`, `target`) に落とす。

---

## 11. Open Questions

1. **管理権限キーのローテーション**: 現状、再起動が必要。Raft ベースの動的更新は将来検討。
2. **マルチノード同一ダッシュボード**: 任意のノードに繋げば同じ一覧が見える (すべて Raft 経由)。リーダーへのフォワードは既存 adapter が行うので追加実装不要のはず。ただしリーダーが不在の時の UX (503) の扱いを決める必要あり。
3. **Audit log sink**: `slog` 先を外部 (Loki 等) に流す統合は次フェーズ。
4. **DynamoDB `DescribeTable` の項目数**: 近似値で良いか / 厳密値が必要か。近似値 (sampling) を基本とし、厳密カウントは UI 上の明示操作 (「Count items」ボタン) に限定する方針でよいか。

---

## 12. Alternatives Considered

### 12.1 外部 AWS コンソール互換 UI を流用

例: `DynamoDB Admin` (OSS) を別コンテナで動かす。

- 利点: 実装ゼロ。
- 欠点: S3 との統合 UI がない、ACL 等 elastickv 固有拡張が扱えない、資格情報の二重管理。

→ 却下。本プロジェクトでは「単一バイナリに同梱」が大きな価値を持つ。

### 12.2 既存 Grafana ダッシュボードに組み込む

- 利点: 既にメトリクス可視化基盤がある。
- 欠点: Grafana は "書き込み UI" の用途に向かない。プラグイン化は保守コストが高い。

→ 却下。メトリクスは Grafana、CRUD は本ダッシュボードと役割を分ける。

### 12.3 CLI 拡張のみ (`cmd/client` にサブコマンド)

- 利点: 最小実装。
- 欠点: GUI ニーズを満たさない。

→ 並行して `cmd/client` に `table create` / `bucket create` を追加することは有意義だが、本 PR のスコープ外とする。
