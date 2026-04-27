# elastickv Admin Dashboard Design

**Status:** Partial — every phase of the original P1–P4 plan has shipped. The doc stays at `_partial_` (rather than `_implemented_`) because AdminForward acceptance criterion 5 (rolling-upgrade compatibility flag) is explicitly deferred and the AdminDeleteBucket TOCTOU caught during PR #669 review is tracked here as a pre-existing limitation. See the status table for the per-phase breakdown and Outstanding open items below.
**Author:** bootjp
**Date:** 2026-04-24
**Last updated:** 2026-04-27 (P2 write paths + P4 operator doc landed; status table refreshed)

## Implementation status (as of 2026-04-27)

| Phase | Status | Landed via |
|---|---|---|
| **P1** — `internal/admin/` skeleton, auth, DynamoDB list/create/describe/delete, AdminForward (Section 3.3 acceptance criteria 1–4 + 6; criterion 5 deferred — see outstanding items) | ✅ shipped | #634, #635, #644, #648 |
| **P2** — S3 bucket list/create/delete/ACL, DescribeTable | ✅ shipped | #658 (read-only slice 1) + #669 (writes, slice 2a) + #673 (AdminForward integration, slice 2b) |
| **P3** — React SPA + embed | ✅ shipped | #649, #650 |
| **P4** — TLS, read-only role, CSRF, `docs/admin.md`, deployment runbook + `scripts/rolling-update.sh` admin support | ✅ shipped | TLS / role / CSRF live in P1; operator doc + runbook + script wiring in #674 / #669 / #678 |

Outstanding open items (kept here so future readers know what is still owed against the original proposal):

- **AdminForward acceptance criterion 5** — rolling-upgrade compatibility flag (`admin.leader_forward_v2`). Deferred behind a cluster-version bump; not blocking dashboard usability today because every node forwards through the same `pb.AdminOperation` enum.
- AdminDeleteBucket TOCTOU — A race condition exists where AdminDeleteBucket scans ObjectManifestPrefixForBucket at readTS, but the transaction only includes the BucketMetaKey in its read set. A concurrent PutObject inserting a manifest key in the scanned prefix between readTS and commitTS will not trigger a conflict, leading to orphaned objects. This pre-existing race is also present in the SigV4 path (adapter/s3.go:deleteBucket). Potential fixes include (a) using a bucket-level version key as an OCC token (noting the significant performance trade-off for write-heavy buckets), or (b) extending OperationGroup with ReadRanges for atomic range validation at commit time. This is tracked for a future fix; while the current operator-side workaround is to pause writes, the design should investigate mitigation strategies like a temporary proxy or bridge mode to avoid service interruption during this state.
- **S3 object browser** — explicitly called out as "next phase" in Section 2 Non-goals; no work item yet.
- **Operator-visible TLS cert reload** — out of scope; restart-to-rotate is the documented model in `docs/admin.md`.

When the rolling-upgrade flag and the TOCTOU are both addressed, this doc is renamed `2026_04_24_implemented_admin_dashboard.md` per `docs/design/README.md`'s lifecycle convention.

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

```text
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

- **Admin HTTP handler** (新規): `main.go` が **DynamoDB / S3 のデータプレーン listener (`:8000` / `:9000`) とは別に、専用の admin listener を起動する**。デフォルトは `127.0.0.1:8080` で loopback のみ (Section 7.1)。データプレーン listener と共有しない理由:
  - admin 固有のガードレール (TLS 強制 / ボディサイズ 64 KiB / レート制限 / `allow_plaintext_non_loopback` フラグ) をデータプレーンのパブリック listener に誤適用しない。
  - データプレーンを `0.0.0.0` で公開しつつ admin だけ loopback に閉じる、という典型的な運用を自然に許す。
  - admin を完全に無効化する (`admin.enabled = false`) ときにデータプレーンのルーティングに影響しない。
- **SPA** (新規): React + Vite でビルドした静的ファイルを `go:embed` し、admin handler が配信。
- Admin handler は内部的に **DynamoDB / S3 adapter と同じハンドラ関数を呼び出す** (Section 3.2 の内部エントリポイント経由)。独自のストレージ書き込みは実装しない。

### 3.1 Why re-use existing adapters instead of going direct to `store/`

- DynamoDB の `CreateTable` は `makeCreateTableRequest` (`adapter/dynamodb.go:926`) でテーブルスキーマを OperationGroup にエンコードする非自明なロジックを含む。S3 の `CreateBucket` も `s3BucketMeta` のメタキー書き込みを含む。これらを再実装すると整合性バグを招く。
- 既存アダプタ経路を通すことで権限モデル・ACL チェック・バリデーションをそのまま再利用できる。
- 将来アダプタ側が変更されても UI 経路に追従の必要がない。

### 3.2 SigV4 bypass for internal calls (important)

Admin handler はセッションを SigV4 ではなく JWT Cookie で認証するため、**SigV4 検証を通過できない**。Admin → adapter の呼び出しで `http.Request` を組み立てて署名するのは (1) サーバ側が secret key を保持しない設計 (Section 6.1) と矛盾し、(2) 自己再認証はムダなオーバヘッドである。

したがって adapter 側のハンドラ関数を、**SigV4 検証をスキップする内部エントリポイント**として切り出す:

```go
// adapter/dynamodb.go (既存ハンドラを内部呼び出し可能に分割)
func (a *Dynamo) CreateTable(ctx context.Context, in createTableInput, principal AuthPrincipal) (*dynamoTableSchema, error)
```

- 外部 (`:8000`) の SigV4 ハンドラは署名を検証 → `principal` を作って `CreateTable` を呼ぶ。
- Admin handler は JWT から解決した `principal` を直接渡して `CreateTable` を呼ぶ。
- `principal` には access key / role が含まれ、**認可 (read-only/full) は adapter 側で再評価**する。Admin handler の JWT 検証は認証だけを担当し、認可の真実は常に adapter 側にある。

S3 も同様に `CreateBucket` / `DeleteBucket` / `PutBucketAcl` / `ListBuckets` を内部エントリポイントとして切り出す。SigV4 検証を経ない経路が追加されるため、**adapter 側の既存テストと統合テストの両方で「SigV4 を持たない呼び出し口」の認可を検証する**。

### 3.3 Follower → Leader Forwarding (Required, not optional)

ロードバランサ背後のマルチノード構成では admin の書き込みリクエストが日常的に follower にも着信する。既存の adapter 側 `proxyToLeader` は **SigV4 ヘッダを保持したまま HTTP で leader に再送する**前提のため、SigV4 を持たない admin 内部エントリポイントをそのまま呼ぶと以下が起きる:

- follower でローカル書き込みが試みられ、`etcd raft engine is not leader` で失敗する (data loss や inconsistency ではないが、UX として常時エラーになる)。
- あるいは admin が愚直に HTTP 転送を試みる場合、leader 側で SigV4 検証に失敗して 4xx を返す。

このため **follower→leader 転送は P1 の必須要件**として実装する (Open Question から昇格、Section 11 にはもう置かない)。

#### 3.3.1 採用方式: Principal-aware internal RPC

- 既存の `proxyToLeader` (HTTP 再送) と並行して、**admin 専用の internal gRPC メソッド `AdminForward(principal, operation)`** を raft engine に追加する。
- `principal` には admin JWT から解決した `access_key` と `role` (`read_only` / `full`) が入る。SigV4 ヘッダは含めない。
- follower の admin handler は「自ノードがリーダーでない」と判定したら、gRPC クラスタ内 peer 経由で `AdminForward` をリーダーに送る。
- リーダーは **受信した `principal` を信用せず、クラスタで同期された access-key 設定に照らして再評価** (認可の真実はリーダー側にある)。
- 既存 SigV4 パスの `proxyToLeader` は変更しない。admin パスだけが新 RPC を使う。

#### 3.3.2 受け入れ基準 (acceptance criteria)

書き込み対応の admin API をリリースする前に、以下のユニット + 統合テストをすべてグリーンにする:

1. **リーダー直接接続**: リーダーノードに admin write を投げて成功。
2. **follower 転送**: follower ノードに admin write を投げて成功 (透過的に `AdminForward` 経由でリーダー到達)。
3. **リーダー不在**: 選挙中は `503 AdminLeaderUnavailable` + `Retry-After: 1` を返し、クライアント再試行で最終成功することを確認。
4. **権限の二重チェック**: follower 側で full-access JWT を持ち、`AdminForward` の途中で設定が reload され read-only に降格 → リーダー側で 403 になることを確認 (principal を信用しない設計の検証)。
5. **ローリングアップグレード互換性マトリクス** (AGENTS.md の無停止要件):
   - 旧 follower + 新 leader: 旧 follower の admin は `AdminForward` を知らないので **feature flag `admin.leader_forward_v2` が有効になる前は admin write を 503 `AdminUpgradeInProgress` で返す**。
   - 新 follower + 旧 leader: リーダーが `AdminForward` を受け取れないので同じく 503。
   - クラスタ全台が新版に揃った (バージョンベクタを Raft 経由で確認) 後にフラグを有効化し、以降は透過的に動く。
6. **監査ログ**: 転送された書き込みにも `actor=principal.access_key`、`forwarded_from=<follower node id>` を監査ログに残すこと。

上記 1〜6 を満たさないと P1 の DoD (Definition of Done) 未達とし、マージしない。

---

## 4. Admin HTTP API

Admin API は internal な JSON API であり、AWS API プロトコルには準拠しない。フロントが使いやすい形を優先する。

### 4.1 Endpoint Layout

| Method | Path | 概要 |
|---|---|---|
| `POST`   | `/admin/api/v1/auth/login` | access key / secret key で認証しセッション Cookie を発行 (Section 6.1) |
| `POST`   | `/admin/api/v1/auth/logout` | セッション Cookie を失効させる |
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

`handleCreateBucket` は **初期 ACL を引数に取る内部 API** を新設し、バケット作成と ACL 適用を単一の OperationGroup (Raft 1 本) で原子的にコミットする。2 段階で呼び出す案 (作成 → `PutBucketAcl`) は、途中で失敗するとバケットだけが残り、再試行時に `BucketAlreadyExists` で詰まる / 結果的な ACL が不定といった運用ハザードを生むため却下。

- API 契約: リクエスト全体が成功するか (2xx)、何も残らないか (4xx/5xx) のいずれか。
- ACL 省略時は `private` で固定作成。
- 既存の外部 S3 API (`:9000`) の `CreateBucket` と `PutBucketAcl` の 2 step フローは互換のためそのまま残す。

### 4.3 Pagination

Admin API は **下位アダプタのページネーションをそのまま露出する**。全ページを admin handler 側で結合して返す案は、大規模クラスタでのメモリ圧・レイテンシを招くため却下。

- レスポンスに `next_token` (opaque string) を含め、クライアントは `?next_token=...` で次ページを要求する。
- `limit` クエリパラメータ (デフォルト 100、最大 1,000) を受ける。
- SPA は無限スクロールまたは「Load more」ボタンで逐次取得する (5.2 のページ実装に反映)。
- `next_token` は内部的に DynamoDB `LastEvaluatedTableName` / S3 `NextContinuationToken` をそのまま base64 で包んだもの。サーバーステートは持たない。

### 4.4 Request body size limits

すべての `POST`/`PUT` エンドポイントは `http.MaxBytesReader` で `64 KiB` にハードキャップする (DynamoDB テーブルスキーマ・S3 バケットメタはいずれもこの上限で十分)。超過時は `413 Payload Too Large` を返す。これはプロジェクト規約に沿った DoS 防止。

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

Admin handler のルーティング優先順位 (上ほど先にマッチ):

1. `/admin/api/v1/*` → JSON API ハンドラ (マッチしなければ `404 {"error":"not_found"}`)
2. `/admin/healthz` → プレーンテキストヘルスチェック
3. `/admin/assets/*` → `dist/assets/` の静的資産
4. それ以外の `/admin/*` → `index.html` にフォールバック (SPA ルーティング)

SPA フォールバックは **1〜3 にマッチしなかった場合のみ** 発火する。API / healthz パスが誤って HTML を返さないよう、`http.ServeMux` ではなく `chi` など Prefix マッチの順序を保証できるルータで実装する (または明示的に method + prefix 判定してから fallthrough する)。この振る舞いはユニットテスト (`GET /admin/api/v1/unknown` が 404 JSON、`GET /admin/some/route` が HTML) で必ず固定する。

---

## 6. Authentication and Authorization

### 6.1 Credentials

- 既存の SigV4 資格情報テーブル (config に静的定義) を再利用する。
- 管理 UI は **ブラウザに SigV4 署名ロジックを実装しない**。代わりに以下のフローを採る。

```text
1. POST /admin/api/v1/auth/login
     body: { "access_key": "...", "secret_key": "..." }
   server: signs a short-lived (1h) JWT with HS256 using a server-side
           admin signing key. Returns JWT in a session cookie.

2. 以降の /admin/api/v1/* は Cookie で認証。
```

- **セッション Cookie 属性 (必須)**: `HttpOnly` / `Secure` / `SameSite=Strict` / `Path=/admin` / `Max-Age=3600` を必ず付与する。
  - `Secure` は TLS が有効な時だけでなく **常に付与** する。loopback 平文モードでも同じく付与し、ブラウザが HTTP で送信することを拒否する (ローカル開発は `admin.listen` を `127.0.0.1` で HTTPS を推奨、どうしても平文が必要なら専用の `--admin-dev-insecure-cookie` フラグで明示的に opt-in)。
  - `SameSite=Strict` によりクロスサイト書き込み攻撃を阻止。
  - 仕様を満たさない Cookie を出す実装はユニットテストで検出する (Set-Cookie ヘッダを正規表現で検証)。
- 秘密鍵はサーバー側でのみ検証し、以降のリクエストに渡らない。
- **JWT 署名鍵はクラスタ共有**。ノード毎にランダム生成する案は、ロードバランサの背後で複数ノードに分散された時にノード A で発行したトークンがノード B で検証失敗する (断続的なログアウトが発生する) ため却下。
  - 採用: 起動時に設定ファイル `admin.session_signing_key` (64 byte の base64、Kubernetes Secret 等で配布) を読み込み、全ノードで同一の HS256 鍵を使う。
  - 設定未指定の場合は admin を `enabled = true` で起動させない (起動時失敗)。
  - 鍵ローテーションは **2 鍵並行方式**: `admin.session_signing_key` (現行) に加え `admin.session_signing_key_previous` を受け取り、検証時はどちらかで成功すれば可。ローテ完了後に `_previous` を削除。
  - 将来的には TSO と同様に Raft で管理し、動的ローテーションをノード全台に配る方式へ発展させる (Open Questions 参照)。

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
- **ロール重複の起動時検証**: 同じ access key が `read_only_access_keys` と `full_access_keys` の両方に含まれた場合、実装の lookup 順序次第で read-only のつもりの鍵に書き込み権限が与わる事故を招くため、**設定読み込み時に集合交差を検出したらハードエラーで起動失敗する**。黙って片方を優先する fallback は置かない。ユニットテストで「重複した設定 → 起動失敗」を必ず固定する。

### 6.3 CSRF

変更系 (`POST`/`PUT`/`DELETE`) は **double-submit cookie パターン**で CSRF を防ぐ。`localStorage` にトークンを置く案は XSS 時に奪取されるため却下。

- ログインレスポンスで `csrf_token` を **別の Cookie** (`admin_csrf`) に `Secure`、`SameSite=Strict`、`Path=/admin`、**`HttpOnly` なし** で発行する (SPA の JS が読み取る必要があるため、セッション Cookie とは別 Cookie)。
- SPA は書き込みリクエストで `admin_csrf` Cookie の値を読み取り、`X-Admin-CSRF` ヘッダに入れて送る。
- サーバは Cookie 値とヘッダ値の一致を検証。ミスマッチなら 403。
- CSRF Cookie はセッション Cookie と同じ有効期限 (1h) で回転。
- `SameSite=Strict` セッション Cookie 単独でもクロスサイト書き込みは大半防げるが、browser bug / ユーザー拡張機能等の防御深度として double-submit を併用する。

---

## 7. Configuration and Deployment

### 7.1 New flags / config

| 設定キー | デフォルト | 説明 |
|---|---|---|
| `admin.enabled` | `false` | Admin handler を有効化するか |
| `admin.listen` | `127.0.0.1:8080` | 管理 UI の listen address (デフォルトは loopback のみ) |
| `admin.tls.cert_file` / `admin.tls.key_file` | 空 | TLS 有効化。non-loopback + 未設定なら起動失敗 |
| `admin.allow_plaintext_non_loopback` | `false` | TLS 強制を明示的に opt-out するエスケープハッチ |
| `admin.session_signing_key` | (必須) | クラスタ共通の HS256 鍵 (base64 64 byte)。未設定なら `admin.enabled = true` で起動失敗 |
| `admin.session_signing_key_previous` | 空 | ローテーション時の旧鍵 (検証のみ受け入れる) |
| `admin.read_only_access_keys` | `[]` | 読み取り専用キー |
| `admin.full_access_keys` | `[]` | 管理権限キー |

### 7.2 Binary size

React バンドルは gzip 後 ~150KB を目標。`go:embed` 経由で elastickv バイナリが +1〜2MB 程度膨らむ想定。`admin.enabled = false` でも静的資産はバイナリに含まれる (ビルドフラグで削る案は初版では採らない)。

---

## 8. Implementation Plan

| Phase | 内容 | 目安 |
|---|---|---|
| **P1** | `internal/admin/` 新規作成。Go 側 API スケルトン、auth、DynamoDB テーブル一覧/作成/削除、**`AdminForward` RPC と follower→leader 転送 (Section 3.3 受け入れ基準 1〜6)** | ~1.5 週 |
| **P2** | S3 バケット一覧/作成/削除/ACL、`DescribeTable` 対応 | ~1 週 |
| **P3** | React SPA 実装 + embed | ~1.5 週 |
| **P4** | TLS、read-only ロール、CSRF、ドキュメント (`docs/admin.md`) | ~3 日 |

**P1 の DoD**: Section 3.3.2 の受け入れ基準 1〜6 がすべてグリーンでない限り、書き込み対応の admin API はマージしない。各フェーズ末にユニット + 統合テストを必ず通す。

---

## 9. Testing Strategy

1. **Go ユニットテスト** (`internal/admin/handlers_test.go`)
   - 各エンドポイントに対する happy path / 不正 body / 未認証 / 権限不足 / CSRF 欠落 / 本体サイズ超過 (413)。
   - SigV4 バイパス経路 (`CreateTable(ctx, in, principal)`) に対する認可テスト: read-only principal が write を試みて拒否される、full-access principal が成功する、など。
   - JWT 鍵ローテーション: `session_signing_key_previous` で署名されたトークンが検証成功することを確認。
2. **ブラックボックス統合テスト**
   - `main.go` を in-process で起動し、`net/http` クライアントからログイン → テーブル作成 → 既存 DynamoDB API (`:8000`) で `DescribeTable` が成功することを確認。
   - S3 についても同様に `aws s3api head-bucket` 相当 HTTP 呼び出しで確認。
   - TLS 強制: non-loopback + TLS 未設定 + `allow_plaintext_non_loopback=false` で `main.go` が起動失敗することを検証。
3. **リーダーフォワーディング互換性テスト**: 3 ノード構成で follower に Admin リクエストを送り、書き込みがリーダーに正しく転送されること、旧版ノードが混在する場合に 503 `AdminUpgradeInProgress` が返ることを検証。
4. **フロントエンド**: Vitest + React Testing Library で主要 3 ページのスモーク。E2E (Playwright) は将来検討。
5. **Lint**: 既存 golangci-lint に加え、`dist/` は `.golangciignore` 対象。

---

## 10. Security Considerations

- Admin listener をパブリック IP にさらさないことを README で強く推奨。`admin.enabled` のデフォルトは `false`。
- ログインエンドポイントにレート制限 (per-IP、5 req/min)。
- 全 `POST`/`PUT` エンドポイントで `http.MaxBytesReader` による本体サイズ制限 (4.4 参照) を必須化する。
- Secret key は JWT payload には入れず、サーバーでのみ照合する。
- **TLS 強制**: `admin.listen` が loopback (`127.0.0.1` / `::1`) 以外にバインドされ、かつ `admin.tls.cert_file` が未設定の場合は**起動時にハードエラーで失敗する** (警告だけで続行しない)。平文ネットワーク越しに `secret_key` が流れるのを防ぐため、`--admin-allow-plaintext-non-loopback` のような明示的 opt-out フラグを付けた場合のみ起動を許す (運用ドキュメント上も強く非推奨)。
- すべての書き込み系は監査ログ (`slog` 構造化ログ、key: `admin_audit`, `actor`, `action`, `target`) に落とす。

---

## 11. Open Questions

1. **管理権限キーのローテーション**: 現状、再起動が必要。Raft ベースの動的更新は将来検討。
2. **Audit log sink**: `slog` 先を外部 (Loki 等) に流す統合は次フェーズ。
3. **DynamoDB `DescribeTable` の項目数**: 近似値で良いか / 厳密値が必要か。近似値 (sampling) を基本とし、厳密カウントは UI 上の明示操作 (「Count items」ボタン) に限定する方針でよいか。
4. **JWT 鍵の Raft 同期**: 当面は設定ファイル経由。運用で動的ローテが必要になったら TSO と同じ Raft グループに載せる。

_(follower→leader 転送は当初 Open Question として扱っていたが、書き込み系 API の正しさそのものに関わるため Section 3.3 に必須要件として昇格した)_

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
