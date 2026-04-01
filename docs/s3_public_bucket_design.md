# S3 Public Bucket Access Design

## 1. Background

現在の S3 互換アダプターは全リクエストに AWS Signature V4 認証を要求する。バケット単位・オブジェクト単位のアクセス制御は存在せず、認証が通れば全リソースにフルアクセスとなる。

静的アセット配信や公開データセット共有などのユースケースでは、特定のバケットを認証なしで読み取り可能にする機能が求められる。本ドキュメントでは、バケットレベルの公開配信機能を設計する。

## 2. Goals and Non-goals

### 2.1 Goals

1. バケット単位で公開/非公開を切り替えられる仕組みを提供する。
2. 公開バケットに対する `GetObject`, `HeadObject`, `ListObjectsV2` を認証なしで許可する。
3. 書き込み操作（`PutObject`, `DeleteObject`, マルチパート等）は公開バケットでも認証を必須とする。
4. 既存のバケットはデフォルトで非公開（後方互換性を維持）。
5. `PutBucketAcl` / `GetBucketAcl` API で公開設定を変更・確認できるようにする。

### 2.2 Non-goals

1. オブジェクト単位の ACL（`x-amz-acl` per object）。
2. バケットポリシー JSON（IAM-style の条件付きポリシー）。
3. IAM / STS / クロスアカウント認可。
4. S3 静的ウェブサイトホスティング（index.html 自動解決、カスタムエラーページ等）。
5. CORS 設定。
6. 公開バケットへの匿名書き込み。

## 3. ACL Model

### 3.1 Supported ACL values

AWS S3 の Canned ACL のうち、以下の 2 つのみをサポートする：

| Canned ACL | 読み取り | 書き込み |
|---|---|---|
| `private`（デフォルト） | 認証必須 | 認証必須 |
| `public-read` | 認証不要 | 認証必須 |

それ以外の Canned ACL（`public-read-write`, `authenticated-read`, `aws-exec-read`, `bucket-owner-read`, `bucket-owner-full-control` 等）はエラー `NotImplemented` を返す。

### 3.2 Why bucket-level only

- オブジェクト単位の ACL は AWS 自身も非推奨（S3 Object Ownership の `BucketOwnerEnforced` がデフォルト）。
- 実装の複雑さを大幅に抑えつつ、公開配信の主要ユースケースをカバーできる。
- 将来バケットポリシーを導入する場合も、バケットレベル ACL は自然に共存する。

## 4. Data Model Changes

### 4.1 Bucket metadata extension

`s3BucketMeta` に `Acl` フィールドを追加する：

```go
type s3BucketMeta struct {
    BucketName   string `json:"bucket_name"`
    Generation   uint64 `json:"generation"`
    CreatedAtHLC uint64 `json:"created_at_hlc"`
    Owner        string `json:"owner,omitempty"`
    Region       string `json:"region,omitempty"`
    Acl          string `json:"acl,omitempty"`   // "private" or "public-read"
}
```

- `Acl` が空文字列または未設定の場合は `private` として扱う（後方互換性）。
- 有効な値は `"private"` と `"public-read"` のみ。

### 4.2 Key layout

追加のキーは不要。既存の `!s3|bucket|meta|<bucket-esc>` に ACL 情報が含まれる。

### 4.3 Migration

既存のバケットメタデータには `acl` フィールドが存在しない。JSON デシリアライズ時に `omitempty` により空文字列となり、これは `private` として扱われるため、マイグレーション不要。

## 5. API Changes

### 5.1 PutBucketAcl

```
PUT /<bucket>?acl HTTP/1.1
x-amz-acl: public-read
```

- **認証**: 必須（SigV4）
- **リクエスト**: `x-amz-acl` ヘッダーで Canned ACL を指定。XML ボディによる ACL 指定はサポートしない（`NotImplemented` を返す）。
- **処理**:
  1. バケットメタデータを読み取る
  2. `Acl` フィールドを更新
  3. OCC トランザクションでメタデータを書き戻す
- **レスポンス**: `200 OK`（ボディなし）
- **エラー**:
  - `NoSuchBucket`: バケットが存在しない
  - `NotImplemented`: サポート外の ACL 値
  - `AccessDenied`: 認証失敗

### 5.2 GetBucketAcl

```
GET /<bucket>?acl HTTP/1.1
```

- **認証**: 必須（SigV4）
- **レスポンス**: AWS 互換の XML:

```xml
<AccessControlPolicy>
  <Owner>
    <ID>owner-id</ID>
    <DisplayName>owner-id</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:type="CanonicalUser">
        <ID>owner-id</ID>
        <DisplayName>owner-id</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <!-- public-read の場合のみ追加 -->
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:type="Group">
        <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
      </Grantee>
      <Permission>READ</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>
```

### 5.3 CreateBucket with ACL

```
PUT /<bucket> HTTP/1.1
x-amz-acl: public-read
```

既存の `createBucket` に `x-amz-acl` ヘッダー処理を追加。省略時は `private`。

### 5.4 PutObject ACL header

`PutObject` リクエストの `x-amz-acl` ヘッダーは**無視**する（オブジェクト単位 ACL は非サポート）。将来的に対応する場合は `NotImplemented` を返す方針に変更可能。

## 6. Authentication Flow Changes

### 6.1 Current flow

```
handle(w, r)
  → authorizeRequest(r)        // 全リクエストに SigV4 検証
  → parseS3Path(r)
  → route to handler
```

### 6.2 Proposed flow

```
handle(w, r)
  → parseS3Path(r)             // パスを先にパースする（軽量操作）
  → resolveAuth(r, bucket, objectKey, method)
    → isPublicReadRequest(bucket, method, query)?
      → yes: skip auth
      → no:  authorizeRequest(r)
  → route to handler
```

### 6.3 resolveAuth の詳細

```go
func (s *S3Server) resolveAuth(r *http.Request, bucket, objectKey string) *s3AuthError {
    // ACL/管理 API は常に認証必須
    if isAclRequest(r) || isWriteMethod(r) {
        return s.authorizeRequest(r)
    }

    // バケットが空（ListBuckets）は認証必須
    if bucket == "" {
        return s.authorizeRequest(r)
    }

    // バケットの ACL を確認
    meta, err := s.loadBucketMeta(r.Context(), bucket)
    if err != nil || meta == nil {
        // バケットが見つからない場合は認証フローへ（NoSuchBucket は後段で処理）
        return s.authorizeRequest(r)
    }

    if meta.Acl == "public-read" && isReadOnlyRequest(r) {
        return nil  // 認証スキップ
    }

    return s.authorizeRequest(r)
}
```

### 6.4 Read-only request の定義

以下の条件をすべて満たすリクエストを read-only とみなす：

- HTTP メソッドが `GET` または `HEAD`
- クエリパラメータに `acl`, `uploads`, `uploadId` を含まない
- 対象が `ListObjectsV2`、`GetObject`、`HeadObject`、`HeadBucket` のいずれか

### 6.5 パフォーマンスへの影響

公開バケット判定のためにメタデータ読み取りが認証前に 1 回追加される。ただし：

- バケットメタデータは小さい（〜100 バイト）
- `GetObject` / `ListObjectsV2` はいずれにせよバケットメタデータを後段で読み取るため、キャッシュで重複を回避できる
- 非公開バケットでは従来と同じ認証パスを通るためオーバーヘッドは最小

将来的にインメモリの ACL キャッシュ（TTL 付き）を導入してメタデータ読み取りを削減できるが、初期実装では不要。

## 7. Request Flow Examples

### 7.1 Public bucket: anonymous GetObject

```
Client                    S3 Server
  |  GET /public-bucket/file.txt    |
  |  (Authorization ヘッダーなし)    |
  |-------------------------------->|
  |                                 | parseS3Path → bucket="public-bucket"
  |                                 | loadBucketMeta → acl="public-read"
  |                                 | isReadOnlyRequest → true
  |                                 | → 認証スキップ
  |                                 | getObject()
  |  200 OK + body                  |
  |<--------------------------------|
```

### 7.2 Public bucket: anonymous PutObject (rejected)

```
Client                    S3 Server
  |  PUT /public-bucket/file.txt    |
  |  (Authorization ヘッダーなし)    |
  |-------------------------------->|
  |                                 | parseS3Path → bucket="public-bucket"
  |                                 | isWriteMethod → true
  |                                 | authorizeRequest → AccessDenied
  |  403 AccessDenied               |
  |<--------------------------------|
```

### 7.3 Private bucket: anonymous GetObject (rejected)

```
Client                    S3 Server
  |  GET /private-bucket/file.txt   |
  |  (Authorization ヘッダーなし)    |
  |-------------------------------->|
  |                                 | parseS3Path → bucket="private-bucket"
  |                                 | loadBucketMeta → acl="" (private)
  |                                 | authorizeRequest → AccessDenied
  |  403 AccessDenied               |
  |<--------------------------------|
```

### 7.4 Changing ACL

```
Client                    S3 Server
  |  PUT /my-bucket?acl             |
  |  x-amz-acl: public-read        |
  |  Authorization: AWS4-HMAC-...   |
  |-------------------------------->|
  |                                 | authorizeRequest → OK
  |                                 | putBucketAcl()
  |                                 |   load meta → update acl → txn write
  |  200 OK                         |
  |<--------------------------------|
```

## 8. Implementation Plan

### Phase 1: Core ACL infrastructure

1. `s3BucketMeta` に `Acl` フィールドを追加
2. ACL バリデーション関数 (`validateCannedAcl`) を追加
3. `isPublicReadBucket` / `isReadOnlyRequest` ヘルパーを追加

### Phase 2: Auth flow changes

1. `handle()` の認証フローを変更し、`resolveAuth()` を導入
2. パスのパースを認証より前に移動
3. 公開バケットの read-only リクエストで認証をスキップ

### Phase 3: ACL API endpoints

1. `PutBucketAcl` ハンドラーを実装
2. `GetBucketAcl` ハンドラーを実装
3. `CreateBucket` に `x-amz-acl` ヘッダー処理を追加
4. `handleBucket` のルーティングに `?acl` クエリパラメータ分岐を追加

### Phase 4: Proxy integration

1. `maybeProxyToLeader` でプロキシ時にも公開バケット判定が正しく動作することを確認
2. フォロワーノードでのメタデータ読み取りの一貫性を検証

## 9. Security Considerations

### 9.1 Write protection

公開バケットであっても書き込み操作は常に認証を要求する。`resolveAuth` では HTTP メソッドを最初にチェックし、`PUT`, `POST`, `DELETE` は早期に認証フローへ送る。

### 9.2 ACL 変更の保護

`PutBucketAcl` は認証必須。意図しない公開化を防ぐため、サーバー起動オプションとして `--s3DenyPublicBuckets` フラグを将来追加可能（初期実装では不要）。

### 9.3 Listing protection

公開バケットでは `ListObjectsV2` を認証なしで許可する。これは AWS S3 の `public-read` ACL と同じ挙動。バケット内のキー一覧が見えることを許容するユースケースのみを想定。

### 9.4 ListBuckets

`ListBuckets`（`GET /`）は公開バケットが存在しても常に認証を要求する。バケット一覧の公開は行わない。

## 10. Testing Plan

### 10.1 Unit tests

1. `validateCannedAcl` のバリデーション（private, public-read, 不正値）
2. `isReadOnlyRequest` の判定ロジック（GET/HEAD/POST/PUT/DELETE × クエリパラメータ）
3. `resolveAuth` の分岐（公開バケット×read / 公開バケット×write / 非公開バケット×read）
4. `GetBucketAcl` の XML レスポンス生成

### 10.2 Integration tests

1. バケット作成時に `x-amz-acl: public-read` を指定 → 匿名 GET 成功
2. 匿名 PUT → 403
3. 匿名 ListObjectsV2 on public bucket → 成功
4. 匿名 GetObject on private bucket → 403
5. `PutBucketAcl` で public-read → private に変更 → 匿名 GET が 403 に変化
6. 既存バケット（`acl` フィールドなし）が private として動作する後方互換性テスト

### 10.3 Compatibility tests

1. `aws s3api put-bucket-acl --acl public-read`
2. `aws s3api get-bucket-acl`
3. `curl` による匿名アクセス（Authorization ヘッダーなし）
4. `aws s3 cp` による匿名ダウンロード（`--no-sign-request`）

## 11. Future Extensions

- **バケットポリシー**: JSON ベースの条件付きポリシーによる細粒度アクセス制御
- **`--s3DenyPublicBuckets`**: クラスタレベルで公開バケット作成を禁止するガードレール
- **ACL キャッシュ**: インメモリキャッシュによるメタデータ読み取り削減
- **静的ウェブサイトホスティング**: index.html 自動解決、カスタムエラーページ
- **CORS 設定**: ブラウザからの直接アクセス対応
