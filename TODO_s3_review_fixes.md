# S3 Phase2 レビュー指摘修正 TODO

## Priority 1 (Critical)

- [x] 1. createMultipartUpload の IsTxn=true 化 — 分散環境での原子性保証
- [x] 2. セマフォ満杯時の GC 回復 — non-blocking select でセマフォ満杯時に cleanup を skip（goroutine スポーン前に semaphore 取得済み、無限 goroutine 蓄積を防止）
- [x] 3. completeMultipartUpload リトライ最適化 — パート検証を retry 外へ分離

## Priority 2 (Medium)

- [x] 4. concurrent uploadPart の同一パート番号書き込み保護（PartDescriptor をトランザクション化）
- [x] 5. deleteByPrefix の readTS をバッチ毎に更新（rolling snapshot）
- [x] 6. BucketMeta fence の冗長 PUT 削除（Complete 内） — #3 で同時解決

## Priority 3 (Test gaps)

- [x] 7. テスト追加: Complete ETag 不一致 (InvalidPart)
- [x] 8. テスト追加: Complete EntityTooSmall (前パートサイズ不足)
- [x] 9. テスト追加: Presigned URL 誤認証情報
