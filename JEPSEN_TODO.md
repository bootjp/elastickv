# Jepsen 本番相当テストを行うための構成メモ

本家 Jepsen Redis テストに近いレベルで Elastickv/Redis プロトコルを検証するために必要な構成要素と TODO です。

## 1. クラスタ構成
- [x] 最低 5 台のノードを用意する:
  - 1 台: コントロールノード（Jepsen runner）
  - 4〜5 台: DB ノード（Elastickv を起動）
- [x] 全ノードで SSH 鍵認証（root または sudo 無パス）をセットアップ。
- [x] `/etc/hosts` 等でノード名解決をそろえる。

## 2. DB ドライバの拡張
- [x] `jepsen/src/elastickv/redis_workload.clj` を拡張し、`jepsen.os.debian` などを使って以下を実装:
  - [x] 各ノードへのバイナリ配布（ビルド or 配布）。
  - [x] 起動/停止/リスタートを `Remote` 経由で管理。
  - [x] データ/ログディレクトリをノードごとに初期化・掃除。
  - [x] ポート割り当て (Redis/GRPC/Raft) をノードごとに設定。
  - [x] 起動時のサービス待機（gRPCポート待機など）の実装。

## 3. 故障注入 (Nemesis)
- [x] 本家に近いセットを有効化する:
  - [x] ネットワーク分断（partition-random-halves, majorities-ring 等）
  - [x] プロセス kill / 再起動 (server 側の bootstrap エラー無視対応済み)
  - [x] ノード停止/再起動（必要なら再デプロイ）
  - [x] 時刻ずれ（clock-skew）
- [x] `jepsen.nemesis` を使い、複数 nemesis の組み合わせをテスト。

## 4. ワークロード
- [x] 既存 append ワークロード (基本動作確認済み)
- [ ] 以下を追加検討:
  - [ ] 混合 read/write、トランザクション長バリエーション
  - [ ] キー数増加、コンカレンシ/レート可変
  - [ ] テスト時間 5〜10 分 / 試行、複数試行 (現在は 60秒程度で検証中)

## 5. バイナリ配布/ビルド
- [x] DB ノード上で Elastickv をビルドするか、事前ビルド済みバイナリを `scp` 配布。
- [x] systemd/supervisor か Jepsen 管理スクリプトでプロセスを起動・監視。

## 6. 計測と証跡
- [x] Raft/Redis/アプリログをノード別に収集。
- [x] Jepsen history/analysis の保管。
- [ ] 可能なら tcpdump やメトリクス（Prometheus/Grafana）も併設。

## 7. CI への統合
- [x] GitHub Actions 単体では不足するため、外部の VM/ベアメタルクラスタを用意し、Actions から SSH で制御するワークフローを作成。
- [x] サブモジュール（`jepsen/redis`）取得を忘れず `submodules: recursive` を設定。

## 8. 作業ステップ案
1. [x] ワークロードドライバを `db` / `os` / `nemesis` 付きに書き換える。
2. [x] デプロイ・起動スクリプトを用意（ビルド or 配布を選択）。
3. [x] 小規模 (2~3 ノード) でローカル VM を用いた疎通テスト。
4. [x] 故障注入を広げ、本家標準セットに近づける (Partition, Kill, Clock 対応)。
5. [ ] 外部クラスタで長時間テストを回し、結果を保存。

## 9. 実装済みの足回り (最新ステータス)
- **Vagrant Cluster**: `jepsen/Vagrantfile` と `jepsen/VM.md` でコントロール + 5 ノードの VM クラスタ構築済み。
- **DB Adapter**: `jepsen/src/elastickv/db.clj` 実装済み。Go バイナリのビルド/配布、起動・停止、Raft 参加、再起動時の待機処理を管理。
- **Fault Tolerance**: `elastickv` サーバー側で `raft.ErrCantBootstrap` を無視する修正を適用し、Kill Nemesis 耐性を獲得。
- **Workload**: `jepsen/src/elastickv/redis_workload.clj` は Debian/SSH/combined nemesis（partition, kill, clock）対応済み。
- **CI**: `.github/workflows/jepsen.yml` で self-hosted ランナー向けワークフローを用意。
- **Result**: `partition`, `kill`, `clock` 障害下での `append` ワークロードテストをパス (`{:valid? true}`)。