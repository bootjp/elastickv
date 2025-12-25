# Jepsen 本番相当テストを行うための構成メモ

本家 Jepsen Redis テストに近いレベルで Elastickv/Redis プロトコルを検証するために必要な構成要素と TODO です。

## 1. クラスタ構成
- 最低 5 台のノードを用意する:
  - 1 台: コントロールノード（Jepsen runner）
  - 4〜5 台: DB ノード（Elastickv を起動）
- 全ノードで SSH 鍵認証（root または sudo 無パス）をセットアップ。
- `/etc/hosts` 等でノード名解決をそろえる。

## 2. DB ドライバの拡張
- `jepsen/src/elastickv/redis_workload.clj` を拡張し、`jepsen.os.debian` などを使って以下を実装:
  - 各ノードへのバイナリ配布（ビルド or 配布）。
  - 起動/停止/リスタートを `Remote` 経由で管理。
  - データ/ログディレクトリをノードごとに初期化・掃除。
  - ポート割り当て (Redis/GRPC/Raft) をノードごとに設定。

## 3. 故障注入 (Nemesis)
- 本家に近いセットを有効化する:
  - ネットワーク分断（partition-random-halves, majorities-ring 等）
  - プロセス kill / 再起動
  - ノード停止/再起動（必要なら再デプロイ）
  - 時刻ずれ（clock-skew）
- `jepsen.nemesis` を使い、複数 nemesis の組み合わせをテスト。

## 4. ワークロード
- 既存 append ワークロードに加え、以下を追加検討:
  - 混合 read/write、トランザクション長バリエーション
  - キー数増加、コンカレンシ/レート可変
  - テスト時間 5〜10 分 / 試行、複数試行

## 5. バイナリ配布/ビルド
- DB ノード上で Elastickv をビルドするか、事前ビルド済みバイナリを `scp` 配布。
- systemd/supervisor か Jepsen 管理スクリプトでプロセスを起動・監視。

## 6. 計測と証跡
- Raft/Redis/アプリログをノード別に収集。
- Jepsen history/analysis の保管。
- 可能なら tcpdump やメトリクス（Prometheus/Grafana）も併設。

## 7. CI への統合
- GitHub Actions 単体では不足するため、外部の VM/ベアメタルクラスタを用意し、Actions から SSH で制御するワークフローを作成。
- サブモジュール（`jepsen/redis`）取得を忘れず `submodules: recursive` を設定。

## 8. 作業ステップ案
1. ワークロードドライバを `db` / `os` / `nemesis` 付きに書き換える。
2. デプロイ・起動スクリプトを用意（ビルド or 配布を選択）。
3. 小規模 (2~3 ノード) でローカル VM を用いた疎通テスト。
4. 故障注入を広げ、本家標準セットに近づける。
5. 外部クラスタで長時間テストを回し、結果を保存。
