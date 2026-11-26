# Kairo

> **Kairo は汎用的な Project / Task オーケストレーションの意味論を担いますが、プロジェクト固有の状態は一切解釈しません。**

Kairo は、数台のマシン上で協調的な ML ワークロードを安全にオーケストレーションするためのシステムです。1 台のデーモン（`kairo serve`、自身の Executor を持つ構成も可能）と、他のホスト上で動作するノードエージェント（`kairo agent`）で構成されます。`[[executors]]` や `[[providers]]` を持たず、`[node]` の識別情報のみを備えた制御専用（control-only）デーモンも有効な構成です。この場合、デーモンが API の提供と各種リコンサイラの実行を担い、ジョブを実行するすべてのホストで `kairo agent` を稼働させます。

Kairo は次の 2 つのレイヤーに分かれています。

- **宣言的オーケストレーション層**: 小さな宣言的レイヤーであり、ProjectSpec を保存し、静的な Task DAG をリコンサイル（reconcile）し、明示的に選択された汎用タスクポリシーを適用します。
- **コーディネーション層**: イミュータブルな Execution リクエストを受け取り、Attempt、Lease、Command、Provider、および Executor の仕組みを担います。

Task は永続的な論理オブジェクトであり、一連のイミュータブルな Execution を持つことができます。一方、Execution、Attempt、Lease はそれぞれ別個の物理ライフサイクルオブジェクトとして維持されます。コーディネーション境界においては、プロジェクト・キュー・Task の名前は不透明（opaque）なコーディネーションスコープとして扱われ、オーケストレーション層が自身の論理的な Task を意図的にそれらのスコープへマッピングします。

オーケストレーション層が理解するのは、「依存関係が満たされた」「Execution が終了した」「選択されたポリシーにより継続（continuation）が許可された」「Task が成功・失敗・ブロック状態になった」といった汎用的な事実のみです。メトリクス、モデル品質、アーティファクトの内容、データセットなどのドメイン固有の状態は一切解釈しません。また、DAG トポロジは静的であり、動的ファンアウト、実行時の DAG 変更、アーティファクト由来のトポロジ生成、メトリクスゲート、cron スケジューリング、クリーンアップ用言語といった機能はありません。

## 目次

- [アーキテクチャ](#アーキテクチャ)
  - [プロセス構成](#プロセス構成)
  - [レイヤー構成](#レイヤー構成)
  - [Execution のライフサイクル](#execution-のライフサイクル)
  - [エージェントプロトコル](#エージェントプロトコル)
  - [常に維持される不変条件](#常に維持される不変条件)
- [クイックスタートと基本操作](#クイックスタートと基本操作)
  - [一時停止と再開](#一時停止と再開)
  - [ProjectSpec・タスクポリシー・デフォルト値・環境変数](#projectspecタスクポリシーデフォルト値環境変数)
  - [Task のキャンセル・チェックポイント・再実行・整理](#task-のキャンセルチェックポイント再実行整理)
  - [観測専用モードとリソース・ハードウェア履歴](#観測専用モードとリソースハードウェア履歴)
- [CLI からの Task 監視とログ確認](#cli-からの-task-監視とログ確認)
- [認証と TLS](#認証と-tls)
  - [TLS](#tls)
- [HTTP API](#http-api)
  - [エンドポイント一覧](#エンドポイント一覧)
  - [プロジェクトと Task の操作](#プロジェクトと-task-の操作)
  - [Attempt ログの取得](#attempt-ログの取得)
  - [ページネーション](#ページネーション)
  - [クライアント契約とエラーコード](#クライアント契約とエラーコード)
- [コーディネーションモデル](#コーディネーションモデル)
  - [エージェント再起動と引き継ぎ](#エージェント再起動と引き継ぎ)
  - [アドミッション制御とプランニング](#アドミッション制御とプランニング)
  - [実行時間の推定](#実行時間の推定)
  - [Executor のティック処理](#executor-のティック処理)
  - [Attempt ログ](#attempt-ログ)
  - [電力と消費電力量](#電力と消費電力量)
  - [マシン宣言](#マシン宣言)
  - [コードスナップショット](#コードスナップショット)
  - [コンテナ実行](#コンテナ実行)
  - [データストア](#データストア)
  - [ハードウェア設定](#ハードウェア設定)
  - [ノード隔離](#ノード隔離)
  - [ノードフェンス](#ノードフェンス)
  - [強制停止](#強制停止)
  - [Gang（廃止）](#gang廃止)
- [Loader（ストリーム）](#loaderストリーム)
- [Worker SDK](#worker-sdk)
  - [管理対象 Attempt に渡される環境変数](#管理対象-attempt-に渡される環境変数)
  - [セーフポイントの挙動と Python・Rust SDK](#セーフポイントの挙動と-pythonrust-sdk)
  - [プロジェクト管理の低レベルコントローラポリシー](#プロジェクト管理の低レベルコントローラポリシー)
- [パイロット受け入れ契約](#パイロット受け入れ契約)
- [ハードウェアベンチマーク](#ハードウェアベンチマーク)
  - [ベンチマーク実行](#ベンチマーク実行)
  - [プリミティブ](#プリミティブ)
  - [記録とプロファイル](#記録とプロファイル)
  - [安全設計](#安全設計)
  - [GPU ベンチマーク](#gpu-ベンチマーク)
- [データベース](#データベース)
- [稼働中環境のアップグレード](#稼働中環境のアップグレード)
  - [運用スクリプト](#運用スクリプト)
  - [自己バックアップ](#自己バックアップ)
  - [Durable Store からの環境再構築](#durable-store-からの環境再構築)
- [検証とテスト](#検証とテスト)
  - [GitHub 上の CI](#github-上の-ci)
  - [Kairo 上の CI](#kairo-上の-ci)

## アーキテクチャ

システム全体の見取り図です。各要素の詳細は、記載されたセクションで解説しています。

### プロセス構成

- `kairo serve`（デーモン）: API、プランニング、およびすべてのリコンサイル（reconcile）を担当し、状態は PostgreSQL に保持します（[データベース](#データベース)）。自身の Executor を動かすこともできますが、制御専用デーモンの場合は Executor を持ちません。
- `kairo agent`（ジョブを実行するすべてのホスト上で稼働）: 自身の Executor（そのノード上で Attempt を実行する各手段）、Provider（GPU、CPU・RAM、ファイルシステム）、および pack サーバを動かします（[データストア](#データストア)）。
- `kairo controller`（物理ホストごとに 1 つ）: デバイス設定とコンテナのトリム（container trims）を担当します（[ハードウェア設定](#ハードウェア設定)）。
- CLI および各 Attempt のプロセス内で利用する Worker SDK（Python、Rust）（[Worker SDK](#worker-sdk)）。

### レイヤー構成

| レイヤー | 担当内容 | パッケージ | 関連セクション |
|---|---|---|---|
| `orchestration` | ProjectSpec、Task（イミュータブル）、ポリシーによる判断、再実行（rerun） | `orchestration`, `store`（`orchestration`, `task_control`, `task_views`） | [クイックスタートと基本操作](#クイックスタートと基本操作), [CLI からの Task 監視とログ確認](#cli-からの-task-監視とログ確認) |
| `coordination` | Execution（ワンショット）、Attempt、Lease、Command、スコープゲート（Scope gate）、隔離（quarantine）、強制停止 | `store`（`execution`, `scheduler`, `worker`, `scopes`, `quarantine`, `forcestop`）, `execution` | [コーディネーションモデル](#コーディネーションモデル) |
| `planning` | 環境全体に対する 1 回のパスでの処理: 配置、バックフィル、プリエンプション、実行時間の推定、アドミッション計上 | `store`（`planner`, `preemption`, `estimate`） | [アドミッション制御とプランニング](#アドミッション制御とプランニング), [実行時間の推定](#実行時間の推定) |
| `execution` | Executor のバックエンドを通じた起動（ランタイムのコンテナ内: `executor/backend.go`、`executor/oci.go`）、shim と引き継ぎ（adoption）、コンファインメント（Confinement）、強制停止、静止（quiescence）、使用量計測、観測 | `executor`, `processidentity`, `provider`, `agentclient` | [エージェント再起動と引き継ぎ](#エージェント再起動と引き継ぎ), [コンテナ実行](#コンテナ実行), [強制停止](#強制停止) |
| `data` | ストア（store）、`scratch`・`output`・`cache` ディレクトリ、データオブジェクト・pack・コピー・レプリカ、インポート・Run スナップショット・チェックポイント・時系列データ、フェッチ・エビクション・リードバック（read-back）・インベントリ | `store`（`packs`, `copies`, `replicas`, `deletions`, `imports`, `inputs`, `placements`, `evictions`, `temporal`, `regenerate`, `transfer`, `runsnapshots`, `checkpoints`, `inventory_durable`, `scrub`, `shed`, `control_snapshot`）, `executor`（ストアワーカー `storeworker.go`: `datastore`, `pack`, `transfer`, `fetch`, `retire`, `evict`, `inventory`, `storefs`）, `machine`, `datainput` | [データストア](#データストア), [自己バックアップ](#自己バックアップ) |
| `loader` | ストリーム: raw データの取得と照合、決まった順番（混合・シャッフル・rank への分割）、stage、ソケットでの受け渡し、位置 | `loader` | [Loader（ストリーム）](#loaderストリーム) |
| `provenance` | コードスナップショット、ダイジェスト指定のコンテナイメージ | `sourcesnap`, `imageref`, `store/provenance` | [コードスナップショット](#コードスナップショット), [コンテナ実行](#コンテナ実行) |
| `hardware` | マシン宣言、デバイス設定、動作点ポリシー、ベンチマーク、電力と消費電力量 | `machine`, `devicecontrol`, `hwpolicy`, `hwbench`, `bench`, `cpupower`, `store`（`device`, `device_policy`, `device_samples`, `hardware_bench`, `hardware_history`, `hosts`） | [マシン宣言](#マシン宣言), [ハードウェア設定](#ハードウェア設定), [ハードウェアベンチマーク](#ハードウェアベンチマーク), [電力と消費電力量](#電力と消費電力量) |
| `access` | HTTP API とエージェント操作、トークン、TLS、プライベートファイル | `api`, `agentops`, `tlsutil`, `secfile`, `store`（`tokens`） | [認証と TLS](#認証と-tls), [HTTP API](#http-api) |
| `support` | 決定ロック（decision lock）配下のストアトランザクションとイベントログ、設定、環境変数ルール、ID、時刻、外部コマンド、ビルドスタンプ、テスト | `store/dbtx`, `config`, `envspec`, `id`, `instant`, `extcmd`, `buildinfo`, `conformance`, `pgtest` | [データベース](#データベース), [検証とテスト](#検証とテスト) |

### Execution のライフサイクル

1. `kairo project apply` が宣言を保存し、各新規 Task をそのコードスナップショットおよびイメージに紐づけます。
2. オーケストレーションポリシーが、依存先 Task がすべて成功した時点でその Task の Execution を投入します。
3. プランニングパスが配置先を決定し、ノード上に Lease を予約します（配置できない場合は待機理由を記録します）。
4. 該当ノードの Executor が Lease を準備し、Attempt 起動の認可を得て shim（`__kairo-shim`）を起動・有効化します。その後、shim がコマンドを起動します（Execution で指定されている場合、またはデータポリシーがすべての起動にコンファインメントを適用している場合は、コンファインメントされた状態で起動します）。
5. Executor が終了（exit）を記録し、その Attempt のすべてのプロセスが存在しなくなった時点で Lease が解放されます。
6. ポリシーが次の遷移先を判断します（成功、チェックポイント後の継続（continuation）、またはブロック状態の Task）。
7. 実行中を通じて、ジョブが書き込んだデータは pack され、durable store へコピーされて保持されます。

### エージェントプロトコル

- エージェントがデーモンへ接続します。デーモンからエージェントへ接続することはありません。
- エージェントは自身のノード、Executor、Provider を登録し、引き継ぐ（adopt する）Attempt を申告します。
- その後、各 Executor は 250 ms ごとにティックを実行します。各ティックでは観測結果を報告し、指示された操作（静止（quiescence）チェック、終了処理、強制停止、pack、コピー、フェッチ、削除、エビクション、リードバック（read-back）、`scratch` の精算）を実行したうえで、次の Execution を予約します。
- すべての操作は `POST /api/agent/<operation>` であり、指定された対象を所有するノードと照合して検証されます。各操作はリクエスト・レスポンス・`observe_only` 時のルールとあわせて `internal/agentops` に 1 箇所だけ宣言されており、デーモンがこれを提供し、エージェントのクライアントがこれを呼び出します。
- 指示は 2 回実行しても同じ結果になるように設計されており、Lease のエポック（lease epoch）によって `stale` な呼び出し元からの操作をフェンス（fence）します。

### 常に維持される不変条件

- デーモンが意思決定を行い、エージェントは指示の実行と事実の報告のみを担います。Kairo はプロジェクトの状態を一切解釈しません（終了コード 0 は、プロジェクト固有の意味での作業成功を意味するものではありません）。
- 宣言された Task はイミュータブルであり、1 つの Execution が開始されるのは最大 1 回です。
- Lease が解放されるのは、その Attempt に属するすべてのプロセスが存在しなくなり、かつそれ以降の観測で対象リソースが未占有であることが確認された場合のみです。時間の経過だけで Lease が解放されることはありません。
- Kairo がストアのルート配下で操作するのは自身が管理する領域だけです。durable store 上で削除するのは、自身が書き込んだものの未記録のファイルと、ストアの予約空き容量（reserve）を下回らないように維持するために削除する「他の 2 つの durable store 上に検証済み（`verified`）のコピーが存在するコピー」のみです。
- すべての Attempt は Kairo が割り当てた領域にのみ書き込みます（コンファインメント / Confinement）。Attempt が書き込んだデータは、書き込まれた瞬間から Kairo が保持する対象となります。

## クイックスタートと基本操作

デーモンは状態を PostgreSQL データベースに保存します（[データベース](#データベース)を参照）。最初に空のデータベースを 1 つ作成し、`database_url`（または環境変数 `KAIRO_DATABASE_URL`）でその接続先を指定してください。

```powershell
# TLS 用マテリアル: プライベート CA と、クライアントがデーモンへの接続に使うすべてのアドレス向けのサーバ証明書（後から kairo tls issue で再発行可能）
go run ./cmd/kairo tls init --dir local/tls --host 127.0.0.1 --host localhost --host 192.0.2.12
go run ./cmd/kairo serve --config examples/kairo.toml     # スキーマを作成
# 別ターミナルにて: オペレータ用トークンを作成し、CLI の参照先に保存
go run ./cmd/kairo token create --config examples/kairo.toml --name operator --role admin --save
go run ./cmd/kairo doctor
go run ./cmd/kairo project validate examples/project-spec.toml
go run ./cmd/kairo project apply examples/project-spec.toml
go run ./cmd/kairo project status example-project
go run ./cmd/kairo task list example-project
go run ./cmd/kairo task wait example-project train
go run ./cmd/kairo task logs example-project train --follow
go run ./cmd/kairo execution submit examples/execution.toml
go run ./cmd/kairo execution list --project example-train
go run ./cmd/kairo project pause example-train
go run ./cmd/kairo project resume example-train
go run ./cmd/kairo resource status
go run ./cmd/kairo machine apply examples/machines.toml
go run ./cmd/kairo machine show
go run ./cmd/kairo hardware show
```

サンプル設定ファイル（`examples/kairo.toml`）は `observe_only = true` でループバック上の平文 HTTP を待ち受けるため、CLI からは `--api http://127.0.0.1:7474` または環境変数 `KAIRO_API` の指定が必要です。他のホストから到達可能なデーモンでは、後述の [TLS](#tls) 設定が必要になります。なお、CLI が平文 HTTP 経由でトークンを送信するのはループバックアドレス宛ての場合に限られます。また、CLI のフラグは位置引数よりも前に指定してください。

### 一時停止と再開

`pause` を実行すると、選択したコーディネーションスコープが閉じられ、並行する新規起動がフェンスされます。デフォルトでは、捕捉されたすべての Execution が `quiesced`（静止）状態となり、それらの Lease が解放されるまで待機します。`--no-wait` を指定した場合は、永続的な一時停止操作が保存された時点で即座に返ります。

`resume` はアドミッションを再開するだけです。中断（suspend）コマンドを取り消したり、次にどの Execution を実行すべきかを判断したりすることはありません。

### ProjectSpec・タスクポリシー・デフォルト値・環境変数

#### ProjectSpec の適用と外部依存

`project apply` は宣言を一度保存する操作であり、その後のリコンサイル（reconcile）はデーモンが行います。正規化後に同一となる宣言を再適用する操作はべき等です。

宣言済みの Task はイミュータブルです。後から完全な ProjectSpec を適用して新しい Task を追加することはできますが、既存の Task を変更することはできません。また、宣言から既存 Task を省略してもキャンセルはされません。

Task の `depends_on`（およびそれに伴う `task = "T"` 入力）には、そのファイル内では宣言していないものの、別ファイルによって同じプロジェクト内に既に宣言されている Task を指定できます（たとえば、新しい実験の実行結果と以前の実行結果を比較する評価用 Task など）。デーモンは適用時にこの依存先を解決し、プロジェクトに該当する Task が存在しなければ適用を拒否します。解決された依存先は、そのファイル自身の Task と同様に依存対象として扱われます。`kairo project validate` はこうした外部依存の Task 名を一覧表示します（`--json` 指定時は `external_dependencies` フィールドに出力）。以前に宣言された Task はそれより前に宣言された Task にしか依存できないため、外部依存によって循環（サイクル）が生じることはありません。

#### タスクポリシーと実行先指定・`[defaults]`

初期ポリシーである `RunToCompletion@v1` は、すべての依存先 Task が成功するのを待って Task の初回 Execution を投入し、「終了コード 0」「Attempt の静止（quiescence）」「Lease の解放」のすべてが揃った時点で初めて Task を成功とみなします。チェックポイント付きの協調的中断（cooperative suspend）で不透明な継続（continuation）が返された場合は、次のイミュータブルな Execution を作成します。Attempt の識別情報が失われた場合は Task がブロック状態となります（後述の `continue_after_loss` が宣言されている場合を除く）。また、依存先 Task が成功しなかった場合、後続 Task はすべてブロック状態となります。

宣言フォーマットの詳細は `examples/project-spec.toml` を参照してください。`[tasks.execution.executor]` には `labels` のほか `node` を指定でき、指定するとそのノード上の Executor のみが Task を引き受けます。また、`[[tasks.execution.exclusive]]` には `device`（UUID による単一 GPU 指定、`count = 1`）を指定できます（`kairo hardware bench` の Task と同様の仕組みです）。

トップレベルの `[defaults]` テーブル（`queue`、`policy`、`execution`）は、バリデーション前に CLI によってすべての Task へマージされます。

- Task 自身の値が優先されます。
- テーブルはキー単位でマージされます。
- 配列とスカラー値は上書き（置換）されます。
- `depends_on` が省略されている場合は `[]` になります。
- ダイジェストは展開後の完全な Task に対して計算されるため、`[defaults]` の有無によってデーモンが保存する内容が変わることはありません。

#### 環境変数（`env`）と `argv` の扱い

`[tasks.execution.env]`（および Execution TOML の `[env]`）を使うと、`powershell -Command '$env:...'` や `env A=B` のようなラッパーを使わずに Attempt の環境変数を設定できます。`[defaults.execution.env]` はキー単位でマージされます。

環境変数の適用優先順位は次のとおりです（後から適用されるものが優先）。

1. Executor 自身の環境変数を継承します。
2. 宣言された環境変数を適用します（`PATH` などの上書きも可能です。Windows では変数名を大文字・小文字を区別せずに比較します）。
3. Kairo が設定する環境変数（`KAIRO_*`、割り当て GPU 向けの `CUDA_VISIBLE_DEVICES`、`scratch_tmpdir` 有効時の `TMPDIR` / `TMP` / `TEMP`）を適用します。これらは宣言側で上書きできません。

また、次の制約があります。

- `KAIRO_` で始まる変数名は拒否されます。
- 排他的 GPU リクエストを持つ Execution では `CUDA_VISIBLE_DEVICES` の指定も拒否されます（CPU Task であれば `-1` を指定可能です）。
- `argv[0]` の探索には、引き続き Executor 自身の `PATH` が使われます。
- すべての Executor において、`argv` はシェルを介さずそのまま 1 語ずつプログラムへ渡されます。そのため、引数内の `$VAR` が展開されるのは `argv` 自体が起動したシェルによって処理される場合だけです。
- `env` は Task のダイジェストに含まれ、API の読み取り権限を持つすべてのクライアントに平文で保存・表示されます。シークレットの格納場所としては使用しないでください。

### Task のキャンセル・チェックポイント・再実行・整理

#### キャンセル（`kairo task cancel`）

`kairo task cancel [--dry-run] PROJECT [QUEUE] TASK` は、実行中でない Task（依存先待ち、`pending`、`blocked`、または一時停止中の Task）を終了させます。未開始の Execution をすべて取り下げ、Task を `cancelled` 状態にします。後続 Task は失敗した Task に依存する場合と同様にブロックされます。

既に開始され終了状態に達していない Execution がある間はキャンセルが拒否されるため、先に Task を一時停止するか、`task pause --force` を実行してください。既存のチェックポイントやその他の出力ファイルはそのまま残されます。なお、開始前の Execution を `execution withdraw` で取り下げた場合も、その Task は `cancelled` 状態になります。

#### 実行中 Task のチェックポイント作成（`kairo task checkpoint`）

`kairo task checkpoint [--wait] [--timeout 30m] PROJECT [QUEUE] TASK` は、Task の実行中のすべての Attempt に対し、次の安全なタイミングでチェックポイントを公開したうえで実行を続行するよう要求します。

- `checkpoint` コマンド（origin は `operator_checkpoint`）が発行され、suspend と同様に `accepted`、`checkpointing`、そしてパスを `continuation_ref` として含む `checkpointed` で応答されます。
- ただし、Attempt は `running`、Lease は `active` のまま維持され、作成されたチェックポイントは Attempt の継続（continuation）にはなりません（イベントも `checkpoint_published` ではなく `checkpoint_saved` となります）。
- 1 つの Attempt で同時に進行できるチェックポイント要求は最大 1 件です。進行中に再度要求すると、既存の要求がそのまま返されます。
- 実行中の Attempt が存在しない Task、またはチェックポイント非対応（not checkpointable）の Execution を持つ Task に対する要求は拒否されます。
- `--wait` を指定すると、応答完了時に公開された各パス（または拒否理由）が出力されます。コマンドの状態とペイロードは `GET /api/commands/{id}` でも取得できます。
- このコマンドより古い Worker SDK を使用しているワーカーはコマンドをスキップするため、保留（pending）のままになります。

デーモン自身も同じ仕組み（origin は `data_protection`）でチェックポイントを要求し、公開されたチェックポイントを保護・保持します（[実行中ジョブのチェックポイント保護](#実行中ジョブのチェックポイント保護)を参照）。

#### 再実行（`kairo task rerun`）と宣言の整理（`kairo project prune`）

`kairo task rerun PROJECT [QUEUE] TASK` は、`failed`、`stopped`、`cancelled`、または `blocked` 状態の Task について、すべての Execution が終了した後に同一の宣言で次の Execution（理由は `rerun`）を実行します。依存先が成功しなかったことだけを理由にブロックされていた後続 Task は `pending` へ戻り、依存先が成功すれば実行されます。なお、宣言内容を変更したい場合は、引き続き新しい Task 名が必要です。

`kairo task rerun` には次のオプションを指定できます（両方を併用して、修正したコードでチェックポイントから再開することも可能です）。

- `--from-checkpoint latest|COMMAND_ID`（チェックポイント対応 Task が対象）: 最初からではなく、その Task が公開したチェックポイント（`latest`、または `checkpointed` 応答済みの suspend / checkpoint コマンドの `COMMAND_ID`、`GET /api/commands/{id}` で確認可能）から再開します。新しい Execution は、チェックポイント付き中断の再開時と同様に、そのチェックポイント参照（`KAIRO_CONTINUATION_REF`）の継続（continuation）として作成されます。再実行は新しい Run なので、チェックポイントを公開した Run の output（チェックポイントは `/kairo/output` の中にあります）が pack されたあと、その中身をこの Run の output ディレクトリとして持ち込んでから起動します（[Run の output の持ち込み](#run-の-output-の持ち込み)）。配置はチェックポイントを公開した Attempt のノードへ計画され、CLI には使用されるチェックポイントが出力されます。チェックポイントを一度も公開していない Task や、該当 Task のチェックポイントではないコマンドを指定した場合は拒否されます。
- `--source current|SNAPSHOT`（ソース上で実行する Task が対象）: 宣言時に紐づけられたスナップショットとは別の、同じソースのスナップショット上で実行します。`current` を指定すると、そのソースに対して最新の `project apply` が送信したスナップショットを使用します（コードを修正して同じファイルを再適用しても既存 Task の宣言自体は変わらないため、このオプションで修正を反映させます）。あるいはスナップショット ID を直接指定することもできます。新しい Execution には実行に使用したスナップショットが記録される一方、Task 自体は元のスナップショットに紐づいたまま維持されるため、後続 Task や入力参照は引き続き同じ Task 名を指定できます。デーモンが保持していないスナップショットや別ソースのスナップショットは拒否されます。

`kairo project prune [--write] FILE` は、まだ実行可能な Task とそれらが必要とする依存先 Task を報告します（`--write` を指定すると、残す各 Task のダイジェストを維持したまま `FILE` を書き換えます）。

#### project の統合（`kairo project merge`）

Task の入力（`task`・`data`・`object`）は同じ project の中しか読めません。1 つの仕事を複数のリポジトリが別々の project として宣言していたときは、`kairo project merge FROM INTO [--rename OLD=NEW]... [--dry-run]` で FROM のすべてを INTO に移せます（`POST /api/projects/FROM/merge`）。

- **移すもの**: Task（スコープと名前、Execution・依存・rerun・判断の履歴ごと）、データオブジェクト、イベント、宣言、ソース。id はどれも変わりません。
- **queue**: INTO に同じ名前の queue があれば、そこへまとめます。なければ queue ごと移します。
- **名前のぶつかり**: INTO にも同じ名前の Task があれば、`--rename OLD=NEW` で FROM 側に新しい名前を付けます（付けなければ、ぶつかる名前をすべて挙げて拒否します）。改名した Task の宣言とダイジェストは新しい名前のものに書き換わるので、新しい名前で宣言し直せば同じ Task として扱われます。名前を変えなかった Task も、INTO の名前で宣言し直せば同じ Task です（Task のダイジェストに project 名は入りません）。
- **同じデータ名**: 両方にある名前を `data = NAME` で読むと、どちらのものでも最も新しいものを読みます（`--dry-run` と結果に出ます）。
- **拒否される場合**: FROM に走っているか待っている Execution がある、`pending`・`running` の Task がある、または FROM か FROM の queue の gate が閉じている。
- **FROM の扱い**: 空の project として残ります。各ノードの FROM のキャッシュ（`<store>/cache/<project scope id>`）は移さず、INTO のものがまた作られます。

### 観測専用モードとリソース・ハードウェア履歴

#### 観測専用モード（`observe_only = true`）

`observe_only = true` を設定すると、リソースおよび外部占有（`external_claim`）のインベントリ収集のみを行い、ジョブの予約・起動・中断・解放は一切行いません。Kairo の管理外で学習プロセスが動いているホストへ最初に導入する際の推奨モードです。

#### リソース状態の表示（`kairo resource status`）

`kairo resource status` は、ノードごとにリソース 1 件につき 1 行の表を出力します（GPU、続いて `cpu`、`ram`、`disk` の順）。各行には次の情報が表示されます。

- リソースの状態（隔離（quarantine）されている場合はその理由を含む）
- 最終観測時点の総量と空き容量（GPU はメモリと使用率、`cpu` はコア数）
- 稼働中の Lease が予約している量（`cpu`、`ram`、`disk`）
- 最新の電力・クロックサンプル:
  - `POWER`: GPU の消費電力と有効な電力上限（例: `289/320 W`）、`cpu` の場合は CPU パッケージの消費電力
  - `CLOCKS`: GPU の SM クロック、P-state、有効なクロックイベント理由（例: `1905 MHz P2 sw-power`。詳細は[電力と消費電力量](#電力と消費電力量)を参照）
- そのリソースを保持している Task（Lease の状態が `active` 以外の場合はその状態も併記）

表に続けて、根拠（evidence）付きの外部占有（`external_claim`）と、報告されているもののノードに宣言されていない GPU が表示されます。外部占有の `since` は現在の占有エピソードが最初に観測された時刻であり、一度解消されてから再び発生した場合は新しい時刻からカウントし直されます。

`--json` を指定すると、代わりに `GET /api/resources` のレスポンスがそのまま出力されます。JSON 内の各リソースには `total_bytes`、`free_bytes`、`utilization`、`reserved`、`holders`、および `device`（最新サンプル）も含まれます。なお、`kairo doctor` の末尾にも同じ表が出力されます。

#### 観測データとハードウェア履歴の保持・圧縮

リソース観測データは「現在の状態」を表すためのものであり、各リソースの最新 1 件のみが利用されます。それ以外の古い観測データは 1 時間経過後にデーモンが自動で削除します。

デバイスサンプルは 72 時間保持されます（各デバイスの最新 1 件は常に保持）。ハードウェア履歴も同様に 72 時間保持され、リソースごとに最大 30 秒に 1 件の観測データ（その時点で稼働中の Lease が予約していた量を含む）と、デバイスごとに同頻度のデバイスサンプルが記録されます。

`kairo db compact --config kairo.toml` を実行すると、期限切れデータをまとめて削除し対象テーブルに VACUUM を実行します。デーモンの稼働中に並行実行しても問題ありません（PostgreSQL の autovacuum も時間をかけて同等の処理を行います）。

#### ダッシュボード向けハードウェア履歴 API

ハードウェア履歴はダッシュボード向けに 2 つの読み取りエンドポイントから取得できます。いずれもクエリパラメータとして `since` と `until`（RFC 3339 形式。デフォルトは直近 24 時間で、`since` は 72 時間前より過去にはできません）および `key`（複数回指定可。特定の系列のみに絞り込み）を受け付けます。

- `GET /api/hardware/samples`:
  - 追加パラメータとして `step`（`60s` のような期間指定または秒数。30 秒未満、および指定ウィンドウ幅の 1/2000 未満にはなりません）を受け付け、`since` に揃えた `step` 幅のバケットごとに履歴を平均化して `{"since", "until", "step_seconds", "series"}` を返します。
  - 各系列は `{"key", "kind", "nodes", "resources", "t", "metrics"}` の形式です。`t` にはサンプルが存在する各バケットの開始時刻（Unix ミリ秒）が入り、各メトリクスにはバケットごとに 1 つの値が入ります（バケット内のいずれのサンプルにも値がない場合は `null`）。
  - **GPU**: UUID ごとに 1 つの系列となり、その GPU が宣言されているすべてのノードが `nodes` に列挙されます（`kind` は `gpu`。メトリクスは `util`・`mem_util`（%）、`mem_used`・`mem_total`（バイト）、およびデバイスサンプル由来の `power_w`・`power_limit_w`・`sm_clock_mhz`）。
  - **ノードの `cpu`**: `total`、`reserved`（ミリコア）、およびノード自身が読み取った CPU パッケージ電力 `power_w` を持ちます。
  - **`ram` と `disk`**: `used`、`total`、`reserved`（バイト）を持ちます。
  - **ホストの CPU パッケージ**: コントローラ（`kairo controller`）が読み取ったホスト単位の CPU パッケージ電力は `host/<host>/cpu` という系列（`kind` は `host_cpu`、メトリクスは `power_w`、`nodes` はそのホストに属するノード一覧）として提供されます。コンテナノードにおける CPU パッケージ電力は、この系列からのみ取得できます。
- `GET /api/hardware/leases`:
  - `{"since", "until", "leases"}` を返します。指定ウィンドウ内にリソースを保持していたすべての Lease が、リソースおよび開始時刻順で返されます。
  - 各要素は `{"key", "resource_id", "kind", "node", "lease_id", "state", "execution_id", "project", "queue", "task", "quantity", "start", "end"}` の形式です。
  - `start` は Lease が作成された時刻（アドミッションは予約時点から計上するため）、`end` は解放された時刻で、リソースを保持している間（`stale` な Lease を含む）は `null` になります。いずれも RFC 3339 形式です。

#### リソース予約の意味論と GPU 外部占有の検出

CPU、RAM、およびディスクの予約は OS レベルのハード制限ではなく、アドミッション計上のための仕組みです（計上方法の詳細は[アドミッション制御とプランニング](#アドミッション制御とプランニング)を参照）。GPU の排他制御は Kairo に参加する主体間でのコーディネーション保証であり、外部プロセスについては観測してスケジュール対象から除外することはできますが、物理的にフェンスすることはできません。

GPU 上の外部利用は次の基準で分類されます。

- **`external_claim`**: いずれの Attempt からも起動されていない計算（compute）プロセスが GPU 上に存在する場合。
- **`unattributed_activity`**: 説明のつく計算プロセスが存在しないにもかかわらず、1 GiB を超える GPU メモリが使用されている場合（Linux では別コンテナ上のワークロードがこの状態として観測されます）。
- **Windows の例外**: Windows 上でグラフィックスプロセス（`nvidia-smi pmon` で `G` または `C+G` として現れるコンポジタ、ブラウザ、ディスプレイ接続 GPU のデスクトップなど）が表示されている GPU のメモリ使用量は、WDDM がプロセスごとのメモリ使用量を報告せず切り分けができないため、それらのプロセスによるものとして扱い `unattributed_activity` には計上しません。

#### 所有者の使用（owner use）

Kairo が使うマシンには持ち主がいます（デスクトップの GPU でゲームをする、ホストで手作業をする、など）。HTCondor の「持ち主が戻ったら譲る」と同じ考えを、資源ごとに、checkpoint を活かして全ノードで行います（issue #21）。持ち主の使用は、その資源への種類 `owner_use` の claim です。

- **見つけ方（観測）**: ノードの provider が、Kairo の仕事ではないものによる使用を見つけて報告します。しきい値はエージェント設定の `[owner_use]` で変えられます。
  - **GPU**: 描画のプロセス（`nvidia-smi pmon -s um` の型 `G` / `C+G`。Windows で Kairo の仕事を動かす WSL の VM（`vmmem*`）は除く）が使う SM の割合の合計が `gpu_sm_percent`（既定 30%）以上。
  - **CPU**: コアの 90% 以上が使われていて、そのうち Kairo のコンテナ以外の分（全体から、Linux では Incus のインスタンス `kairo-*` の cgroup、Windows では wslc の VM の `vmmemwslc*` の CPU 時間を引いた分）が `cpu_percent`（既定 50%）以上。Kairo の分を読めないノードでは報告しません（Kairo 自身の仕事を持ち主の使用と取り違えないため）。
  - **RAM**: 使える量が `ram_free_mib`（既定は 2 GiB とメモリの 5% の大きい方）を下回った。
  - **持ち主がキーボードの前にいる**: 直近 10 分以内にキーボードかマウスの入力があれば、各しきい値を `*_present` の値（既定: GPU 10%、CPU 25%、RAM は 4 GiB とメモリの 10% の大きい方）に下げます。入力だけでは何も譲りません。Windows では、エージェントが入力を見られないセッション 0 で動くため、持ち主のセッションで `kairo input-report` を動かし、最後の入力時刻を `%LOCALAPPDATA%\kairo\input.json`（`input_file`）に書かせます（`ops/windows/install-input-report.ps1` がログオン時に動くタスク `kairo-input-report` として登録します）。2 分以上書かれていない記録は「いない」とみなします。
- **手動**: `kairo node owner-use NODE [--gpu UUID]... [--cpu] [--ram] [--for D]` で、そのノードの全資源（または指定したもの）を持ち主が使うと伝えます。`kairo node owner-done NODE` か `--for` の期限まで続きます。
- **譲り方**: 持ち主の使用がある資源には、新しい仕事を置きません（待機理由 `owner_use`。CPU か RAM なら、そのノードに置きません）。今使われている（手動のもの、または 90 秒以内に観測されたもの）間は、その資源を持つ仕事のうち checkpoint できるもの（`checkpointable` かつ `preemptible`）にサスペンドを送ります（origin `owner_use`）。
  - GPU は、その GPU を持つ仕事すべて。
  - CPU と RAM は、それを最も多く持つ仕事から 1 つずつ（前のサスペンドが終わってから、使用がまだ続いていれば次）。手動のものは全部。
  - 続き（continuation）は、ほかのノードへ移るか（[Run の output ディレクトリと配置](#run-の-output-ディレクトリと配置)）、その資源が空くのを待ちます。
  - checkpoint できない仕事は最後まで走ります（issue #44）。
- **戻り方**: 観測された使用は、最後に観測されてから 10 分たつと終わります（ゲームのメニューやビルドの合間ですぐ戻らないため）。

## CLI からの Task 監視とログ確認

すべてのコマンドグループは `help`、`--help`、`-h` に対応しています（例: `kairo task --help`、`kairo task wait --help`）。サブコマンドを打ち間違えた場合は候補が提示されます。なお、ProjectSpec で宣言された Task に対する `task` 系コマンドでは `QUEUE` を省略できます。

| コマンド | 出力内容 |
|---|---|
| `kairo task list PROJECT [--state S,..] [--match PREFIX] [--live] [--since D] [--all] [--json]` | 稼働中（live）の Task と直近 24 時間以内に更新された Task（`--all` で全 Task）を表形式（name、queue、state、execution、start、elapsed、および exit / reason / progress / pause）で出力し、続けてプロジェクトの状態別件数を表示 |
| `kairo task show PROJECT [QUEUE] TASK [--json]` | 単一 Task の詳細を表示: 宣言内容、`env`、依存先 Task と後続 Task、スコープゲート（閉じている場合は閉じた操作者と日時）、各 Execution とその Attempt・ノード・終了結果、出力オブジェクト（ID・状態・ライフサイクル・検証済みコピーの保存先、JSON では `outputs`）、ログのパスとサイズ、ログに関する問題、GPU の消費電力量（`energy: GPU 1.23 kWh (partial)`）、予約量と並べて表示されるプロセスの最大 RAM 使用観測値（`ram: peak 9.0 GiB seen (sampled every 15 s)`）、待機中 Execution の待機理由 |
| `kairo task logs PROJECT [QUEUE] TASK [--stream stdout\|stderr\|both] [--tail N] [-f] [--attempt latest\|all\|N] [--rank R]` | 最新 Attempt のログの末尾 N 行（デフォルト 50 行、`0` で全行）を出力（エージェントノード上の Attempt にも対応）。`-f` を指定すると、Attempt が終了してログの追記が止まるまで追従 |
| `kairo task wait PROJECT [QUEUE] TASK [TASK...] \| PROJECT --match PREFIX [--until terminal\|started] [--timeout D] [--stall D]` | 状態変化ごとに 1 行出力（`10:01:33 hello running -> succeeded`）。デーモンに到達できない間は `DOWN` / `UP`、実行中 Task のログが期間 `D` の間増えなければ `STALL` を出力。すべて成功（または開始）なら終了コード `0`、それ以外の結果で終わったものがあれば `1`、タイムアウトなら `2`、引数や参照エラーなら `3` で終了 |
| `kairo project watch PROJECT [--match PREFIX] [--exit-when-idle]` | 状態変化、進捗（Task ごとに 1 分に 1 回）、10 分ごとのサマリを出力。`--exit-when-idle` を指定すると `pending` や `running` の Task がなくなった時点で終了（監視対象に成功しなかった Task があれば終了コード `1`） |
| `kairo execution logs EXECUTION_ID`, `kairo logs ATTEMPT_ID` | Execution ID または Attempt ID を指定して同様にログを表示 |
| `kairo version` | CLI とデーモンそれぞれのビルド情報を表示（一致しない場合は `doctor` が警告を出力） |

終了済みの Attempt に要求されたストリームのログが存在しない場合、`task logs` はその Attempt について理由と（判明していれば）`scratch` パスを 1 行で出力します。これはエラーとしては扱われず、`-f` は追従すべきストリームが残っていない時点で終了します。一方のストリームだけが欠落している場合、CLI はその旨を報告しつつ残りのストリームを引き続き読み取ります。ただし、ログの検索失敗や読み取りエラーが発生した場合は非ゼロの終了コードで終了します。

プロジェクト・キュー・Task いずれかの閉じたスコープゲートによって足止めされている `pending` または `running` の Task は、自身の状態を維持したままになります。その Task の実行中処理がすべて止まった時点（継続（continuation）がゲートの背後で待機している状態）では注記に `paused: task gate closed by alice` のように表示され、Execution の中断が進行中の間は `pausing` と表示されます。一時停止は保存された Task 状態ではなくゲートの状態から得られるビューであるため、`--state` による選択対象にはなりません。

行指向の `wait` と `watch` はバックグラウンド監視に適しており、1 行が 1 イベントとして即座にフラッシュされます。`project status PROJECT` は引き続き JSON を出力し、`--task NAME`（複数回指定可）、`--match`、`--state`、`--live`、`--since` を受け付けます。`project apply` は宣言された Task を出力し（レスポンス全体を得るには `--json`）、競合時には異なる内容で既に宣言されていたすべての Task 名を示します。`project validate` は 1 行のサマリを出力します（展開後の Task を得るには `--json`）。なお、CLI がデーモンより新しく、デーモンが認識できないコマンドを実行した場合は、その旨が通知されます。

## 認証と TLS

すべてのリクエストには `Authorization: Bearer <token>` が必要です。ループバックからの接続も例外ではなく、認証なしで呼び出せるのは `GET /health` のみです。トークンが未指定・不明・失効済みの場合は `401`、トークンのロールがそのルートを許可していない場合は `403` を返します。トークンはランダムな 256 ビットの値で、SHA-256 ハッシュのみが保存されます。失効（revoke）は即座に反映されます。

| ロール | 呼び出し可能な範囲 | 利用元 |
|---|---|---|
| `read` | すべてのオペレータ向け `GET` | ダッシュボード |
| `admin` | すべてのオペレータ向けルート | CLI、`kairo-monitor`、プロジェクトコントローラ |
| `node` | `/api/agent/*`（単一ノード分） | ノードエージェント、ホストのコントローラ |
| worker | `/api/worker/*`（単一 Attempt 分） | Attempt のプロセス |

オペレータ用トークンとノード用トークンは、デーモンのデータベースを直接開いて管理します（データベースへのアクセス権が信頼の起点となります。デーモンが稼働中でも実行可能です）。`--config` を指定するとデーモンの設定ファイルからデータベース接続先を読み取り、代わりに `--database-url URL` で直接指定することもできます。どちらの指定方法でもスキーマは作成されません。デーモンが使用していないデータベースにトークンを作成しても認証には使えないためです。

```powershell
kairo token create --config kairo.toml --name operator --role admin --save
kairo token create --config kairo.toml --name dashboard --role read --out dashboard.token
kairo token create --config kairo.toml --name host-a-gpu --role node --node host-a-gpu
kairo token list --config kairo.toml
kairo token revoke --config kairo.toml host-a-gpu
```

平文のトークンは作成時に 1 度だけ表示されます。`--save` を指定すると、ユーザーの設定ディレクトリ（`%APPDATA%\kairo\token`、`~/.config/kairo/token`）に書き込まれます。

CLI と SDK の `KairoControllerClient` は、以下の共通の優先順位でデーモン・トークン・CA 証明書を解決します。

- **URL**: `--api`、なければ `KAIRO_API`、なければ `https://127.0.0.1:7474`
- **トークン**: `KAIRO_TOKEN`、なければ `KAIRO_TOKEN_FILE` で指定されたファイル、なければ `<config dir>/kairo/token`
- **CA**: `KAIRO_CA_FILE`、なければ `<config dir>/kairo/ca.pem`（存在する場合）、なければシステムのルート証明書

制御トラフィックは `HTTP(S)_PROXY` を経由せず、リダイレクトにも一切追従しません。

ノードトークンは対象のノードに紐づけられます。エージェントはそのノードのみを登録でき、すべての操作は対象として指定された Executor・Lease・Attempt・Provider・ログを所有するノードと照合されます。Executor・Provider・リソースが別のノードへ移動することはありません。`observe_only`（観測専用モード）のデーモンに対しては、エージェントのポーリングに実行すべき作業は返されず、状態を変更する操作は `423` で拒否されます。

各 Attempt には、起動認可時に専用のワーカートークンが発行され、環境変数 `KAIRO_ATTEMPT_TOKEN` として渡されます。このトークンはワーカー API に対して Attempt（および発行元の Lease とエポック）を識別するものであり、Attempt が `quiesced`（静止）になるか、デーモン再起動によって lost とマークされるなどしてフェンス（fence）されると無効になります。

ただし、ワーカーが起動してから Executor がその Attempt を `running` として記録するまでの数百ミリ秒の間（Attempt は認可済み、Lease は準備済みの状態）に API を呼び出した場合、デーモンはフェンスせず、`running` かつ Command なしとして応答し、ハートビートとプロセス情報を記録します。この扱いがなければ、起動直後にポーリングするサーバや即座に登録を行う rank が、自身の起動タイミングで停止させられてしまうためです。なお、Executor は自身のプロセスが持つ `KAIRO_*` 環境変数を Attempt に引き継ぎません。

### TLS

#### 証明書の発行と秘密鍵の保護

`kairo tls init --dir DIR --host H...` を実行すると、`ca.pem`、`ca-key.pem`、`server.pem`、`server-key.pem` が生成されます（ECDSA P-256。サーバ証明書は指定されたすべての `--host` の IP アドレスまたは DNS 名をカバーします）。新しいアドレス向けに同じ CA からサーバ証明書を再発行するには、`kairo tls issue --dir DIR --host H...` を使用します。

Kairo は実行中に `ca-key.pem` を一切読み込みません。`ca-key.pem` は 1 台の管理マシン上に保管し、デーモンのディレクトリやジョブを実行するホストには配置しないでください。

`kairo` が書き出す秘密鍵やトークンファイルは現在のユーザーのみにアクセスが制限されます。デーモンも起動時に自身の `database_password_file` に対して同じ制限を適用します。データベースに書き込める者は誰でもトークンを発行できるため、その認証情報は秘密鍵と同等に保護されます。POSIX のファイルモードが無視される Windows では、継承された ACL を置き換え、現在のユーザーと `SYSTEM` のみにアクセス権を付与します。

#### 証明書の検証と CA ローテーション

デーモンは、自身の証明書が `tls_ca_file` にチェーンしない場合、または `advertise_url` のホストをカバーしていない場合、起動を拒否します。CA ローテーション中は、`tls_ca_file`（およびエージェントの `ca_file`）に旧 CA と新 CA の両方を含めることができます。Attempt にはそのファイル内のすべての証明書が渡されます。

`kairo tls init` が作成する CA はパス長が 0 のため、後継の CA にクロス署名できません。そのため、稼働中のジョブを止めずに CA をローテーションする場合は、以下のようにバンドルを介して切り替えます。

1. 新しいディレクトリで `kairo tls init` を実行し、新 CA を作成します。旧 CA と新 CA の両方をすべての CA ファイル（デーモンの `tls_ca_file`、各エージェントおよび各コントローラの `ca_file`、CLI の CA ファイル）に格納し、それぞれを再起動します（エージェントは稼働中の Attempt を引き継ぎます）。
2. 手順 1 より前に起動された Attempt がすべて終了するまで待機します。Attempt は起動時に受け取った CA（`KAIRO_API_CA`）を保持し続けるため、旧 CA のみを持つ Attempt が残った状態でデーモンが新証明書を提示すると、次の呼び出しで失敗します。
3. デーモンと各エージェントの `[packs]` に対し、新 CA で発行した証明書（`kairo tls issue --dir NEW --host H...`）を配置します。すべての CA ファイルを新 CA のみにして、再度再起動します。
4. 旧 CA の秘密鍵、および旧サーバ鍵・旧 pack 鍵を削除します。

#### デーモンとクライアントの TLS 設定

デーモン側の設定例は以下のとおりです。

```toml
listen = "0.0.0.0:7474"
advertise_url = "https://192.0.2.12:7474"  # Attempt に通知される接続先 URL
tls_cert_file = "local/tls/server.pem"
tls_key_file = "local/tls/server-key.pem"
tls_ca_file = "local/tls/ca.pem"             # Attempt に渡される CA 証明書
```

デーモンは TLS 1.3 のみを提供します。`insecure_http = true` を明示しない限り、TLS なしで他ホストから到達可能なアドレスでのリッスンは拒否されます。また、TLS を有効にする場合、`advertise_url` は `https` でなければなりません。

クライアントは `ca.pem` を通じてデーモンを信頼します。

- **CLI・コントローラ**: `<config dir>/kairo/ca.pem` にコピーするか、`KAIRO_CA_FILE` でパスを指定する
- **ノードエージェント**: 設定ファイルの `ca_file` を設定する
- **Node.js ベースのツール**: `NODE_EXTRA_CA_CERTS=ca.pem` を設定して起動する
- **Attempt**: CA 証明書自体が `KAIRO_API_CA`（Base64 エンコードされた DER 形式）で渡されるため、ローカル・リモートを問わずファイルパスは不要

これらの証明書には失効確認エンドポイントが含まれていないため、Windows の `curl.exe`（Schannel）では `--cacert ca.pem` と併せて `--ssl-no-revoke` を指定する必要があります。

#### ノードエージェントの設定

ノードエージェントは `kairo agent --config agent.toml` で起動します。基本設定は以下のとおりです。

```toml
server_url = "https://192.0.2.12:7474"
token = "kairo_node_..."
ca_file = "/etc/kairo/ca.pem"
# attempt_api_url = "https://192.0.2.12:7474"  # このノード上の Attempt に渡す KAIRO_API_URL
```

`[owner_use]` は、このノードの持ち主の使用の見つけ方です（[所有者の使用](#所有者の使用owner-use)。既定で有効、`disabled = true` で無効。しきい値 `gpu_sm_percent`・`cpu_percent`・`ram_free_mib` と、持ち主がキーボードの前にいるときの `*_present`、`input_file`）。残りの設定項目（`log_directory`、`log_min_free_mib`、`observe_only`、`observation_interval_seconds`、`[node]`）は、デーモン設定（`examples/kairo.toml`）と同じ形式です。`[bench]` は拒否されます（ベンチマークはコンテナで走り、使うプログラムは Kairo が決めます）。`[[executors]]` と `[[providers]]` はエージェントだけが持ちます（デーモンはジョブを走らせないため、デーモンの設定に書くと拒否されます）。

- **Executor の実行形態**: すべての Executor は Attempt をイメージのコンテナで実行します。種別（kind）は `incus`（Linux）と `wslc`（Windows）だけです（[コンテナ実行](#コンテナ実行)を参照）。ホストで直接実行する種別（以前の `linux`・`windows` など）、Proxmox のコンテナの種別（以前の `pct`）、WSL の種別（`wsl2`）を宣言する設定は拒否されます。
- **未宣言 Executor の無効化**: エージェントが登録を行う際、そのノードに属していて現在は宣言されなくなった Executor は無効化されます（イベント `executor_undeclared`）。再起動時に何も引き継がなかった場合と同様にその Lease は `stale` になり、エージェントが再び宣言するまで、新たな配置もストアの提供も行われなくなります。
- **相対パスの基準**: `log_directory`（および `source_directory`）に相対パスを指定した場合、デーモンまたはエージェントの起動時の作業ディレクトリを基準に 1 度だけ解決され、Attempt には絶対パスが渡されます。
- **接続要件**: エージェントは `insecure_http = true` を指定しない限り `http://` の URL を拒否します。ホストのデバイスコントローラ（`kairo controller`。[ハードウェア設定](#ハードウェア設定)を参照）も同じ方法で接続します。

#### pack サーバの設定

あるノードのエージェントが作成した pack を別ノードのエージェントが durable store へコピーする場合（[データストア](#データストア)を参照）、配信元のエージェントは `[packs]` セクションで pack を提供します。以下の 4 項目はすべて指定するか、すべて省略するかのどちらかです。

```toml
[packs]
listen = ":7475"                 # リッスンするアドレス
addr = "192.0.2.14:7475"       # 他エージェントからの到達先アドレス（pack_addr）
cert_file = "/etc/kairo/pack.pem" # kairo tls issue --host 192.0.2.14 で発行した server.pem
key_file = "/etc/kairo/pack-key.pem"
```

証明書には、デーモンの CA がそのアドレス向けに発行したものを使用します。CA の `ca.pem` と `ca-key.pem` のコピーを置いた別ディレクトリ（デーモン自身の `server.pem` を上書きしてしまうため、デーモンのディレクトリは使わないこと）で `kairo tls issue --dir DIR --host 192.0.2.14` を実行すると、そのディレクトリに `server.pem` と `server-key.pem` が生成されます。

この証明書はデーモンの CA から発行されているため、取得側のエージェントは自身の `ca_file` によってすでに信頼しています。設定を有効にすると、そのエージェント配下のすべての Executor が属性の 1 つとして `pack_addr` を公開します。

## HTTP API

処理に 2 秒以上かかったリクエストは、デーモンが WARN レベルでログ出力します（`slow request`。ログにはルートパターン・HTTP ステータス・所要秒数・リモートアドレスが記録されます。実際のリクエストパスやヘッダは一切記録されないため、ID やトークンは含まれません）。SDK は 10 秒でリクエストを打ち切るため、デーモンが Attempt に対して遅くなりすぎている兆候をここで把握できます。

### エンドポイント一覧

| メソッド | パス | ロール |
|---|---|---|
| `GET` | `/health` | なし |
| `GET` | `/api/whoami` | 任意のトークン |
| `POST` | `/api/projects`（ProjectSpec の適用） | `admin` |
| `GET` | `/api/projects/{project}` | `read` |
| `GET` | `/api/projects/{project}/tasks/{task}` | `read` |
| `POST` | `/api/projects/{project}/tasks/{task}/cancel`, `/rerun`, `/checkpoint` | `admin` |
| `GET` | `/api/commands/{id}` | `read` |
| `POST`, `GET` | `/api/executions` | `admin`, `read` |
| `GET` | `/api/executions/{id}` | `read` |
| `POST` | `/api/executions/{id}/withdraw` | `admin` |
| `GET` | `/api/attempts`, `/api/events`, `/api/pause-operations/{id}` | `read` |
| `GET` | `/api/attempts/{id}/log` | `read` |
| `GET` | `/api/scopes`, `/api/scopes/{id}` | `read` |
| `POST` | `/api/scopes/{id}/pause`, `/api/scopes/{id}/resume` | `admin` |
| `GET` | `/api/nodes/quarantines` | `read` |
| `POST`, `DELETE` | `/api/nodes/{id}/quarantine` | `admin` |
| `POST` | `/api/nodes/{id}/fence` | `admin` |
| `GET` | `/api/resources` | `read` |
| `POST` | `/api/resources/{id}/enable`, `/quarantine`, `/reconcile` | `admin` |
| `GET`, `POST` | `/api/machines`（マシン宣言） | `read`, `admin` |
| `GET` | `/api/stores`, `/api/data/objects`（`?project=&kind=&limit=`） | `read` |
| `POST`, `GET` | `/api/sources`（コードスナップショット）、`/api/sources/{id}` | `admin`, `read` |
| `GET` | `/api/agent/sources/{id}/pack`, `/list` | `node` |
| `POST` | `/api/data/imports`, `/api/data/fetches`, `/api/data/drops` | `admin` |
| `GET` | `/api/hardware`（`?host=`） | `read` |
| `GET` | `/api/hardware/samples`, `/api/hardware/leases`（`?since=&until=&step=&key=`） | `read` |
| `POST`, `DELETE` | `/api/hardware/{host}/{device}/settings` | `admin` |
| `POST` | `/api/hardware/{host}/{device}/bench` | `admin` |
| `GET` | `/api/hardware/{host}/profile`（`?device=`） | `read` |
| `POST` | `/api/agent/{operation}`, `/api/agent/logs` | `node` |
| `POST` | `/api/worker/heartbeat`, `/api/worker/processes` | worker |
| `GET` | `/api/worker/commands` | worker |
| `POST` | `/api/worker/commands/{command}/acks` | worker |
| `GET` | `/api/worker/bench`, `/api/worker/bench/cpu-energy` | worker（ベンチマークの Attempt） |
| `POST` | `/api/worker/bench/settings`, `/start`, `/points`, `/kinds`, `/finish` | worker（ベンチマークの Attempt） |

エラーレスポンスの形式は `{"error": ..., "code": ...}` です。pause・resume・quarantine・cancel・rerun の各リクエストで `actor` が空の場合、呼び出しに使用したトークンの名前が記録されます。また、`/health` と `/api/whoami` のレスポンスにはビルドの `version` が含まれます。

### プロジェクトと Task の操作

**`GET /api/projects/{project}`** は以下のクエリフィルタを受け付けます。

- `task`: Task 名（複数指定可能）
- `match`: Task 名の前方一致
- `state`: カンマ区切りの状態リスト
- `live=1`: pending または running の Task に限定
- `since`: RFC 3339 形式（`updated_at` 基準）

フィルタ条件にかかわらず、`state_counts` にはすべての Task が集計されます。各 Task には `created_at` と `updated_at` が含まれ、各 Execution には Attempt、Executor、タイムスタンプ、および `detail` を除いた最新の進捗（progress）が含まれます。

**`GET /api/projects/{project}/tasks/{task}`** では上記に加え、宣言内容（`spec`）、依存先 Task、後続 Task、スコープゲート（閉鎖中のゲートには、そのゲートを閉じた pause 操作由来の `closed_by` と `closed_at` を含む）、さらに Execution ごとの Attempt 一覧（ログのパスとサイズ、`log_problems`、`energy` を含む）・`waiting` の理由が返されます。閉鎖中のゲートによって保留されている稼働中（live）の Task には、最も内側の閉鎖ゲートを示す `paused` が含まれます。

その他のプロジェクト・Task 操作の入出力は以下のとおりです。

- **適用（`POST /api/projects`）**: `{"project", "spec_digest", "idempotent", "created", "unchanged"}` を返す
- **キャンセル（`/cancel`）**: `{"actor", "dry_run"}` を受け取り、`{"task", "state", "withdrawn", "dry_run"}` を返す
- **再実行（`/rerun`）**: `{"actor", "request_id"}` を受け取り、`{"task", "state", "ordinal", "unblocked"}` を返す（同じ `request_id` を再送すると初回のレスポンスを返す）

対象 Task が操作に適さない状態にある場合は、`409 task_conflict` を返します。

### Attempt ログの取得

`GET /api/attempts/{id}/log?stream=stdout|stderr&offset=N&limit=M` は、デーモンが保持している Attempt のログ（エージェントから転送されたもの）を生バイト列で返します。

- **パラメータ**: `stream` のデフォルトは `stderr`、`limit` のデフォルトは `1 MiB`（最大 `8 MiB`）です。代わりに `tail=N` を指定すると、末尾 N バイトを返します。
- **レスポンスヘッダ**: `Kairo-Log-Size` に現在のサイズ、`Kairo-Log-Next-Offset` に次回の読み取り開始位置が返されます（今回のボディの開始位置は `Kairo-Log-Next-Offset` からボディ長を引いた値です）。
- **空ボディとエラーの挙動**:
  - オフセットが末尾を超えている場合、稼働中（live）の Execution でまだ記録されていないストリームを指定した場合、または記録済みのログがまだ転送されていない場合は、空のボディを返します。
  - Execution が終了状態（terminal）で、かつそのストリームに記録されたパスがない場合は `409 no_log` を返します。そのレスポンス JSON の `error` には終了原因が記載され、`scratch` パスが記録されていてストア上に `active` または `kept` として残っている場合はそのパスも含まれます。
  - 存在しない Attempt を指定した場合は変わらず `404 not_found` を返します。
- **配信範囲**: デーモンの `log_directory` 配下にあるファイルのみが配信されます。
- **CLI と SDK**: `kairo logs [--stream stdout|stderr] [--tail N] [--follow] ATTEMPT_ID` で出力でき（`--tail` は末尾 N バイト、`--follow` は Ctrl-C で終了するまで 2 秒ごとにポーリング）、Python SDK の `KairoClient.attempt_log` では 1 スライス分を読み取れます。

### ページネーション

`GET /api/executions`（`?project=&queue=&task=&scope_id=&state=`、投入日時の古い順）と `GET /api/attempts`（`?project=&execution_id=&state=`、認可日時の新しい順）は、最大 `limit` 件（デフォルト 100、上限 1000）を返します。

後続ページが存在する場合、レスポンスに次の開始位置を示す不透明カーソル `next` が含まれます。同じリクエストに `after=` でそのカーソルを付与すると続きを取得でき、最終ページには `next` が含まれません。Python SDK の `KairoClient.all_executions` と `all_attempts` はこのページネーションを自動でたどります。

### クライアント契約とエラーコード

クライアントは `sdk/conformance` に定義された契約（デーモンの探索方法、トークンを送信してよい条件、各エラーの分類方法）に従い、すべての SDK のテストがこの契約をリプレイします。

| エラーコード | HTTP ステータス | 内容 |
|---|---|---|
| `unauthorized` | `401` | 認証失敗（トークン未指定・不明・失効） |
| `forbidden` | `403` | 権限不足（ロールがルートを許可しない） |
| `not_found` | `404` | リソース未検出 |
| `stale_epoch` およびその他の競合 | `409` | 状態・エポックの競合 |
| `gate_closed` / `observe_only` | `423` | スコープゲート閉鎖中または観測専用モード |
| `bad_request` | `400` | リクエスト不正（リクエストを修正する） |
| `internal` | `500` / `503` | デーモン側の障害（`503` はデータベースが到達不能・過負荷・ロック取得を断念した場合。一時的なためリトライ可能） |

`GET /health` は `{"ok": true, "api": 1}` を返します（ビルドの `version` も含みます）。`api` は契約バージョンであり、破壊的変更がある場合にのみ更新されます。

## コーディネーションモデル

Kairo は、以下の 5 つのプリミティブによって実行状態とリソース所有権を厳密に調整します。

| プリミティブ | 役割・特性 | 状態・ライフサイクル |
|---|---|---|
| Execution request | イミュータブルかつ 1 回限り（one-shot）。1 つのリクエストが開始できるのは最大 1 回。 | `waiting` / `authorized` / `started` / `terminal` |
| Attempt | プロセスライフサイクルの事実（facts）を表す。 | `authorized` / `running` / `quiescing` / `exited` / `quiesced` / `lost` |
| Lease | リソース所有権の調整（ownership coordination）を表す。 | `reserved` / `prepared` / `active` / `releasing` / `released` / `stale` / `revocation_requested` |
| Command | 協調的な `suspend` 要求を少なくとも 1 回（at-least-once）配信する。 | `pending` / `accepted` / `checkpointing` / `checkpointed` / `rejected` |
| スコープゲート（Scope gate） | アドミッション可否を制御。予約（reservation）・認可（authorization）・有効化（activation）の各時点で世代フェンス（generation fence）を検査する。 | `open` / `closed` |

**Lease の解放と `stale` な Lease のリコンサイル**

チェックポイントの公開はプロセスの終了を証明せず、プロセスの終了も分散環境における静止（quiescence）を証明しません。Kairo が Lease を解放するのは、登録済みの全プロセスが存在しないことを確認し、かつその不在確認後に取得された Provider の観測結果でリソースが未占有（unclaimed）であると示された後だけです。

Executor が保証できなくなった Lease は `stale` になります。これは、その Executor のエージェントが当該 Attempt を引き継がず（adopt せず）に再登録したとき、または消失したエージェントのノードをオペレータがフェンスしたとき（`kairo node fence`。後述の[ノードフェンス](#ノードフェンス)参照）に発生します。時間経過だけで Lease が `stale` になったり解放されたりすることは決してなく、`stale` な Lease はリコンサイル（reconcile）されるまでリソースを保持し続けます。

デーモンが Attempt のプロセス消滅を証明できない場合、オペレータは次のコマンドでリコンサイルを実行する必要があります。

```sh
kairo resource reconcile RESOURCE_ID --confirm-process-absent
```

リコンサイルには、Lease が `stale` になった後に取得された各リソースの観測結果において、その Attempt のプロセス（自身の `KAIRO_ATTEMPT_ID` を持つプロセス、または登録済みプロセスのいずれか）が 1 つも見つからないことが必要です。なお、その Attempt 以外のプロセスによる占有（他プログラムのプロセス、帰属不明なアクティビティ、ゲームなど）は Lease を引き留めません。その占有自体がデバイスへの新規配置を妨げるため、オペレータが終了を待つ必要はありません。

一方で、以下のいずれかに該当する `stale` な Lease 配下では、プロセスがなお動作している可能性は存在しません。

- **コマンドを一度も起動していない Attempt**: 有効化（activation）前にロストしたもの。shim は有効化後にエージェントから送られる開始許可（go-ahead）を受け取って初めてコマンドを起動するため、エージェントが消失すればその開始許可も失われます。
- **Attempt が紐づいていない Lease**
- **開始後にノードが再起動された Attempt**: 登録時、Linux および Windows のエージェントはスリープや休止状態（hibernation）の時間も含む稼働時間（uptime）を報告するため、復帰が再起動と誤認されることはありません。デーモンは自身の時計でブート時刻を推定し、Attempt の開始時刻を記録します。時刻の不確実性を考慮し、推定ブート時刻が Attempt の開始時刻より 2 分超あとであることを条件とします。

これらに該当する Lease については、Lease が `stale` になって以降の新しい観測結果が各リソースについて揃い、かつ Attempt のプロセスが見られない時点で、デーモン自身が自動的に解放します（イベント `stale_lease_reconciled`、`by: daemon`）。手動リコンサイルを行う場合も、`--confirm-process-absent` による確認は不要です。

**終了済み Attempt の残留プロセス処理**

終了済みの Attempt が残したプロセス（ランチャーが終了してその終了が記録されているにもかかわらず、そのプロセスツリー上のプロセスがまだ動いている状態。未登録のまま起動した子プロセスや、ラッパーが残したサーバなど）は、もはや誰もその結果を読まないため本来の作業ではなく、しかも Lease を保持し続けてしまいます。

Executor はこのような残留プロセスを自動的に終了させます。

1. 検知されてから 10 秒経過すると停止を要求します（ランナーはすでに終了しているので、エージェントがランタイムでコンテナを止めます）。
2. さらに 30 秒経過すると強制終了（kill）します。対象となるのは、Attempt のコンテナ（ランタイムで即座に止めます）と、ランナーのプロセスグループ（Windows ではジョブオブジェクト）であり、[強制停止](#強制停止)（force stop）と同じ方法で特定されます。これら以外のプロセスが対象になることは決してありません。

なお、エージェント再起動に伴ってロストした Attempt は終了済み（`exited`）ではないため、この自動終了の対象にはならずそのまま残されます。

**GPU 上の外部プロセス占有（`external_process`）**

Provider が GPU 上で観測したコンピュートプロセス（`nvidia-smi pmon`）は、その GPU（いずれのノード行で管理されているかを問わない）を保持している Attempt の作業に該当しない限り、`external_process` の占有（claim）として扱われます。Attempt の作業とみなされるのは、以下のいずれかです。

- その Attempt の登録済みプロセス
- 登録済みプロセスの子孫プロセス（親プロセス連鎖（parent chain）でたどれ、各親プロセスが生存しており、かつ子プロセスより後に起動していないこと）
- Linux の場合、その Attempt の `KAIRO_ATTEMPT_ID` を環境変数に持つプロセス（[強制停止](#強制停止)で Attempt のプロセスを特定するのと同様に `/proc/PID/environ` から読み取る。Windows では親プロセス連鎖のみを参照）

この判定が成り立つのは、Attempt が `running` または `quiescing` 状態であり、かつ登録済みプロセスが生存している間だけです。登録済みプロセスより長く生き残った子孫プロセスは再び占有（claim）とみなされ、それが稼働している間は Lease も解放されません。

Lease 保持者に紐づけられないプロセス（別 Attempt のプロセス、親プロセスがすでに終了しているプロセス、環境変数を読み取れないプロセスなど）は占有（claim）のまま残ります。占有が存在する GPU は、プリエンプションを行う Execution にも割り当てられません。

### エージェント再起動と引き継ぎ

Attempt のランチャーは shim です。これは Executor の `kairo` バイナリを `__kairo-shim` として実行するもので、その PID、プロセスグループ（Linux）またはコンソールプロセスグループおよびジョブオブジェクト（Windows）、ならびに作成時刻（creation time）が、そのまま Attempt の識別情報となります。

起動は次の手順で行われます。

1. Executor が shim を起動し、Windows の場合は Attempt 用のジョブオブジェクト（名前は `kairo-attempt-<attempt id>`。shim にそのハンドルを渡すため、shim が稼働している間は名前が維持されます）に格納します。
2. Executor が Attempt を有効化（activate）し、ログディレクトリ配下の `running/<attempt id>.json` に記録します。
3. そこまで完了して初めて、shim が所定の環境変数・作業ディレクトリ・ログ設定のもとで Attempt のコマンドを起動します（Execution にコンファインメント指定がある場合はコンファインメント下で実行します。[データストア](#データストア)参照）。

このため、有効化（activation）が拒否された起動ではコマンドが一切実行されません。shim は `SIGTERM` および `CTRL_BREAK` を自身で止めずに通過させ（同じグループに属するコマンドにも同じシグナルが送られます）、コマンドの終了を待機したうえで、その終了コードを `running/<attempt id>.exit` に書き出してから同じ終了コードで終了します。Attempt の終了はこのファイルから記録されます（ファイルが書き出されずに shim 自体が終了していた場合は kill されたものとして記録されます）。

このようにエージェントは作業プロセスの直接の親ではないため、エージェント自身のプロセスのみを停止・再起動した場合でも Attempt は維持されます（Linux では systemd ユニットの `KillMode=process` を使用し、Windows では同じ実行イメージで動いている shim は終了させずエージェントのみを再起動します）。

エージェントは起動時に、配下の Executor に記録されている各 Attempt のうち、shim が依然として稼働中のもの、または終了ファイルを書き出して終了済みのものを引き継ぎ（adopt）ます。

- エージェントが登録時にそれら（Attempt、Executor、shim の PID、および作成アイデンティティ（creation identity））を申告すると、デーモンはそれぞれを従前の PID とアイデンティティのまま、その Executor 上で `running` または `quiescing` として維持します（イベント `attempt_adopted`）。
- それ以外の、その Executor が保持していた Attempt は従来どおりロスト扱い（`attempt_lost`）となり、記録も削除されます。
- 引き継がれた Attempt は shim が終了するまで 1 秒ごとに監視され、終了時に終了結果が記録されます。その間も、ログはデーモン側コピーの末尾から続けて転送され、リソース使用量がサンプリングされ、強制停止は名前で再取得したジョブオブジェクト（Windows）またはプロセスグループ（Linux）経由で到達し、`scratch` と `output` ディレクトリは使用中としてカウントされます。
- 終了ファイルを残さずに消滅した shim（強制終了された場合やノード再起動時）は引き継がれず、その Attempt はロストとなります。shim 導入前の旧エージェントで起動された Attempt も、従来どおり再起動でロストします。
- 自身で Executor を持つデーモンも、同じ仕組みで引き継ぎを行います。

### アドミッション制御とプランニング

待機中のどの Execution をどこで実行するかは、環境全体を対象とした単一のプランニングパスによって決定されます。このパスでは、スコープゲートが `open` なすべての待機中 Execution、直近 1 分以内にポーリングした（またはまだポーリングしていない）Executor を持つすべてのノード、およびすべての Lease と占有（claim）を考慮します。

各 Executor からの予約（reserve）およびプリエンプションの呼び出しはすべて同じプランニングパスを実行します。選択を行うのは最初にポーリングした Executor ではなくこのプランニングパスであり、別ノードに配置決定された Execution はそのノードの Executor を待ちます（待機理由 `awaiting_executor`）。待機中の Execution がなく、かつクリアすべき待機理由も残っていない場合、ポーリングはプランニングパスを実行しません（ロックなしの読み取りで判定します）。ただし、ディスクウォーターマークのアラートを最新に保つため、10 秒に 1 回だけはパスを実行します。

同一ノード上の複数 Executor はそのノードのリソースを共有するため、ラベル条件に一致する Executor であればどれでも、そのノードに配置された Execution を引き受けることができます。

#### 優先順位と時刻制約

- **優先順位（Order）**: `priority` が高いものを優先します。同じ `priority` 内では以下の順に優先されます。
  1. `start_by` または `finish_by` を持つ Execution が先となり、最も早く開始する必要があるものを優先する（`start_by` と、「`finish_by` から推定実行時間を引いた時刻」のうち早い方）。
  2. 環境全体の GPU・CPU・RAM それぞれに対するプロジェクトの占有率のうち、最大となっている割合が最も小さいプロジェクト。
  3. 投入時刻（submission）が早いもの。
- **開始・完了時刻制約（`not_before` / `start_by` / `finish_by`）**: `[tasks.execution]`（または Execution マニフェスト）で指定する `not_before`、`start_by`、`finish_by` は、タイムゾーンオフセット付きの時刻です。TOML の date-time（例: `not_before = 2026-11-08T06:50:00+09:00`）または同じ形式の文字列で指定します。デーモンと CLI が異なるタイムゾーンにある可能性があるため、オフセットのない時刻は拒否されます。
  - `not_before` に達する前はアドミッションされず、リソースも予約しません（待機理由 `not_before`）。
  - `not_before` の 2 時間前からは、バックフィルと同様に、その時点までに収まるノード（それまでに終了予定の Lease が解放された後に収まるノード）上で、`not_before`（またはそのノードで実行中のジョブの終了予定がそれより遅い場合はその時刻）以降の必要リソースを確保（hold）します。その時刻までは、それまでに終了する見込みのジョブだけがそのリソースを使用できます。したがって、より高い優先度のジョブが先に来たり、実行時間推定のないジョブがすでにそこで動いていたりしない限り、予定どおりに開始されます。この事前確保（hold）のためだけに他のジョブがサスペンドされることはありません。
  - `start_by` と `finish_by` は、それぞれ開始すべき時刻と完了すべき時刻を表します。優先順位付け以外では表示のみに使われ、待機がこれらを過ぎている場合や（推定実行時間から）間に合わない見込みの場合はその旨が表示されます。12 時間以上過去になったもの（数日後の再実行など）は無視されます。いずれのためにも他ジョブがサスペンド・停止されることはなく、プリエンプションの判断はあくまで `priority` に委ねられます。
  - 継続（continuation）は Task の `not_before` と `finish_by` を引き継ぎますが、`start_by` は引き継ぎません（すでに開始済みであるため）。`task show` ではこれら 3 つが `times:` の下に表示されます。

#### 入力データ（`inputs`）

`[[tasks.execution.inputs]]`（Execution マニフェストでは `[[inputs]]`）では、Execution が読み込む入力を指定します。各入力はそのパスが環境変数 `KAIRO_INPUT_<NAME>` として Attempt に渡されます。`<NAME>` は大文字に変換され、先頭が英字、以降が英字・数字・`_` から成る最大 64 文字の識別子である必要があり、重複不可・合計最大 32 件まで指定できます。各入力には、以下のソースをちょうど 1 つ指定します。

- `task = "T"`: 同一プロジェクト内の Task `T` の最新 Run の `output` ディレクトリ（Task でのみ指定可能。`T` は `depends_on` に含まれている必要があり、その Run が先に成功していることが前提）。
- `data = "NAME"`: その名前を持つプロジェクトの最新データオブジェクト（インポートしたものなど。[データストア](#データストア)参照）。
- `object = "obj_..."`: ID で指定するプロジェクトのデータオブジェクト。

入力の読み込み・配置・保護ルールは以下のとおりです。

- **読み込み場所とディレクトリ保証**: 入力は、Attempt が実行されるノード上の既存の場所から読み込まれます。具体的には、volatile store 上にまだ残っている Run の `output` ディレクトリ、インポート元にまだ残っているディレクトリ（未削除、または `--delete-source` なしで保持されているもの）、あるいはそのノードへ持ち込まれたデータオブジェクト（[データストア](#データストア)の持ち込み入力（brought inputs）参照）です。入力は常にディレクトリとして提供されます。単一ファイルをインポートした場合は、元の場所から直接読まれることはなく、元のノード上であっても他ノードと同様に「そのファイルを格納したディレクトリ」として持ち込まれます（これにより、どのノードでも `${KAIRO_INPUT_X}/<name>` で同じように読み込めます）。
- **配置先ノードの決定**: 入力を持つ Execution（Task または直接 Execution）は、Executor セレクタで `node` 自体を明示指定している場合を除き、次の優先順でノードにプランニングされます。
  1. すべての入力が揃っている最初のノード
  2. 最初の入力が存在するノード
  3. そのオブジェクトが作成されたノード
- **待機と持ち込み**: すべての入力がそのノードに揃うまで、Execution は `inputs_not_ready`（どの入力がなぜ未準備かも表示）で待機します。その間、不足している入力は pack 完了後にそのノードへ持ち込まれます（一時データ（temporal data）は存在するノードから持ち込まれるか、どのノードにも存在しない場合はそのノードで再生成されます。[データストア](#データストア)の一時データ参照）。そもそも取得できない入力（実行履歴のない Task、プロジェクトに該当オブジェクトが存在しない名前、再生成した一時データが元の内容と異なっていた場合など）についても、その旨が待機理由に示されます。
- **起動時の再検証**: Attempt の起動時にも各入力が再検証されます。そのノード上にあり、Executor の到達範囲内（担当するストア、またはそのノードのインポート元ディレクトリ）にディレクトリとして存在しなければ起動は中止され、Execution は `launch_aborted: input NAME: why` で終了します。
- **参照中のソース削除抑止**: pending または running の Task、あるいは未終了の Execution が入力として参照している間は、コピー完了後もソースは削除されません（pack とコピー自体は通常どおり行われます）。対象となるのは、Run の `output` ディレクトリ（その Task の最新 Run である間の Task 指定、またはオブジェクト名・ID 指定による参照）と、`--delete-source` 付きでインポートされたディレクトリ（名前または ID 指定による参照）です。
- **ダイジェストへの影響**: 入力設定は、1 件以上指定されている場合にのみ Execution のダイジェストに含まれます。

#### 実行環境・ライフサイクル設定

- **ソース（`source`）**: Task の `[tasks.execution]` に `source = "NAME"` を指定すると、Task 宣言時点で作成されたプロジェクトの `[sources.NAME]` のコードスナップショット内で Attempt を実行します（後述の[コードスナップショット](#コードスナップショット)参照）。`cwd` はスナップショットに対する相対パスとなります（デフォルトは `.`。絶対パスやスナップショット外に出る `cwd` は拒否されます）。設定されている場合のみ Task のダイジェストに含まれますが、スナップショット自体はダイジェストに含まれません。
- **コンテナイメージ（`image`）**: Task の `[tasks.execution]` に `image = "NAME"` を指定すると、Task 宣言時点でプロジェクトの `[images.NAME]` の参照が指していたダイジェストのコンテナ内で Attempt を実行します（後述の[コンテナ実行](#コンテナ実行)参照）。実行はコンテナを実行する Executor 上で行われ、その種の Executor はコンテナ以外のジョブを実行しません。設定されている場合のみ Task のダイジェストに含まれますが、イメージのダイジェスト自体は含まれません。
- **再生成可能出力（`regenerable`）**: Task の `[tasks.execution]` に `regenerable = true` を指定すると、その Run が `output` ディレクトリに書き出す内容は Task の再実行によって再生成できることを宣言します（別所に保管されたソースから構築するデータセットなど）。その出力は一時データ（temporal data。[データストア](#データストア)参照）として扱われて durable store へはコピーされず、参照先のノードで必要になった際に Kairo が Task を再実行して再生成することがあります。設定されている場合のみ Execution のダイジェストに含まれます。
- **ロスト後の自動継続（`continue_after_loss`）**: チェックポイント可能な Task の `[tasks.execution]` に `continue_after_loss = true` を指定すると、Attempt がロストした場合（ノードの電源断やエージェント消失など、理由 `executor_identity_lost`）に Task をブロックせず続行させます。Task は、ロストした Attempt のプロセスが何も動作し得ない状態になるまで（つまり、コマンド未起動またはノード再起動済みでデーモンが自動解放するか、`kairo resource reconcile` によって `stale` な Lease が解放されるまで）`running` を維持します。その後、公開済みの最新チェックポイント（サスペンド時、オペレータによる実行、または `data_protection` チェックポイントコマンドによるもの）から、同じ Run の継続（continuation、理由 `loss_continuation`）として、その Run の output ディレクトリがあるノード（チェックポイントはその中にあります。ほかのノードへ移していなければ、チェックポイントを公開したノード）上で再開します。チェックポイントが 1 つも公開されていない場合は、この設定がない場合と同様にブロックされます。設定されている場合のみ Task のダイジェストに含まれます。
- **ストア指定（`store`）**: `[tasks.execution]`（または Execution マニフェスト）に `store = "ID"` を指定すると、Attempt の `scratch`、Run の `output`、`cache` ディレクトリの配置先となる volatile store（[マシン宣言](#マシン宣言)参照）を明示できます。指定しない場合は Executor が担当する最初のストアが使われます（[データストア](#データストア)参照）が、大容量の出力に余裕のあるストアを選びたい場合などに使用します。この Execution は（Executor セレクタで `node` を直接指定していない限り）指定ストアを持つノードにプランニングされます。そのストアを利用可能な volatile store として担当していない Executor 上での起動は拒否されます（`launch_aborted: store ID is not a usable volatile store executor E serves`）。設定されている場合のみ Execution のダイジェストに含まれます。

#### コンファインメント（`confine`）

`[tasks.execution]`（または Execution マニフェスト）に `confine = true` を指定すると、Attempt の書き込み先が Kairo から渡された次の 3 ディレクトリのみに制限されます（[データストア](#データストア)参照）。

- `scratch` ディレクトリ（`KAIRO_SCRATCH`）
- Run の `output` ディレクトリ（`KAIRO_OUTPUT_DIR`）
- プロジェクトの `cache` ディレクトリ（`KAIRO_CACHE_DIR`）

それ以外の場所は従来どおり読み取れますが変更できなくなり、`TMPDIR`、`TMP`、`TEMP` には `scratch` ディレクトリが設定されます。また、デーモン設定の `[data] confine_from`（オフセット付き時刻、例: `"2026-11-14T00:00:00+09:00"`）を指定すると、その時刻以降に起動されるすべての Attempt が、Execution 側での指定の有無にかかわらずコンファインメント下で実行されます。

利用可能な volatile store を持たない（したがって `scratch` や `output` ディレクトリを持たない）Executor 上でのコンファインメント起動は拒否されます（`launch_aborted: cannot confine the attempt here: ...`）。コンファインメント設定は、Execution 側で明示的に要求された場合にのみ Execution のダイジェストに含まれます。

実装はランタイムごとに次のとおりです。

- **Incus（Linux）**: コンテナの init（`__kairo-container-init`）が、コンテナのマウント名前空間の中で閉じ込めます。上記の 3 ディレクトリ、`/dev/shm`、および `/proc` をそれぞれ自身にバインドマウントし、さらに `scratch` 配下の `tmp` ディレクトリを `/tmp` の上にバインドマウントします。そのうえで `/` から再帰的にすべてのマウントを読み取り専用にし（`mount_setattr`）、上記のマウント先だけを再び書き込み可能に戻してから `argv` を起動します。デバイスはそのまま利用可能です。閉じ込められない場合は、理由を stderr に出力して終了コード `126` で終了します。
- **wslc（Windows）**: コンテナの中でマウントを作れない（`CAP_SYS_ADMIN` がない）ため、マウントによる閉じ込めの代わりに、`argv` を一般ユーザー（`1000:1000`）で実行します（[WSL コンテナ Executor](#wsl-コンテナ-executorkind--wslc) を参照）。

#### 配置・バックフィル・プリエンプション

- **配置（Placement）**: Execution が収まるノードのうち、配置後に残る空き GPU 数が最も少ないノードを選びます（これにより CPU ジョブは GPU ノードを避け、GPU ジョブは可能な限り少数のノードへ集約されます）。同数の場合は空き CPU が最も多いノードを選びます。
- **バックフィル（Backfill）**: 現時点でどのノードにも収まらない Execution は、障害となっている Lease 群が終了した際に実行される見込みのノード上で、すでに利用可能なリソースを「開始予定時刻」（必要な数の Lease が終了すると見込まれる時刻。[実行時間の推定](#実行時間の推定)参照）以降について確保（hold）します。
  - 後続の Execution は、誰も確保していないリソースではそのまま実行でき、誰かが確保しているリソースであっても、その開始予定時刻より前に終了する見込みであれば実行できます。
  - 自身に実行時間の推定値がない Execution や、開始予定時刻を推定できない先行 Execution（妨げとなっている Lease に推定値がない場合）によって確保されているリソースについては、バックフィル実行されません。
  - 待機理由には開始予定時刻が表示されます。
  - 自然終了しない要因で阻まれている場合は確保（hold）を行いません。該当するのは、Kairo 以外が起動した GPU 上のプロセス、プロセスで説明できないアクティビティ、古い観測結果（`stale_observation`）、ノード容量の絶対的不足、ディスクウォーターマーク超過です。
- **プリエンプション（Preemption）**: どのノードにも収まらない Execution（優先度は問わず）は、自身より低い優先度を持ち、`checkpointable` かつ `preemptible` であり、かつ自身の配置を妨げている GPU または RAM を保持している Execution を、必要最小限の数だけサスペンドできます。対象の選定順は次のとおりです。
  1. すでにサスペンド処理中のもの
  2. 優先度が最も低いもの
  4. 開始時刻が最も新しいもの

  CPU やディスクの不足は待機対象であり、他ジョブをサスペンドする理由にはなりません。また、候補すべてをサスペンドしてもなお空きが足りない場合（GPU は空くが他ジョブが保持する RAM が足りない場合など）は 1 つもサスペンドせず待機し、候補が保持しているリソースではなくサスペンドしてもなお不足するリソースを待機理由として示します（例: `insufficient_ram: host-a: 28.0 GiB ram needed, 24.0 GiB free even were infer/serve-r3 (priority -100) suspended, so none is suspended for it; and host-a: 1 GPU(s) needed, 0 usable; leased by infer/serve-r3 (priority -100)`）。

#### Run の output ディレクトリと配置

Run の output ディレクトリ（[`output` ディレクトリ](#output-ディレクトリ)）は 1 つのノードのストアにあり、チェックポイントもその中にあります（コンテナの `/kairo/output`）。そのため、Run の Execution は output ディレクトリのあるノードで動くか、先にディレクトリを持ち込んでから動きます（[Run の output の持ち込み](#run-の-output-の持ち込み)）。

- **サスペンド後の継続**: 宣言（Executor ラベル）が許すどのノードでも動けます。プランニングパスは、動けるノードごとに終わる時刻を見積もり、output ディレクトリのあるノードより十分早く終わるノードがあれば、ディレクトリをそこへ移してから動かします。
  - **始められる時刻**: 今収まるなら今、そうでなければ妨げている Lease が推定どおり終わる時刻（[実行時間の推定](#実行時間の推定)）。推定できないノードは「いつ始まるか分からない」として扱います。
  - **運ぶ時間**: ディレクトリの大きさ ÷ 2 つのノードの agent 間の帯域。帯域は network ベンチ（`net-bandwidth-send`・`-recv` の小さい方）の実測で、なければ 100 MB/s（1 GbE）とみなします。pack と展開の分として 25% と 30 秒を足します。
  - **残りの実行時間**: 推定実行時間からそれまでに走った時間を引いたものを、ノードの速さの比で割ります。速さは、GPU を使う仕事は GPU の gemm ベンチ（基準の動作点）、それ以外は CPU の `cpu-compute` ベンチの値です。どちらかのノードが測られていなければ同じ速さとみなし、推定実行時間がなければ始まる時刻と運ぶ時間だけで比べます。
  - **動かす条件**: 終わる見込みが、ディレクトリのあるノードより 10 分以上、かつそこでかかる時間の 1 割以上早いこと（そこでの終わりが見積もれないときは、見積もれるノードならどこでも）。動かすと決めたら、その Execution は `output_moving` で待ち、ディレクトリが届く見込みの時刻から移る先のノードで必要な資源を確保します（その前に終わる見込みの仕事はそれを使えます）。決めた内容（各ノードの見込み）は持ち込みの記録（`output_landings.estimate_json`）に残り、イベント `output_move_ordered` にも載ります。
  - **制約**: 移すのは 1 つの継続につき 1 回まで、1 つの Run につき同時に 1 つまでです。ディレクトリのあるノードの agent（その Executor）が生きているときだけ移します（pack するのはその agent です）。
- **そのほかの Execution**: ノードが落ちた後の継続（`loss_continuation`）、すでに持ち込んだ継続など、output ディレクトリを持つ Run の Execution は、そのノードで動きます。そこに Executor ラベルに合う生きた Executor がなければ `output_elsewhere` で待ちます。
- **チェックポイントからの再実行**: 持ち込みが終わるまで `output_moving` で待ち、持ち込みを断念したら `output_unavailable` で待ちます。

#### リソース計算・弾性 CPU・ディスクウォーターマーク

- **リソース計算（Accounting）**:
  - **CPU**: ノードの総 CPU から、そのノード上の予約（reservation）の合計を差し引いた値です。
  - **RAM**: 「総メモリ − 予約合計」と、「観測された空きメモリ − 各予約のうち Attempt がまだ使っていない分」のうち小さい方を用います。Executor がサンプリングした Attempt の実使用量は 2 分間有効として計上され、サンプルがない場合は予約全額が計上されます。
    - 同種の過去実行のうち使用量がサンプリングされた成功 Run が 3 件以上ある場合、予約の計上額には実績ベースの上限が適用されます。上限は、最も具体的な実行時間推定キー（プログラム・サブコマンド・設定ファイル、プログラム・サブコマンド、または数字をマスクした Task 名のいずれか。キュー全体やプロジェクト全体は対象外）を共有する直近 5 件の Run における Attempt 最大実使用量の 1.5 倍 + 512 MiB とし、かつその Attempt の現在の実使用量を下回ることはありません。
    - ただし、GPU を保持している Lease は常に予約全額が計上されます。CUDA がピン留め（pin）するホストメモリはサンプリング値に含まれないためです（コンテナのランタイムが報告するメモリ使用量 を計測しており、どちらも pinned memory を含まず、さらに Windows ではモデル重みなどのメモリマップドファイルも含まれません）。スワップのないホストでメモリが枯渇するとハングするため、この保護を行っています。
    - これにより、過大に宣言されたメモリ（実際には 300 MiB しか使わないジョブに 8 GiB を宣言した場合など）で使われない分を抱え込み続けるのを防ぎつつ、実際に大きく使うジョブ（10.7 GiB 使った実績に対して 16 GiB を宣言した場合など）は引き続き全額計上されます。サンプリング間隔は 15 秒であり、1.5 倍 + 512 MiB の余裕（headroom）はこの間隔での増加に備えるためのものです。
  - **Disk**: 観測された空き容量から、その上の全予約を差し引いた値です。書き込みバイト数はディスク上に実際に残る容量を過大評価するため（書き換えられたチェックポイントが二重計上されるなど）、書き込み量によって予約額が減らされることはありません。ディスク要求は、自身の `min_free_after_bytes` とディスクウォーターマークの双方の空き容量を確保します。
- **弾性 CPU（Elastic CPU）**: `[capacity]`（Task では `[tasks.execution.capacity]`）配下の `cpu_millis_min`（上限は `cpu_millis`）は、Execution が最低限維持する CPU 量です。予約 CPU の残余分は同等以上の優先度を持つ Execution に貸し出され、元のジョブを停止させずに横に並走させて処理速度を落とすだけで共存できます（例: `cpu_millis = 16000`、`cpu_millis_min = 4000` で宣言されたサーバは、同等以上の優先度の評価ジョブに対して 12 CPU 分を貸し出して並走を許容します）。より低い優先度の Execution には一切貸し出しません。
- **ディスクウォーターマーク（Disk watermark）**: デーモン設定の `[scheduler] disk_min_free_bytes`（デフォルトは 5 GiB、かつファイルシステム容量の最大 5%。`0` で無効化）を下回る登録済みファイルシステムを持つノードは、空き容量が確保されるまで新規アドミッションを一切受け付けません。該当するファイルシステムはそれぞれ `node_alerts` に記録され、イベント `disk_low_watermark` および `disk_watermark_cleared` で通知されます（同じ設定ファイルの `[data]` では durable コピーのリードバック方式を設定します。[データストア](#データストア)参照）。

#### 待機理由とコード一覧

待機中のすべての Execution について最新の待機理由が保持されます。`task show` では `waiting:` の下に詳細が表示され（例: `gpu_leased: host-a: 1 GPU(s) of >= 11.0 GiB needed, 0 usable; leased by infer/serve-r3 (priority -100)`）、`task list` ではそのコードが NOTE に付加されます（例: `execution waiting: insufficient_ram`）。

待機理由コードは以下のとおりです。

`no_executor` / `not_before` / `inputs_not_ready` / `store_moving` / `output_moving` / `output_elsewhere` / `output_unavailable` / `owner_use` / `awaiting_executor` / `gpu_leased` / `held_for_higher_priority` / `gpu_memory_low` / `insufficient_gpu` / `insufficient_cpu` / `insufficient_ram` / `insufficient_disk` / `external_claim` / `unattributed_activity` / `stale_observation` / `disk_low_watermark` / `preempting`

### 実行時間の推定

バックフィルを行うには、実行中のジョブがおおよそいつ終わるか、また待機中のジョブがどのくらいかかるかを把握する必要があります。このための明示的な宣言は不要であり、1 つの Run（1 つの Task に属する Execution 群をまとめたもの、または直接 Execution）の所要時間は、過去に成功した同種の Run の実績から推定されます。具体的には、最も具体的なキーを共有する直近 5 件の中央値（median）を使用します。

キーは次の優先順（具体性の高い順）で一致を探します。

1. プロジェクト、キュー、プログラム、サブコマンドまたはスクリプト、および数字をマスクした設定ファイル（例: `neo_ime.cli train --config shunran-v0.9-30m.toml` は `... shunran-v0.10-4m.toml` と同種とみなされる）
2. プロジェクト、キュー、プログラム、およびサブコマンドまたはスクリプト
3. プロジェクト、キュー、および数字とハッシュをマスクした Task 名
4. プロジェクト、およびキュー
5. プロジェクト

推定の算出ルールと更新タイミングは次のとおりです。

- **プログラムの抽出**: コマンドライン上でランナー（`uv run`、`python -m`、`pwsh -Command` など）ではない最初の語から、ディレクトリ・拡張子・ハッシュを取り除いたものを用います。
- **成功 Run と実績時間**: 最後の Execution がシグナルなしで終了コード `0` で終了した Run を成功とみなし、その配下の各 Attempt が実行された時間の合計を所要時間とします。
- **残り実行時間の見積もり**: 「推定実行時間 − これまでに各 Attempt が実行した合計時間」を用います。すでに推定実行時間を超過している Run は、「これまで実行した時間と同じだけさらにかかる（ただし最低 1 分）」と見積もられます。
- **特殊な Lease の扱い**: 有効な `suspend` Command が進行中の Lease は 2 分以内に終了すると見込まれます。一方、Attempt が終了済みなのに（残留プロセスにより）なお保持されている Lease や、推定実行時間を持たない Run の Lease は終了見込みなしと扱われるため、その後ろで待機する Execution は推定値がない場合と同様に確保（hold）を行います。
- **履歴の再読み込み**: いずれかの Attempt が終了するたびに再読み込みされ、それ以外でも少なくとも 10 分ごとに再読み込みされます。

実運用環境の履歴（4,200 件の Run）でリプレイ検証したところ、推定値の半数が実際の実行時間の 2 倍以内に収まりました。また、長時間の Run は過小評価される傾向があります（同種の過去 Run が短時間の試行であることが多いためです）。それでもバックフィルはこの誤差を許容できます。バックフィルが判断するのは「短いジョブが、それを遅延させ得る Execution の開始可能時刻より前に終わるかどうか」だけであり、実行時間超過に伴って推定値が補正されても、待機側 Execution へのコストはわずかだからです。同じ履歴でのシミュレーションにおいて、この推定値に基づくバックフィルは実際の実行時間を使ったバックフィルと同等の完了時間を達成し、推定値なしでリソース確保のみを行う場合と比べて大幅に短い完了時間となりました。

### Executor のティック処理

Executor の 1 回のティック（tick）は実行時間に上限が設けられています。

#### 外部呼び出しのタイムアウトと停滞検知

ティック内で行われるすべての外部呼び出しにはデッドラインが設定されています。

- **`nvidia-smi` の各実行**: 20 秒（`pmon` が返らない場合、その GPU 観測は失敗となります）
- **`taskkill` の各実行**: 30 秒
- **各 Provider 呼び出し（`observe`、`prepare`、`release`）**: 45 秒

キャンセルできない呼び出しはデッドライン到達時点で打ち切られ（abandoned）、その呼び出しが返るまで同じ Provider に対する後続の呼び出しは開始されません。

ティック内のステップが 30 秒間続くと、WARN レベルで `executor step stalled`（`executor_id`、`step`、`age_seconds` 付き）がログ出力され、そのステップが終了すると INFO レベルで `executor step resumed` が記録されます。また、実行に 2 秒を超えた外部コマンドは所要時間とともに INFO レベルでログ出力され、タイムアウトした場合は対象コマンドとともに WARN レベルで記録されます。

#### Attempt のリソース使用量サンプリング

Executor は 15 秒ごとに、自身が起動した各 Attempt のリソース使用量をサンプリングします。これは、アドミッション制御が Attempt の RAM 予約量のうち実使用分を重複計上せず 1 回だけ計上できるようにするためです（エージェント API の `attempt-usage`。書き込みバイト数も記録されますが、ディスクのアドミッション計算には影響しません。[アドミッション制御とプランニング](#アドミッション制御とプランニング) を参照）。

サンプリングはティックと並行して 1 回ずつ実行され、1 回の上限は 15 秒に制限されているため、ティック処理をブロックすることはありません。RAM は Attempt のプロセス群が保持しているメモリ量、書き込みバイト数はプロセス群が書き込んだ量を表し、以前のサンプル値を下回ることはありません。

数値はコンテナのランタイムの統計から取ります（Incus は `incus query` のメモリ使用量で、書き込みバイト数は不明として扱います。wslc は `wslc stats`）。コンテナのランナーではなく、コンテナそのものが使った量です。

統計が読めない場合、その数値は不明扱いとなり、アドミッション制御は予約量全体を計上します。エージェント再起動後に引き継がれた（adopted）Attempt はエージェント自身が起動したものと同様にサンプリングされますが、見失われた（lost）Attempt はサンプリングされません。

### Attempt ログ

Attempt の標準出力（stdout）と標準エラー出力（stderr）は、Executor の `log_directory` 内のファイル（`<attempt>.stdout.log`、`<attempt>.stderr.log`）として、Attempt 自身のプロセスによって書き込まれます。ノードエージェントがこれらのファイルをデーモンの `log_directory` に転送し、`task show`、`task logs`、`GET /api/attempts/{id}/log` がデーモン側のコピーを読み取ります。

- **起動前の空き容量チェック**: Executor は、`log_directory` を保持するファイルシステムの空き容量が `log_min_free_mib`（デフォルト `1024`、デーモン設定およびエージェント設定の両方で設定可能）を下回っている間は Attempt を起動しません。この場合、Execution は `launch_aborted: launch failed before activation: the filesystem of the attempt log directory ... has 12.0 MiB free, below the minimum of 1.0 GiB` として終了し、`task show` および `task list` に表示されます。空き容量の計測に失敗した場合や、ログファイルを作成できない場合も同様に起動中止となります。
- **実行中・終了時の空き容量監視**: Attempt の実行中、Executor は 15 秒ごとにこの空き容量を確認し、最小値を下回った場合（`log_space_low`）や枯渇した場合（`log_space_exhausted`: 出力が失われている状態）を各 Attempt に記録します。さらに Attempt の終了時にもう 1 度確認します。Attempt 自身のプロセスが行う書き込みの失敗は外部から直接検知できないため、ファイルシステムの空き容量を監視対象としています。
- **ログ転送とリトライ**: エージェントは空のファイルも含めてすべてのログを転送します。デーモンが受け取れなかったログ（デーモンへ到達できない場合や、デーモン側のディスク満杯により `internal` エラーが返された場合）は、スイープのたびにデーモン側のコピーの末尾から再送されるため、ノード側のファイルに記録された内容は失われません。30 秒間デーモンに到達していないログはストリームごとに Attempt に記録され（`log_forward_failed`）、その後追いついた際も記録されます。Attempt が終了する際は、終了が記録される前にログが転送されます。

発生した各問題はコーディネーションイベント（`attempt_log_problem`、`attempt_log_problem_resolved`）として記録され、`task show` では Attempt の `log issue:` 行として（API では `log_problems` として）表示されます（例: `log issue: stderr not shipped to the daemon: ... (10-27 10:01 to 10-27 10:04, caught up)`）。

### 電力と消費電力量

Provider はデバイスの消費電力と動作状態を読み取り時刻とともに記録します。Kairo はこれらの事実をそのまま保持するだけで、仕様値に対する良否判定は行いません。Provider は読み取り専用であり、設定を変更できるのはホストのコントローラのみです（[ハードウェア設定](#ハードウェア設定) を参照）。

#### デバイス別の計測項目

- **GPU**: 各 `nvidia-smi` ポーリング時に、ボード消費電力（`power.draw.average`、取得できない場合は `power.draw`）、適用中の電力制限（`enforced.power.limit`）、SM クロックとメモリクロック、P-state、アクティブなクロックイベント理由（ビットマスク: `0x1` idle、`0x4` software power cap、`0x40` hardware thermal slowdown 等）、PCIe 世代とレーン幅、メモリ使用率、ファン速度を読み取ります。さらに GPU ごとに 1 回、デフォルト・最小・最大の電力制限、最大クロック、最大 PCIe リンクを保持します。GPU が返さないフィールド（`[N/A]`、`[Not Supported]`）は省略され、エラーにはなりません。これらのフィールド名をドライバが拒否した場合は旧名称（`clocks_throttle_reasons.active`、`pcie.link.gen.current`）で問い合わせ、それでも受け付けられない場合はインベントリ情報のみを取得します。
- **CPU パッケージ**: ノードのホスト Provider が、前回のポーリングからの平均 CPU パッケージ消費電力を読み取ります。Windows では `\Energy Meter(*)\Power` パフォーマンスカウンタ（インスタンス `rapl_package*_pkg`、管理者権限不要）、Linux では RAPL カウンタ `/sys/class/powercap/*-rapl:N/energy_uj`（カウンタの折り返しは `max_energy_range_uj` で処理）を使用します。読み取れない環境（非特権コンテナや非対応 CPU など）では、エラーを出さずサイレントに報告なしとします。読み取り処理は `internal/cpupower` に実装されています。

#### 電力量の積算と保存

サンプルは物理デバイス単位（GPU は UUID 単位）で保持されるため、共有 GPU の複数 Provider からの報告は 1 つの時系列にまとめられます。GPU サンプルが到着するたびに、そのデバイスの前回サンプル以降の消費電力量（2 サンプル間の消費電力の台形積分）が、その GPU を保持している Lease（どのノードの行であっても）に属する Attempt へ加算されます。積分区間は Attempt の開始時刻から、Attempt の終了・Lease 解放・Lease の `stale` 化のいずれかまでで切り出され、どの Provider が報告した場合でも 1 台の GPU は 1 回だけ計上されます。

サンプルによって裏付けられない区間は積分されず、その Attempt の合計値は `partial` となります。該当するのは、前のサンプルの有効期間（`observation_ttl_seconds`、デフォルト 15 秒）を超える空白がある場合、電力値を含まないサンプルがある場合、その GPU の最初のサンプル前に実行が開始されていた場合、または Lease が `stale` になった後も実行が継続した場合です。

- **表示と API**: `task show` は Attempt ごとに `energy: GPU 1.23 kWh (partial)` のように出力します。API では `energy` として `gpu_joules`、`gpu_seconds_covered`（デバイス秒、device-seconds）、`partial` を返します。
- **動作点ポリシーによる節約量**: Attempt の GPU に動作点ポリシーの設定が適用されている間、`task show` は推定節約量も併記します（例: `energy: GPU 1.23 kWh; policy 3.0 h, saved ~120.0 Wh (estimate)`。API フィールドは `policy_seconds`、`policy_saved_joules`、`policy_unestimated_seconds`。[動作点ポリシー](#動作点ポリシー) を参照）。
- **CPU パッケージの扱い**: CPU パッケージはノード上で動くすべての処理で共有されるため、電力データはノード単位でのみ保持されます。ホストのコントローラも毎ポーリングで同じ `internal/cpupower` を使って CPU パッケージを読み取り、ホストの時系列 `host/<host>/cpu` として報告します（Proxmox ホストではホスト自身のみが RAPL を読み取れ、コンテナからは読み取れないためです）。
- **データベースと保持期間**: サンプルは `device_samples` テーブルおよび `device_properties` テーブル、Attempt ごとの合計は `attempt_energy` テーブルに保存されます。サンプルは 72 時間保持され（デフォルトの 5 秒ポーリング時、GPU 1 台・報告 Provider 1 つあたり 1 日約 5 MB）、合計値は各 Attempt とともに保持されます。

### マシン宣言

どのノードがどの GPU を保持するかは自動検出ではなく宣言によって管理します。

#### ノードと GPU の宣言・移動

`kairo machine apply FILE` は宣言全体を置き換えます。`[[nodes]]` テーブルに `id`（デーモンに既知のノード）と `gpus`（`nvidia-smi -L` が出力する形式の NVIDIA UUID 配列）を指定します（`examples/machines.toml` を参照）。

Provider は観測のみを行います。Provider が報告した GPU がそのノードに宣言されていない場合（他ノードに宣言されている場合も、どのノードにも宣言されていない場合も）、登録もスケジューリングもされません。そのポーリング内の他の観測結果は通常通り適用され、未宣言 GPU は `kairo machine show` および `kairo resource status` に未宣言として表示されます（`--json` 指定時は `undeclared_gpus`。登録済み GPU がこの状態になった場合の `derived_state` は `undeclared` です）。

GPU を別ノードへ移動するには、新しいノードにその GPU を宣言します。

- 新しいノードの Provider がその GPU を報告するまで、GPU は旧ノードに登録されたままスケジューリング不可の状態を保ち、新ノードから報告された時点で所有権が移譲されます（イベント `gpu_ownership_transferred`）。
- 宣言が変更された時点で、旧ノード側のその GPU に対するオープンな claim はクリアされます（イベント `gpu_declaration_changed`）。
- アクティブな（live）Lease が保持している GPU の移動・削除や、未知のノードの宣言は `apply` によって拒否されます。
- `--dry-run` を付けると適用を行わず変更内容（`+` 宣言追加、`~` 移動、`-` 削除）のみを表示します。
- `machine show` は宣言済みの各 GPU を `observed`、`stale`、`pending`（対象ノードからまだ未報告）、または `unseen` として一覧表示します。

#### 共有 GPU（SHARED）

複数のノードに列挙された GPU は共有（SHARED）扱いとなり、同一ホスト上の複数ノード（同じカードが見えており、それぞれエージェントが動いているコンテナ群）がそれぞれその GPU をスケジュールできます。

- 各ノードは自身専用の行（row）を登録します（最初のノードは Provider の ID、それ以外のノードは `<id>@<node>`）。
- そのデバイスのいずれかの行に対する Lease は他の行を排他します。スケジューリング、アドミッションチェック、Lease 解放、`resource status` は、そのデバイスのすべての行の Lease および claim を合算して扱います。
- Provider は他コンテナ内のプロセスを見ることができないため、あるノードの行が Lease されている間、他ノードが報告するプロセス不明のアクティビティ（`unattributed_activity`）の claim は Lease 保有者の処理とみなされ記録されません。一方、Lease 保有者自身の Provider は帰属不能なアクティビティを引き続き報告し、他ノード側で実際に観測できたプロセスについては Lease 保有者自身の処理でない限り `external_process` claim として維持されます（[コーディネーションモデル](#コーディネーションモデル) を参照）。
- 優先度プリエンプションは、他ノードの行を通じてそのデバイスを保持している低優先度の victim にも適用されます。
- あるノードの行にアクティブな Lease がある場合、`apply` はその GPU からそのノードを外す変更を拒否します。共有 GPU の変更における `from` / `to` には対象ノードがカンマ区切りで列挙されます。
- コンテナはホストの RAM と CPU も共有しますが、Kairo はこれらをノード単位で管理するため、各コンテナのメモリ上限はホストの実メモリ内に収めてください。

#### 物理ホスト宣言（`[[hosts]]`）

任意の `[[hosts]]` テーブルでは物理マシンを宣言します。ここに記述するのは観測からは分からない「ポリシーと権限」のみであり、デバイスの種別・クロック・消費電力といったハードウェア特性は Provider による観測とベンチマークによる実測で取得するため宣言しません。

- `id`: ホスト ID。
- `nodes`: そのホスト上のノード一覧（コンテナ群、または物理マシン自身。1 つのノードは最大 1 つのホストに属します）。
- `controller`: デバイス設定の変更権限を持つ `kairo controller` のノード（省略時は設定変更せず観測のみ）。
- `max_slowdown`: 電力効率のためにデバイスが最大性能から許容できる性能低下割合。範囲は `[0, 0.5)`（省略時は `0` で、ハードウェアはデフォルト設定を維持します）。
- `[[hosts.devices]]`: 個別デバイス（GPU UUID または `cpu`。1 デバイスにつき 1 回のみ指定可能）に対して `max_slowdown` を上書きするか、`control = false`（設定変更もベンチマークも行わない）を設定します。
- `containers`: ホスト上のノードのうち Proxmox コンテナであるものを vmid にマッピングします（例: `containers = { host-a-gpu = 101 }`）。Kairo がコンテナ上でデータを削除した後にホストのコントローラがトリムを実行するため（[データストア](#データストア) を参照）、`controller` が必須であり、1 つの vmid は 1 つのノードにのみ割り当てられます。

ホストの対象デバイスは、そのホストに属するいずれかのノードに宣言されたすべての GPU と `cpu`（CPU パッケージ）です。ある GPU に紐づくノードが 2 つのホストにまたがる構成はエラーとなります。ホストの `nodes` と `controller` は既知のノードでなければならず（コントローラは起動時に自身のノードを登録します）、1 つのコントローラが制御できるのは 1 ホストのみです。

ホスト定義は `apply` 時に全体が置き換えられます（イベント `host_declaration_changed`）。`--dry-run` では `+ host`、`~ host`（変更フィールドごとの `from -> to`）、`- host` に加え、適用により削除されるデバイス設定（`- setting`: デバイスがホストから外れた、`control = false` になった、またはホストからコントローラが外れた場合）が出力されます。`machine show` にはホスト一覧と各デバイスの有効ポリシー（デバイス個別の値、未設定ならホストの値）も表示されます。

#### データストア宣言（`[[stores]]`）

任意の `[[stores]]` テーブルでは、Kairo がデータを保存するストアを宣言します（[データストア](#データストア) を参照）。

- `id`: 一意なストア ID。
- `class`: `durable`（永続保持用。Kairo は保存済みデータを削除せず、未完了に終わった自身のコピーのみを削除します）または `volatile`（スクラッチ領域用）。
- `node`: ストアが属するノード。
- `root`: 絶対パスのルートディレクトリ（例: `E:/kairo`、`/srv/kairo`）。このルートは Kairo 専用です。
- `filesystem`: ストアが置かれているファイルシステム（そのノードに登録されているディスクリソースの識別子と厳密に一致する値で、アドミッション計算で使われるものと同じです）。
- `backend`: バックエンド種別（現時点では `local-fs` のみ。省略時は `local-fs`）。
- `reserve_free_bytes`: 確保する空き容量。
  - `durable` ストアでは維持すべき空き容量であり、これを下回るコピーは実行されません。
  - `volatile` ストアではこれを下回るとエビクション（[データストア](#データストア) のエビクションを参照）が発動するしきい値であり、未設定時は総容量の 1/10 になります。
  - いずれのクラスでも、pack や fetch でストアへ書き込む際にファイルシステム上に残さなければならない空き容量として機能します（未設定時は 1 GiB）。これにより、同じファイルシステム上の他のデータ（実行中 Attempt のチェックポイントやシステム領域など）がディスク満杯になるのを防ぎます。
- `executor`（任意）: ノード上でこのストアを提供する単一の Executor（省略時はそのノードのすべての Executor が提供します）。

ストアの `root` には以下の制約があります。

- 同一ノード上の 2 つのストアが同じ場所を指したり、一方が他方の配下にネストしたりすることはできません（指定した `executor` の有無や組み合わせを問いません）。
- パス比較は正規化された単一表記で行われるため（区切り文字の統一、`.` および末尾区切り文字の除去、ドライブパスでは大文字・小文字を区別しない）、`E:/data`、`e:\Data\`、`E:/./data` は同一の場所とみなされます。同じルートを `executor` あり・なしの両方で宣言することも拒否されます。
- `root` に `..` や制御文字は含められません。POSIX ルートには空白文字やバックスラッシュを含められません。ドライブパスには Windows が別名として解釈する名前（末尾がドットまたは空白、8.3 短縮名のような `~`、`:`、その他ファイル名に使用できない文字を含むもの）を含められません。
- エージェント側でも各ルートの実体の場所を比較します（[データストア](#データストア) を参照）。

ストア定義は `apply` 時に全体が置き換えられます（`+ store`、`~ store`、`- store`。イベント `store_declaration_changed`）。ストアを宣言から外してもデータは削除されません。また、別の `root` で宣言し直した場合、旧ルートにあるデータは手付かずのまま残り、以降は使用されなくなります。データを新ルートへ引き継ぐには、新ルートの宣言に `moved_from` で現在のルートを指定してください（[データストア](#データストア) のストア移動を参照）。`machine show` にはストア一覧も表示されます。

#### マシン宣言 API

- **`POST /api/machines`**
  - リクエスト: `{"nodes": [{"id", "gpus"}], "hosts": [{"id", "nodes", "controller", "max_slowdown", "devices": [{"id", "max_slowdown", "control"}]}], "stores": [{"id", "class", "node", "executor", "root", "filesystem", "backend", "reserve_free_bytes", "moved_from"}], "dry_run"}`
  - レスポンス: `{"changes": [{"uuid", "from", "to"}], "unchanged", "host_changes": [{"host", "from", "to"}], "unchanged_hosts", "store_changes": [{"store", "from", "to"}], "unchanged_stores", "cleared_settings", "dry_run"}`
  - Lease 中の GPU に対する競合操作は `409 machine_conflict` を返します。
- **`GET /api/machines`**
  - レスポンス: `{"gpus", "undeclared", "hosts", "devices", "stores"}`
  - `devices` の各要素には `host_id`、`device_id`、`kind`、`max_slowdown`、`control`、`controller` が含まれます。
- ストア機能より古いデーモンは `stores` を含む宣言を拒否します（`stores` が未知フィールドとなるため）。

### コードスナップショット

Task は宣言時点のコードで実行されます。書き換えによって失われうる git コミット名で参照するのではなく、Kairo 自身がコードを保持します。`kairo project apply` はファイル内の Task が指定する各ソースをスナップショット化してデーモンに保存するため、後から作業ツリーや git 履歴がどう変化しても影響を受けません。

```toml
[sources.example-app]
root = "."            # このファイルのディレクトリからの相対パス（デフォルト）
paths = ["src", "crates", "configs", "pyproject.toml", "uv.lock", "Cargo.toml", "Cargo.lock"]
exclude = ["__pycache__", "*.pyc", "target", ".venv"]

[[tasks]]
name = "train-x"
# ...
[tasks.execution]
source = "example-app"
cwd = "."             # スナップショット内の作業ディレクトリ
argv = ["uv", "run", "python", "-m", "neo_ime.cli", "train", "--config", "configs/x.toml"]
```

#### スナップショットの作成・保存・バインド

スナップショットには、`root` 配下の `paths`（各要素はファイルまたはディレクトリで、ディレクトリは丸ごと取得）に含まれる通常ファイルが格納されます。ただし `exclude` パターンに一致するものは除外されます（スラッシュを含まないパターンは任意のパス要素に一致し、スラッシュを含むパターンは相対パス全体に一致します）。リンクや特殊ファイルは取り込まれません。

スナップショットはデータオブジェクトと同じ形式（`tar.zst` と、各ファイルのパス・バイト数・sha256・mtime を記したファイル一覧）でパックされ、mtime を除いたファイル一覧の sha256 から命名されます（`src_` に続けて 32 桁の 16 進数）。そのため、同じファイル構成であれば常に同一のスナップショット ID になり、無駄に作り直したり再送信したりすることはありません。

CLI はデーモンに存在しないスナップショットだけを送信します（`POST /api/sources`、multipart 形式: ファイル一覧と作成元情報を含む `meta`、続けて `pack`）。デーモンは以下を検証します。

1. ファイル一覧とスナップショット ID の一致
2. `pack` のサイズと sha256
3. `pack` 内の各ファイルとファイル一覧の一致、および一覧外のファイルが含まれていないこと

検証を通過したスナップショットは、デーモン設定の `source_directory`（デフォルトは `log_directory` と同じ階層の `sources`）に保存され、`source_snapshots` テーブルに記録されます（プロジェクト名、ソース名、作成元のホスト・ディレクトリ、そのディレクトリの git HEAD と未コミット変更の有無を記録しますが、これらは参照用情報のみです。イベント `source_snapshot_recorded`）。同一のスナップショットが同時に 2 回送信された場合（中身が同じで mtime だけ異なる別の pack の場合を含む）、先に記録された 1 つだけが保持され、既存ファイルが上書きされることはありません。

その後 `apply` は、今回新たに宣言する各 Task をそのソースのスナップショットに恒久的にバインドします（`orchestration_tasks.source_snapshot_id`）。以前宣言済みの Task は内容が不変であるため対象外です。後から別コードで `apply` しても新しく宣言された Task のみに紐づき、再実行（rerun）ではその Task が宣言された当時のコードが実行されます。古い `kairo` CLI などで、`apply` 時にスナップショットが送信されなかったソースを指定した Task は拒否されます。

#### ノードでの展開と改変検知

Task の各 Execution はそのスナップショット内で実行されます（`execution_requests.source_snapshot_id`。API から投入する Execution では `source_snapshot` で指定します）。Executor はスナップショットを自ノードへ 1 回取得し（`GET /api/agent/sources/<id>/pack` および `/list`。取得した入力データと同様に、記録済みのサイズ・sha256 およびファイル一覧に対する各ファイルの一致を検証）、自身が提供する最初の `volatile` ストアの `<root>/sources/<id>` に展開します。Attempt はそこを基準とした相対パスを `cwd` とし、`KAIRO_SOURCE_DIR` と `KAIRO_SOURCE_ID` を設定した状態で起動されます。

- 実行可能権限付きでパックされたファイル、および先頭が `#!` で始まるファイル（Windows 上で作られたスナップショットには実行ビットがないため）は実行可能ファイルとして展開されるため、Linux 上で `./scripts/x.sh` などがそのまま実行できます。
- Windows では、ストアハンドル経由で作成できない予約済みデバイス名のエントリは検証のみ行ってスキップし、該当パスをログ出力します。

展開されたスナップショットは読み取り専用に設定されます（すべての OS でファイルが読み取り専用となり、Windows 以外ではディレクトリも読み取り専用になります。Windows ではディレクトリを読み取り専用にしても中へのファイル作成を防げないためです）。ただしこれだけでは Linux の root や Windows 上でのファイル追加は防げないため、展開時の状態を隣接ファイル `<root>/sources/<id>.state`（各ディレクトリ、および各ファイルのサイズ・mtime・モード）に記録し、起動のたびに毎回ツリーと照合します。

- 変更（ファイルの追加・削除・書き換え）が見つかったツリーは `<id>.changed-<time>` として退避され（その中で実行中の Attempt があっても、その Attempt は退避されたディレクトリをそのまま使い続けます。Executor のログには最初に検出された差分パスが出力されます）、スナップショットが再展開されます。これにより、すべての Attempt が必ず宣言時のファイル状態で開始されます。
- ビルド成果物やインストール先はスナップショット外に配置してください（Task の環境変数で uv 環境は `UV_PROJECT_ENVIRONMENT`、Rust は `CARGO_TARGET_DIR` を指定します）。
- スナップショット内で実行される Attempt には、環境変数に `PYTHONDONTWRITEBYTECODE` または `PYTHONPYCACHEPREFIX` が設定されていない限り `PYTHONDONTWRITEBYTECODE=1` が自動設定され、Python がスナップショット内にバイトコードを書き出さないようにします。
- コンファインメントされた Attempt はスナップショット内へ一切書き込みできません。

#### 展開済みスナップショットのクリーンアップと CLI

展開済みスナップショットは永続的に残るわけではありません。各 Executor は 1 時間ごとにデーモンへ問い合わせ、まだ終了状態になっていない Execution が使用中のスナップショット一覧を取得します（エージェント操作 `sources-in-use`）。この応答が得られない限り削除は一切行いません。応答を得たうえで、自身が提供する `volatile` ストアの `<root>/sources` から以下を削除します。

- どの非終了 Execution にも使われておらず、最後に起動されてから 3 日以上経過した展開済みスナップショット（途中まで削除されたツリーが「変更あり」と判定されるよう、まず `.state` ファイルを先に削除します）
- 1 時間以上前に変更ありとして退避されたツリー（対象スナップショットが使用中でなくなった後）
- 対応するツリーが存在しない `.state` ファイル
- 1 日以上前に展開が途中で終わった残骸

上記以外のものには一切触れません。`<root>/sources` へは通常のディレクトリのみを経由してアクセスし、途中の経路や `sources` 自体がリンクである場合は展開時も含め拒否されます。また、削除対象ツリー内のファイルは書き込み可能に変更せずそのまま削除します（他所へのハードリンクである可能性があるためです）。一方、デーモン自身は受信したすべてのスナップショットを各 Task の実行記録として保持し続けます。

CLI および API（`GET /api/sources`、`/api/sources/<id>`）では以下を利用できます。

- `kairo source list [--project P]`: スナップショット一覧を新しい順に表示（プロジェクト、ソース、ファイル数、サイズ、git 状態）
- `kairo source show ID [--files]`: 指定スナップショットの詳細と、それにバインドされた Task 一覧を表示
- `kairo source diff A B`: スナップショット A から B への追加（`+`）、削除（`-`）、変更（`M`）ファイルを表示

### コンテナ実行

コードスナップショットは Task が実行するコードを固定しますが、その土台となる実行環境（Python やパッケージ、システムライブラリ）はホスト側のものであり時間とともに変化します。代わりに Task は、ダイジェストで固定されたコンテナイメージ内で実行することもできます。

```toml
[images.torch]
ref = "pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime"   # タグ指定、または name@sha256:...

[[tasks]]
name = "train-x"
# ...
[tasks.execution]
image = "torch"
source = "example-app"     # /kairo/src に読み取り専用でマウントされ、cwd はそこからの相対パス
cwd = "."
argv = ["python", "-m", "neo_ime.cli", "train", "--config", "configs/x.toml"]
```

#### イメージの宣言とダイジェスト固定

イメージ参照は Docker と同じ規則で解釈されます（レジストリ省略時は Docker Hub とみなし、1 単語のリポジトリは `library/` 配下、タグ省略時は `latest`）。`kairo project apply` は各イメージのレジストリに対してその参照が現在指しているダイジェストを問い合わせ（レジストリ API を匿名で利用し、パブリックイメージの pull にレジストリが要求する bearer トークンを使用）、宣言とともに送信します（`image_pins`。スナップショットと同様に宣言自体のダイジェストには含まれません）。

デーモンは今回宣言された各 Task をそのダイジェストへ恒久的にバインドします（`orchestration_tasks.image_ref`、形式は `registry/repository@sha256:...`）。後から同じタグに別イメージが push された後に再 `apply` しても、新しいダイジェストがバインドされるのは新たに宣言された Task のみです。各 Execution には実行イメージが記録され（`execution_requests.image_ref`。API 経由で Execution を投入する場合はダイジェスト固定済みのイメージを `image` として指定します）、`kairo task show` にも `image:` として出力されます。`source` を指定しない Task はイメージ自身のファイルシステム上で動作するため、絶対パスの `cwd` を指定する必要があります。

#### Incus Executor の要件と設定

**すべての Task はコンテナで動きます。** image を指定しない Task を含む宣言は `kairo project apply` で拒否され、image を持たない Execution の直接投入（`POST /api/executions`、`kairo execution submit`。`image` にはダイジェスト固定の参照 `registry/repository@sha256:...` を書きます）も拒否されます。どの Executor もコンテナを動かすため、そうした仕事は実行先がなく、待ち続けるだけだからです。イメージを指定した Execution を受け付けるのはコンテナ実行用 Executor のみであり、コンテナ実行用 Executor はそれ以外の Execution を受け付けません。`kind = "incus"`・`kind = "wslc"` の Executor は属性 `runtime`（値は kind と同じ）を登録し（`labels` に `runtime` を手動指定すると拒否され、`kind` のみが設定できます）、イメージを持つ Execution のセレクタにはイメージ指定に基づいて `"container": true` が自動設定されます（呼び出し元から直接設定することはできません）。

`kind = "incus"` の Executor は Linux 上で動作し、ノード上の他の Executor と並存できます。

```toml
[[executors]]
id = "host-a-ctr"
kind = "incus"
labels = { environment = "linux", site = "host-a" }
```

必要な環境要件は以下のとおりです。

- **Incus 6.3 以降**: OCI イメージ対応が必要です（Debian 13 の 6.0 LTS には対応がなく、Zabbly パッケージを利用する必要があります）。また、デフォルトプロファイルにストレージプールとネットワークが設定されている必要があります。
- **GPU 利用時**: ノード上に `nvidia-container-cli`（`libnvidia-container-tools`）とドライバのユーザー空間ファイルが必要です。
- **コンテナの権限**: ホスト上では、コンテナは非特権です。コンテナの root は、root の `/etc/subuid` の範囲の先頭の ID（既定は 100000。Proxmox のコンテナと同じ）です。コンテナが書き込む領域は、起動の前に、コンテナの ID の範囲（root から 65536 個。uid と gid はそれぞれのマップから読みます）の外の ID が持つもの（ホストやエージェントが書いたもの）だけを、コンテナの root の所有にします。範囲の中の ID が持つもの（attempt が切り替えた利用者が書いたもの）はそのままにするので、同じキャッシュを使う別の attempt が走っていても、その持ち物は変わりません。たどるときはディレクトリを記述子で開き、リンクは一切たどりません（走っているコンテナがリンクを置いても、領域の外には及びません）。読み取り専用で渡すもの（データ、Kairo のバイナリ、GPU のドライバ）は、所有者を変えずに渡します。エージェント自身が非特権の LXC の中で動いているとき（入れ子）は、LXC には UID マップが 1 つしかないため、コンテナを特権にします（`security.privileged`。物理ホストから見れば非特権のままです）。
- **AppArmor（Proxmox のホスト）**: Incus が作る AppArmor のプロファイルは ABI を宣言しません。Debian の既定の ABI のままだと、Proxmox のカーネル（7.0）はコンテナの中での UNIX ソケットの作成を拒否します（`failed protocol match`）。`/etc/apparmor/parser.conf` の `policy-features` を、Proxmox の LXC が使う `/etc/apparmor.d/abi/lxc` にします。

Proxmox VE のホストに直接入れる場合の手順です（Debian 13）。VM のための依存を入れないよう、コンテナだけの `incus-base` を使います。

```sh
curl -fsSL https://pkgs.zabbly.com/key.asc -o /etc/apt/keyrings/zabbly.asc   # 指紋 4EFC 5906 96CB 15B8 7C73 A3AD 82CC 8797 C838 DCFD
cat > /etc/apt/sources.list.d/zabbly-incus-stable.sources <<EOF
Enabled: yes
Types: deb
URIs: https://pkgs.zabbly.com/incus/stable
Suites: trixie
Components: main
Architectures: amd64
Signed-By: /etc/apt/keyrings/zabbly.asc
EOF
apt-get update && apt-get install --no-install-recommends incus-base
# ストレージは Proxmox と同じ thin pool、ネットワークは vmbr0 へのブリッジ（DHCP）
cat <<EOF | incus admin init --preseed
storage_pools:
- name: default
  driver: lvm
  config: {source: pve, lvm.vg.force_reuse: "true", lvm.thinpool_name: data}
profiles:
- name: default
  devices:
    root: {type: disk, path: /, pool: default}
    eth0: {type: nic, name: eth0, nictype: bridged, parent: vmbr0}
EOF
```

pve-firewall を有効にしているホストでは、**入れる前に** `/etc/sysctl.d/90-kairo-bridge-nf.conf` に `net.bridge.bridge-nf-call-iptables = 1` と `net.bridge.bridge-nf-call-ip6tables = 1` を書きます。`incus-base` は入れるとき（更新のたびにも）`sysctl.d` を再適用します。そのとき Proxmox の `10-pve.conf` によってブリッジのフィルタが一瞬 0 に戻り（pve-firewall は実行時に 1 にしているだけです）、conntrack の追跡が途切れます。その結果、`firewall=1` のゲストどうしで張られていた接続が INVALID として捨てられます（TCP の再送が尽きる約 15 分後まで止まります）。

Incus は自分の lxcfs（`/var/lib/incus-lxcfs`）を使い、Proxmox の LXC とは別に動きます。Incus が thin pool に作るボリューム（`images_*`、`containers_*`）は、Proxmox の一覧には出ません。

#### WSL コンテナ Executor（`kind = "wslc"`）

Windows 上のエージェントは、`kind = "wslc"` の Executor で、Attempt ごとに WSL のコンテナ（`wslc`、WSL 3 以降に付属）を作り、終わったら削除します。Windows の GPU を Linux の仕事で使うための Executor です（Windows でしか動かない仕事は Kairo の対象外です）。

```toml
[[executors]]
id = "host-w-wslc"
kind = "wslc"
labels = { site = "host-w" }
```

- **ランナー**（`__kairo-oci-run`。wslc のときの動作）: イメージをダイジェストで取得し（`wslc pull`）、同じ名前の残りを消してから、`wslc run` を前景で動かして停止を待ち、`/kairo/run/exit` の終了コードを読んでコンテナを削除します。Attempt の停止（CTRL_BREAK）が届くと、`wslc stop` でコンテナを止めます。
- **init**: エージェントの横に置いた Linux 版のバイナリ `kairo-linux` を `/kairo/bin/kairo` にマウントし、`__kairo-container-init` として動かします（`ops/windows/replace-agent.ps1` が Windows 版と一緒に置きます）。
- **閉じ込めの代わりに一般ユーザーで動かす**: wslc のコンテナはマウントができない（CAP_SYS_ADMIN がない）ため、init は Incus のような読み取り専用化をしません。代わりにコンテナ全体を uid 1000 で動かします。Kairo が渡す領域は書けますが、イメージのファイルは書けないので、`/kairo` の外に書いたものは黙って消えず、書いた時点で失敗します（`/tmp` だけは書けますが、`TMPDIR` は scratch を指します）。`HOME` は scratch の `home` です。実行時に root を前提とするイメージ（実行中の `pip install` など）は動きません。
- **停止**: 他のランタイムと同じく、停止はランナーに届き、ランナーが `wslc stop` でコンテナを止め、init が `argv` のプロセスグループへ SIGTERM を渡します（30 秒後に wslc が強制終了します）。プロセスの生死は、コンテナが動いているかどうかで判断します。
- **GPU**: `--gpus all` で WSL の GPU（`/dev/dxg` と `/usr/lib/wsl/lib`）を渡し、`CUDA_VISIBLE_DEVICES` に Lease した GPU の UUID を入れます。glibc のイメージに限ります（alpine では wslc の GPU フックが失敗します）。
- **ネットワーク**: コンテナは wslc のブリッジから外へ出られ、デーモンにも直接届きます。
- **stream**: コンテナは VM の中にあるため、raw キャッシュの窓口のソケットに届きません。Loader は窓口の横のディレクトリ（`/kairo/raw-requests`）にファイルを置いて agent に依頼し（`kairo loader serve --raw-requests`）、agent はそれを 0.1 秒ごとに見て、ソケットと同じように答えます。
- **掃除**: ランナーが強制終了されて残ったコンテナ（`kairo-att-*` で、Attempt がもうこのノードで動いていないもの）は、掃除処理が削除します。

#### コンテナの起動・終了ライフサイクルとマウント構成

Attempt のランチャーは shim であり、エージェントのバイナリをランナー（`__kairo-oci-run`）として起動します。ランナーはどのランタイムでも同じで、ランタイムごとの違い（作り方・止め方・消し方・一覧）だけをランタイムの実装（`ociRuntime`：Incus・wslc）が持ちます。後片付け（ランナーが強制終了されて残ったコンテナの削除）、止め方、プロセスの生死の判定、使用量の測り方も共通です（下記）。以前のランナーの引数（`__kairo-incus-run`・`__kairo-wslc-run`）も受け付けます。Incus のときは、ランナーはイメージからインスタンス `kairo-<attempt id>` を作成し（`incus init REMOTE:REPOSITORY@DIGEST`。初回はそのレジストリ用の OCI リモートを追加）、起動して停止を待ち、コンテナが書き出した終了コードを読み取ってインスタンスを削除し、その終了コードで終了します（終了コードが書かれていなかった場合は `-1`）。実行中にエージェントが再起動しても、他の Attempt と同様に引き継がれます（[エージェント再起動と引き継ぎ](#エージェント再起動と引き継ぎ) を参照）。

コンテナ内では、読み取り専用でマウントされた同一のバイナリが Incus 自身の init の配下でコンテナ用 init（`__kairo-container-init`）として動作します。

- コンファインメント（`confine`）の指定どおりにコンテナ内を制限します（Kairo が渡すディレクトリ以外はすべて読み取り専用、`/tmp` は scratch 配下のディレクトリ、`/proc` と `/dev/shm` は書き込み可能。[アドミッション制御とプランニング](#アドミッション制御とプランニング) のコンファインメントを参照）。
- 指定されたディレクトリで `argv` を専用のプロセスグループとして起動し、その出力を Attempt のログへ追記します。
- コンテナ内で終了したプロセスを回収（reap）し、`argv` の終了コードを書き出します（シグナルで終了した場合はそのシグナルも Attempt の stderr に書き出します）。
- `__kairo-container-init` 自身が終了するとコンテナが停止し、内部に残っていたプロセスもすべて終了します。

強制停止、ノード隔離、および終了済み Attempt の残留の終了は、どのランタイムでも同じ経路をとります。

- **穏やかな停止**: エージェントはランナー（shim の下のプロセスグループ。Windows では CTRL_BREAK）にだけ停止を送ります。ランナーはランタイムでコンテナを止め（`incus stop`・`wslc stop`、30 秒の猶予つき）、コンテナの init（`__kairo-container-init`）が `argv` のプロセスグループへ SIGTERM を渡します（lxc が送る停止のシグナル SIGPWR も SIGTERM として渡します）。猶予を過ぎるとランタイムが残りを止めます。ランナーがいない（強制終了された、引き継いだ Attempt でシグナルを送れない）ときは、エージェントがランタイムで直接コンテナを止めます。
- **強制終了**: エージェントはランタイムでコンテナを即座に止め（`incus stop --force`・`wslc kill`）、ランナーのプロセスグループも終わらせます。
- **生死**: Attempt が登録したプロセスは、コンテナが動いている間は生きているものとして扱います。ランナーのプロセスグループかコンテナのどちらかが動いていれば、Attempt はまだ動いています。ホストから `KAIRO_ATTEMPT_ID` でプロセスを探すことはしません。
- **使用量**: RAM と書き込んだバイト数は、ランタイムの統計から取ります（Incus は RAM だけで、書き込みは不明として扱います。wslc は `wslc stats`）。

ランナーはコンテナが止まるのを見届けてから削除します。ランナーが強制終了されて残ったコンテナは、動いているかどうかにかかわらず、後述の定期スイープが削除します。

Kairo が渡した領域以外に Attempt が書き込んだ内容はインスタンス削除とともに消えます。Kairo が渡す領域は全ノード共通で `/kairo` 配下の同一パスにマウントされるため、継続（continuation）実行時もチェックポイントに記録されたパスでそのまま前回のファイルを参照できます（別ノードで続けるときは、先にディレクトリを持ち込みます。[Run の output の持ち込み](#run-の-output-の持ち込み)）。

| パス | 内容 |
|---|---|
| `/kairo/scratch` | `KAIRO_SCRATCH`、`TMPDIR`、`TMP`、`TEMP`（その配下の `tmp/` が `/tmp`） |
| `/kairo/output` | `KAIRO_OUTPUT_DIR` |
| `/kairo/cache` | `KAIRO_CACHE_DIR` |
| `/kairo/src` | `KAIRO_SOURCE_DIR`、コードスナップショット（読み取り専用） |
| `/kairo/inputs/<name>` | `KAIRO_INPUT_<NAME>`（読み取り専用） |
| `/kairo/nvidia` | ノードの NVIDIA ドライバファイル群（GPU 利用時、読み取り専用。wslc では使わない） |
| `/kairo/raw`, `/kairo/raw-requests` | stream の raw キャッシュの窓口（読み取り専用）と、wslc での依頼の置き場 |
| `/kairo/run`, `/kairo/log`, `/kairo/bin` | init の spec と終了コード、ログ、バイナリ |

#### 環境変数・GPU 受け渡し・制約事項

- **環境変数と `/dev/shm`**: コンテナの環境変数は「イメージの環境変数 → 宣言された env → Kairo の環境変数」の順に構築されます。Kairo が渡すパスはすべて上記のコンテナ内パスとなり、その配下を指すパスも書き換えられます（例: 継続時の `KAIRO_CONTINUATION_REF`）。エージェント自身の環境変数はコンテナには渡されません。`/dev/shm` はメモリの最大半分を上限とする tmpfs です（PyTorch のデータローダワーカーがバッチ共有に使用します。ここに書き込まれた分はメモリ使用量となります）。
- **GPU の受け渡し**: Lease された GPU は UUID から特定されたデバイスノード（`/dev/nvidia<minor>`）および制御デバイスとしてコンテナに渡されます。ドライバのライブラリとプログラム群は `nvidia-container-cli list` が列挙するものをドライババージョンごとに 1 回 `<log dir>/containers/nvidia/<version>` にまとめて配置し（soname 付きでリンク）、`/kairo/nvidia` にマウントします（`lib` を `LD_LIBRARY_PATH` の先頭に、`bin` を `PATH` の末尾に追加）。`CUDA_VISIBLE_DEVICES` には UUID が設定されます。なお、ネストされたコンテナ内では cgroup のデバイスプログラムを読み込めないためデバイスアクセス自体はフェンスされません。その GPU が Attempt の所有物であるのは、Kairo が Lease していることに基づきます。
- **プロセス帰属・チェックポイント・後始末**: Incus のコンテナ内のプロセスはホストの `/proc` から見え、`KAIRO_ATTEMPT_ID` を持つため、GPU プロセスの帰属が機能します。止め方・生死・使用量はランタイムを通します（上記）。コンテナが公開するチェックポイントはコンテナ内パス（`/kairo/output/...`）を指すためインポートは行われず（[実行中ジョブのチェックポイント保護](#実行中ジョブのチェックポイント保護) を参照）、チェックポイント公開が指示する Run スナップショットによって保持されます。また、Executor は 10 分ごと、およびコンテナ上の Attempt 終了直後に、もはやそこで実行されていない Attempt のインスタンスを削除します（ランナーが強制終了された場合はランナーによる削除が行われないためです）。
- **現時点の制約**: 実行中コンテナの状態そのものを保持する機能はまだなく、処理の継続はジョブ自身のチェックポイントから再開します。

### データストア

ストア（[マシン宣言](#マシン宣言) の `[[stores]]`）は、Kairo がデータを保存する領域です。durable store は格納されたデータを恒久的に保持し、volatile store は一時作業領域（scratch space）として機能します。

ストア内で Kairo が操作対象とするのは自身が管理するデータのみであり、具体的にはストアのルート配下に自身で作成したもの（および P1b 以降は、durable store 上に検証済みコピーが存在するもの）に限られます。同一ファイルシステム上にあるそれ以外のデータは使用量の集計対象にはなりますが、移動・変更・削除されることは一切なく、ルート外の領域が走査されることもありません。

#### ストアの認識・検証・監視

有効化された各 Executor は、自ノードに属するストアのうち、`executor` として自身を指定しているもの、および `executor` が未指定のものを担当します。Executor は起動時および 5 分ごとにデーモンへ自ノードのストア一覧を問い合わせます（エージェント操作 `stores`）。

ルートディレクトリが存在しない volatile store については、Executor が自動的にルートを作成します。一方、durable store のルートは決して自動作成しません。ルートが存在しない場合はディスクが未マウントである可能性があり、そのまま作成して書き込むと別のディスク上にデータが書かれてしまうためです。durable store は、ルートが実際に存在するようになるまで利用不可（`unusable`）として扱われます。

Executor は各ストアについて、以下の条件を検証します。

- **パス上にリンクを含まないこと**: ルートパスのどの構成要素もリンク（シンボリックリンク、ジャンクション、その他のリパースポイント）であってはなりません。
  - **Windows**: 開いたルートが解決されるパス（ドライブ文字を含む）が宣言されたルートと一致する必要があり、代替ドライブ（`subst` ドライブ）や短い 8.3 形式の名前も許可されません。
  - **Linux**: ルート自体がその実パス（real path）と一致する必要があります。
- **宣言されたファイルシステム上にあること**:
  - **Windows**: ルートのドライブが NTFS または exFAT である必要があります。
  - **Linux**: 宣言されたファイルシステムと同一デバイス上にあり、かつそのファイルシステムが `/proc/self/mountinfo` 上のマウントポイントである必要があります（未マウントのディスクでは単なる通常ディレクトリが残るためです）。
- **ハンドル経由の安全な逐次検証**: ルートを開く際（および存在しないルートを作成する際）は、ボリュームのルートから 1 ディレクトリずつ親のハンドル経由でたどり、対象の通常ディレクトリであることを都度確認します。これにより、途中でリンクが差し込まれた場合であっても、リンク経由でディレクトリが作成・到達されることはありません。
- **Linux における volatile store の重複・包含禁止**: Linux では、この Executor が担当する他のストアのルートとディスク上で同一の位置にある、あるいはその内側・外側にある（包含関係にある）volatile store も使用されません。宣言上のパスだけでは判別できないバインドマウントを含め、`/proc/self/mountinfo` から得たデバイスとファイルシステム内パスで比較されます。

検証に失敗したストアは使用されず、その理由とともに利用不可（`unusable`）として報告されます。

Executor は 5 分ごとにバックグラウンドで各ストアの状態を報告します（エージェント操作 `store-observation`）。報告には直近で読み取った宣言上のルートが付与され、すでにそのルートで宣言されていないストアの報告は拒否されます。また、同一ストアに対する 2 つの報告は直列に（1 つずつ順番に）書き込まれます。報告内容と走査ルールは以下のとおりです。

- **報告項目**: ファイルシステムの総容量と空き容量（バイト数）、およびルート配下にある各 Kairo scratch ディレクトリ（自身のマーカーが自身を指しているもの）とそのサイズ。
- **測定頻度と走査方式**: まだ実行中の Attempt が使用している scratch ディレクトリは毎回の報告で測定され、それ以外はエージェントプロセスごとに 1 回測定されます（保持された scratch は保持時に測定されます）。走査時にリンクをたどったりマウントポイントに入ったりすることはなく、各ディレクトリは 1024 エントリずつ読み込まれます。
- **`missing` の判定**: `<root>/scratch` 全体の読み取りを完了した報告において、そのルートに記録されているのに見つからなかった scratch は、再び見つかるまで `missing`（バイト数 `0`、`kairo store status` に一覧表示）としてマークされます。Kairo 自身が削除したわけではないため、削除済みとはみなされません。なお、そこに存在するディレクトリのマーカーが現在読み取れない場合（パーミッション等によるもの）は、`missing` とはみなされません。
- **サンプリング順序**: Executor は scratch と output のスキャンを完了した後、最後にファイルシステム使用量をサンプリングします。

#### ストア状態の表示（`kairo store status`）

`kairo store status [--json]`（`GET /api/stores`、レスポンス形式 `{"stores": [...]}`）は、各ストアの種別（class）、ノード（および Executor）、状態（`state`）、ファイルシステムの総容量と空き容量、Kairo による種別ごとの使用量、および外部使用量を表示し、続けて各ストアの保持中（kept）の scratch と、各 durable store の `damaged` コピーを表示します。各ストアの最新報告は `store_observations` テーブルに保存されます。

**ストアの状態（`state`）**

| 状態 | 説明 |
|---|---|
| `unknown` | 一度も報告がない、担当するエージェントがいない、またはストア機能より古いバージョンのエージェントが動作している |
| `ready` | 正常に利用可能 |
| `unusable` | 利用不可（原因を示す `problem` を併記） |
| `stale` | 15 分間報告がない |
| `moving` | ストアを移動中（後述） |
| `undeclared` | 宣言から削除されたが、保持中の scratch がまだ残っている |

**使用量の内訳**

| フィールド / 表示項目 | 説明 |
|---|---|
| `scratch_active_bytes` | 実行中 Attempt の scratch 使用量 |
| `scratch_kept_bytes` | 保持されている scratch の使用量 |
| `output_bytes` / `outputs`（`OUTPUTS` 表示） | 最新報告で確認された Run output ディレクトリの合計バイト数と件数 |
| `pack_bytes` / `packs`（`PACKS` 表示） | そのストア上で作成され未削除の pack（ソースが存在するもの、または保持されているソースに紐づくもの）の合計バイト数と件数 |
| `copy_bytes` / `copies`（`COPIES` 表示） | durable store 上の検証済みコピーの合計バイト数と件数 |
| `pinned_bytes` | 現時点では `0` |
| `cache_bytes` | そのストアに持ち込まれた入力データのサイズ |
| `external_bytes` | ファイルシステム全体の使用量から Kairo の上記各クラス合計を差し引いた値（Kairo の管理外データ） |

後方互換性として、ストア機能より古いバージョンのエージェントが動作している場合、そのストアは `unknown` のままになります。逆にデーモン側がストア機能より古い場合は、エージェントの `stores` 要求に HTTP `404` を返し、エージェントは（1 度だけ警告を出したうえで）ストアなしで動作します。

#### ストアの移動（`moved_from`）

ストアの宣言において、現在のルートを `moved_from` に指定したうえで新しいルートを宣言すると（例: `root = "D:/_kairo-volatile"`、`moved_from = "D:/kairo-store"`。他の属性は変更せず、同一ドライブまたは同一ファイルシステム内であること）、ストアはコピーではなくリネームによって移動されます。

移動が完了するまでの間、システムは以下のように動作します。

- **既存レコードの固定と I/O 停止**: ストアに紐づくすべてのレコード（scratch、Run output、データオブジェクトとその pack、コピー、そのストアに持ち込まれたオブジェクトやそこから取得されるオブジェクト）は旧ルートにとどまり、旧ルート・新ルートのいずれに対しても読み取り・書き込み・削除は行われません。
- **実行の待機**: そのストアを指定している Execution は待機状態（`store_moving`）になります。直前にアドミッションされた Execution も起動されず、予約が解放されて待機に戻ります。また、移動完了までルートを再度変更することはできません。
- **使用中リソースの通知**: ストアは移動中 `unusable` 扱いとなり、`kairo store status` には `moving` と待機対象が表示されます。ノード側には、旧ルートでまだストアを使用している以下の要素が通知されます。
  - そのストアを使用している（ストアを指定している、またはそこに scratch を持つ）Execution のうち、Lease を保持しており、実行中・起動中・ロストして未リコンサイル状態にあるもの（プロセスがまだ書き込んでいる可能性があるため）
  - 削除中の scratch
  - そのストア上でパッキング中・削除中・エビクト中、または転送用パッキング中のオブジェクト
  - そのストア上で作成中のコピー
  - そのストアへ持ち込み中・エビクト中、またはそこから取得中のオブジェクト

使用中の要素がすべてなくなると、担当 Executor は旧ルート内にマーカー `.kairo-store-move`（ストア名および新旧両ルートを記録）を書き込み、旧ルートを新ルートへリネームします。リネーム先の新ルートは未存在である必要があります（親ディレクトリは存在している必要があります）。Windows ではディレクトリ内のファイルが開かれている間はリネームが拒否されるため、次回のリフレッシュ時に再試行されます。なお、このマーカーを持たずにすでに存在している新ルートは、そのストアとしては認識されません。

次の報告で Executor が移動完了を伝えると、デーモンは 1 つのトランザクション内でストアに関する全レコードを新ルートへリベースします（各パスはその固有の区切り文字のまま変換されます。イベント `store_moved`）。その後、Executor はマーカーを削除します。

移動完了後に宣言ファイルへ `moved_from` を残したままにしてもそれ以上の効果はないため、同じ宣言ファイルを変更せずに再適用できます。なお、ストア移動機能より古いバージョンのエージェントは移動を実行しないため、先にエージェントをデプロイする必要があります。

#### `scratch` ディレクトリ

利用可能な volatile store を担当する Executor が Attempt を起動すると、そのストア（複数ある場合は ID が先頭のストア）上に scratch ディレクトリ `<root>/scratch/<attempt id>` が割り当てられます。このディレクトリはプロセスの起動前に作成され、Attempt ID を記したマーカーファイル `.kairo-scratch` が置かれ、環境変数 `KAIRO_SCRATCH` でプロセスに渡されます。

scratch ディレクトリは、Attempt がまだ認可されている間、デーモンへ記録されます（エージェント操作 `attempt-scratch`、`attempt_scratch` テーブルに attempt・store・root・path・状態 `active`/`deleting`/`kept`/`deleted`/`missing`・bytes・reason を記録。イベントは `scratch_created`、続いて `scratch_deleted` または `scratch_kept`）。デーモンが記録を受け入れるのは、以下の条件をすべて満たす場合に限られます。

- その Attempt が属するノードの volatile store であり、かつ担当 Executor の名前空間内にあること
- ストアのルートが宣言どおりの表記であること
- パスが厳密に絶対パス `<root>/scratch/<attempt id>` であること
- 同一ノード上の他のどのストアの内側にも含まれないこと（[マシン宣言](#マシン宣言) のルート表記ルールで比較）

ディレクトリを作成または記録できない場合、Attempt は scratch ディレクトリなしで起動します（ディレクトリは作成されたものの記録できなかった場合、マーカーしか入っていなければ削除されます）。

環境変数 `TMPDIR`、`TMP`、`TEMP` はデフォルトでは変更されません。`[tasks.execution]`（または Execution TOML）で `scratch_tmpdir = true` を設定した場合にのみ、宣言された環境変数設定より優先してこれらの変数が scratch ディレクトリを指すようになります。デフォルトで無効になっているのは、Attempt ごとに空になる一時ディレクトリを使うと、`torch` のコンパイルキャッシュなどのキャッシュが毎回失われてしまうためです。scratch ディレクトリの作成と後処理は Executor が自動的に行うため、Worker SDK は不要です。

#### `cache` ディレクトリ

利用可能な volatile store を担当する Executor が起動するすべての Attempt には、scratch が作成されるストアと同じストア上に、そのプロジェクト専用のキャッシュディレクトリ `<root>/cache/<project scope id>`（プロジェクト名を記したマーカー `.kairo-cache` を格納）も割り当てられ、環境変数 `KAIRO_CACHE_DIR` で渡されます。

このディレクトリは、同一ノード上でプロジェクト内の Attempt 同士が共有して保持するデータ（コンパイル済みカーネルやパッケージキャッシュなど）のための場所であり、`XDG_CACHE_HOME`、`TORCHINDUCTOR_CACHE_DIR`、`TRITON_CACHE_DIR`、`UV_CACHE_DIR`、`HF_HOME`、`CUDA_CACHE_PATH` などの指定先として利用できます。

Kairo はこのディレクトリを削除せず、現時点では専用クラスとしての集計も行いません（`external_bytes` として表示されます）。コンファインメントされた Attempt（[アドミッション制御とプランニング](#アドミッション制御とプランニング) のコンファインメント参照）もこのディレクトリへ書き込むことができます。

#### `output` ディレクトリ

Run が保持すべき出力データは、scratch が作成されるストアと同じストア上の output ディレクトリ `<root>/outputs/<run id>` に保存されます。このディレクトリには Run ID を記したマーカーファイル `.kairo-output` が置かれ、環境変数 `KAIRO_OUTPUT_DIR` で渡されます。

ここで **Run** とは、ある Execution とそれに続く継続（continuation）のまとまりを指します。具体的には、継続ではない Task の Execution（初回実行、rerun、ノード隔離後の再スタート）とそれに連なる継続群（サスペンド後の継続と、ノードが落ちた後の継続。Run ID は起点となる Execution の ID）、あるいは直接実行（direct execution）が 1 つの Run になります。（以前の Gang の rank 0 以外の rank では、Run ID は `<leader's run>-rank<r>` でした。）

そのストア上で起動される同一 Run のすべての Attempt には同じディレクトリが割り当てられます。最初の Attempt がディレクトリを作成し、継続（continuation）は前の Attempt がそこに書き込んだ内容（チェックポイントなど）をそのまま見つけることができます。一方、再実行（rerun）や別 Task には別のディレクトリが割り当てられるため、ある Run のファイルが別の Run と衝突することはありません。別ノードで動く継続には、先にディレクトリを持ち込みます（[Run の output の持ち込み](#run-の-output-の持ち込み)）。Attempt の scratch・output・cache は、その Run の output ディレクトリがあるそのノードのストアに置かれます（Execution がストアを指定していればそのストア）。

output ディレクトリはデーモンへ記録されます（エージェント操作 `run-output`、`run_outputs` テーブルに run・store・root・path・状態 `active`/`missing`・最後に割り当てられた Attempt・bytes を記録。イベントは初回が `output_created`、以降の各 Attempt が `output_reused`）。記録時の検証ルールは scratch と同様で、その Run で起動しようとしている Attempt のためであること、自ノードの volatile store かつ担当 Executor の名前空間内であること、厳密に `<root>/outputs/<run id>` である必要があります。作成または記録できない場合、Attempt は output ディレクトリなしで起動します。ディレクトリは作成されたものの記録できなかった場合はそのまま残されて集計対象となり、すでにその Run のディレクトリがパッキング済みであるなどの理由で恒久的に拒否された場合は、マーカーしか入っていなければ再び削除されます。

ストア報告では各 output ディレクトリも測定されます（その Run の Attempt がその Executor 上で実行中であれば毎回の報告で測定、それ以外はエージェントプロセスごとに 1 回測定）。全体スキャンで見つからなくなったものは、再び見つかるまで `missing` としてマークされます。

Run が終了すると、output ディレクトリはパッキングされ、durable store へコピーされた後に削除されます（後述）。パッキング開始以降、デーモンはそのディレクトリに対する新規 Attempt の記録を行わなくなります（書き込まれた内容がコピーされないまま削除されてしまうためです）。また、削除されたディレクトリは以降の集計対象外になります。なお、output ディレクトリ機能より古いバージョンのデーモンは Run を割り当てないため、Attempt にも output ディレクトリは渡されません。

#### Run の output の持ち込み

Run の Execution が、その Run の output ディレクトリのないノードで動く前に、ディレクトリをそのノードへ持ち込みます（`output_landings`、`internal/store/outputlandings.go`）。

- **移動（move）**: サスペンド後の継続を別ノードで動かすとき（[Run の output ディレクトリと配置](#run-の-output-ディレクトリと配置)）。
  1. ディレクトリのある Executor が、ディレクトリ全体を種類 `output-move` のオブジェクトとして pack します（temporal として扱い、durable store へはコピーしません）。
  2. 移る先のノードの volatile store（Execution が指定したストア、なければ空きの最も多いもの）を担当する Executor が、その pack を元の agent の pack サーバから取り寄せ、ファイル一覧と照合しながら `<root>/inputs/land-<run id>.partial` に展開し、目印（`.kairo-output`）を置いて `<root>/outputs/<run id>` に移します。以後は、それがその Run の output ディレクトリです（`run_outputs` に Attempt なしの `active` として記録）。
  3. 元のディレクトリは `moved` になり、元の agent が pack と一緒に消します（記録は `moved` のまま残り、Run がまたそのストアへ持ち込まれれば `active` に戻ります）。
- **チェックポイント（checkpoint）**: チェックポイントからの再実行（`kairo task rerun --from-checkpoint`）は新しい Run なので、チェックポイントを公開した Run の output オブジェクトが pack されたあと、それを同じように（検証済みの durable コピー、またはまだ元にある pack から）取り寄せ、再実行の Run の output ディレクトリとして置きます。
- **状態**: `waiting`（チェックポイントの Run の output がまだ pack されていない）→ `packing`（移動の pack 中）→ `bringing`（取り寄せ中）→ `placed`（移動: 置いた。元のディレクトリを消すまで）→ `done`。イベントは `output_move_ordered`・`output_move_packed`・`output_landed`・`output_move_done`・`output_landing_ordered` です。
- **断念**: 指示から 1 時間（大きいものは 50 MB/s で運ぶ時間の 4 倍）以内に置けなかったもの、置く前に Execution が終わったものは断念します（`failed`、イベント `output_landing_failed`）。移動の継続は元のノードで動き、再実行は `output_unavailable` で待ちます。断念した取り寄せの途中のファイル（`<root>/inputs/land-*`）は残ります。
- **置き場所にあるもの**: 移る先にすでにその Run の目印を持つディレクトリがあれば、そのまま使います。別の Run のものなど、それ以外のものがあれば拒否します。
- **前回のスナップショットとの関係**: 移った先のディレクトリの Run スナップショットは、そのストアでの最初のものとして全ファイルを pack します。

#### パッキング（Packing）

Kairo が保持するデータは、書き込みが完全に止まった後に、データが書かれたそのストア上でパッキングされます。パッキング対象となるタイミングは以下のとおりです。

- **Run の output ディレクトリ**: Run が終了し（Task が `pending` でも `running` でもなくなったか、その Task の再実行が始まった時点。直接実行では終端状態に達した時点）、かつ最後にそのディレクトリを受け取った Attempt が `quiesced`（静止）した時点。なお、チェックポイントで一時中断された Run には後続の継続があるため、まだ終了したとはみなされません。
- **失敗した Attempt の保持中 scratch**: その Attempt が `quiesced` した時点（ロストした Attempt は、プロセスがまだ書き込んでいる可能性があるため、リコンサイルが完了するまで待機します）。

パッキング対象はそれぞれ 1 つのデータオブジェクトとして管理されます（`data_objects` テーブルで `packing` から `packed` へ遷移。イベントは `pack_ordered`、`packed`）。パッキングは 1 度だけ実行され、現在のストア宣言が依然としてそのパスを該当ディレクトリとして指している間にのみ実施されます（ストア移動前に指示されたオブジェクトは、宣言が再びそのパスを指すようになるまで再配布されません）。実行はディレクトリを作成した Executor が担当し、1 分ごとに `packs-to-make` で対象を取得して完了後に `record-pack` で記録します。

パッキングでは、以下の 2 つのファイルが `<root>/packs/` 配下に生成されます（これは `example-app` 自身が durable store 上で使用する pack と同一の形式です）。

- **アーカイブ本体**: `<root>/packs/<object id>.tar.zst`（PAX 形式の tar を zstd で圧縮）
- **ファイル一覧**: `<root>/packs/<object id>.tar.zst.files.tsv`
  - 先頭行はタブ区切りのヘッダ `path`、`bytes`、`sha256`、`mtime_ns` とし、以降に通常ファイルごとに 1 行ずつタブ区切りで出力します。
  - パスは `/` 区切りの相対パスで、パス内のバックスラッシュ・タブ・CR・LF はそれぞれ `\\`、`\t`、`\r`、`\n` にエスケープされます。
  - 一覧は生のパス（raw path）順でソートされます。

アーカイブに含めるエントリの扱いと、走査・検証のルールは以下のとおりです。

- **エントリの扱い**: ディレクトリはモード（パーミッション）と mtime を保持してパックされます。リンク、特殊ファイル、マウントポイントはスキップされます（件数はカウントされますが、追跡・進入は行いません）。マーカーファイルはアーカイブから除外されます。Windows の予約デバイス名（`aux` など）は、リパースポイントをたどらずディレクトリハンドル経由で文字どおりの名前（literal name）のままパックされます。
- **安全な到達と整合性チェック**: 対象ディレクトリはストアのルールに従ってアクセスされます。Executor は送信されたパスをそのまま使わず、ストアのルート・オブジェクト種別・ID から対象ディレクトリを導出し、ハンドル経由で 1 ディレクトリずつ開いてマーカーが自身を指していることを確認します。各ファイルは一覧取得時のファイルと同一であり、読み取り中にサイズと mtime が変化せず、かつ宣言サイズを超えるデータを含んでいてはなりません。これに反する場合はそのラウンドが失敗となり再試行されます。また、UTF-8 ではないファイル名は一覧に記述できないため拒否されます。
- **書き込み・同期・リードバック検証**: 2 つのファイルはいずれも `.partial` の仮名で書き出され、ディスク同期のうえリネームされます（ファイル一覧を先にリネーム）。リネーム自体もディスク同期（Linux ではディレクトリ、Windows では各ファイル）された後、両ファイルともキャッシュをバイパスしてディスクから直接リードバックされます。書き込んだ内容と一致するリードバックが確認された pack だけがデーモンへ記録されるため、記録済みの pack は完全であることが保証され、途中で中断されたラウンドは最初からやり直されます。
- **待機条件とリトライ間隔**: この Executor が起動した Attempt がまだ実行中のソースディレクトリ、およびディレクトリサイズがストアの空き容量を上回るオブジェクトは待機します。失敗したオブジェクトは次回ラウンドで再試行され、その後は 1 分を起点に毎回 2 倍ずつ間隔を延ばし、最大 1 時間までバックオフして再試行されます（このバックオフ規則はコピー、削除、リードバックにも同様に適用されます）。

`kairo store status` では各ストア上で作成された pack が `PACKS`（`pack_bytes`、`packs`）として表示され、それらが移動されるまではそのストア上で Kairo が使用するバイト数として計上されます。

#### Durable Store へのコピーとレプリケーション

##### 初回コピーとレプリカ配置

pack 済みの各オブジェクトは、まず pack が存在するノード上の durable store のうち ID 順で最初のものへコピーされます。そのノード上に条件を満たす durable store がない場合は、他ノード上の ID 順で最初の durable store へコピーされます（この場合は pack 側ノードのエージェント経由で読み取ります）。

コピー先として選ばれる durable store は、以下の条件をすべて満たしている必要があります。

- 移動中ではないこと
- 消滅済みまたは無効化された Executor を指定していないこと
- オブジェクトを収める空き容量があること（最後に報告された空きバイト数から `reserve_free_bytes` と、その報告以降にそのストアで作成中または検証済みとなったコピーの pack サイズを差し引き、その報告以降にシェッドされた pack サイズを加えた値で判定。一度も報告されていないストアは空き容量ありとみなす）

初回コピーの完了後は、空きのある限り、他のすべての durable store へレプリカとしてコピーされます。これにより、1 つのストア（ディスクの削除や故障）あるいはホスト全体を失っても、Kairo が保持するデータは失われません。空きが足りず全部を写せないときは、守りの薄いものから写します。

- **レプリカの取得ルール**: オブジェクトに検証済み（`verified`）コピーが 1 つできると、いずれかの Executor が担当している他の各 durable store のうち、まだそのオブジェクトを持たないものが、空き容量のある限りコピーを取得します。順番は、検証済みコピーが載っているホスト（マシン宣言の `hosts`。ホストに属さないノードはそれ自体を 1 つと数えます）の数が少ないもの、次にコピーの数が少ないもの、次にサイズの小さいものの順で、1 回につき最大 50 件まで処理します。ホストが 1 つ壊れる・消されると、そのホストのディスク上のコピーはすべて失われるためです。
- **レプリカの読み取り元**: まずその Executor のノード上にある durable コピー（元の pack はすでに削除されている場合があります）から読み取り、なければ別ノード上の durable コピーから、そのストアを今担当しているエージェントの pack サーバ（その `pack_addr`）経由で読み取ります（コピーを作成したエージェントではありません。ストアが別ノードへ移った後や、作成したエージェントが退役した後も読めます）。そのコピー用に発行されたトークンは、そのコピーが存在するノードでのみ有効です（`object_copies.source_*`）。
- **優先順位と順次問い合わせ**: まだどの durable store にも存在しないオブジェクトは、常にどのレプリカよりも優先されます。また Executor はコピーを 1 件終えるたびに次のオーダーを再度問い合わせるため、優先度の高いコピーが長時間のコピーの後ろで待たされることはありません。
- **実行主体と記録**: 各コピーは対象ストアを担当する 1 つの Executor が引き受けます（毎分の `copies-to-make`、`record-copy`、テーブル `object_copies`、状態は `copying` から `verified`、イベント `copy_ordered`・`copy_verified`）。マシン宣言の変更によりそのルートで durable store ではなくなったストア、またはその Executor が担当しなくなったストア向けに引き受けたコピーは、次回ラウンドで放棄され、現在の宣言におけるストア向けに再度引き受けられます。

コピーは以下のファイル構成で書き込まれます（`<YYYYMM>` はオブジェクトの作成月です）。

- pack 本体: `<root>/objects/<YYYYMM>/<object id>.tar.zst`
- ファイル一覧: pack と同じディレクトリに配置
- 説明ファイル（description）: `<object id>.tar.zst.object.json`（`format` は `kairo object 1`）。オブジェクト自体、種別（kind）、ライフサイクル（lifecycle）、プロジェクト、名前、生成元の Run に属する Task、何をどこで pack したか、どのノード・どのストア上か、サイズと sha256 群、作成時刻および pack 時刻を記録します。

この説明ファイルにより、デーモンなしでもコピーの内容を読み取れます（`kairo store catalogue ROOT` は説明ファイルのみから durable store のコピー一覧を表示します）。説明ファイル導入以前に作成されたコピーには後から説明ファイルが付与されます（同じラウンドで 1 回につき最大 200 件の説明作成専用オーダーを配布し、`record-described` および `object_copies.described_at` で記録します）。

##### 空き容量不足時のシェッド（Shed）

durable store の実質的な空き容量（最後に報告された空きバイト数から、その報告以降にそこで作成中または検証済みとなったコピーのサイズを差し引き、その報告以降にそこでシェッドされたコピーのサイズを加えた値。報告には数分の遅延があるためこの補正を行います）が `reserve_free_bytes` を 1 GiB 超下回ると、コピーのシェッドが行われます。

- 対象ストアの Executor に対し、`copies-to-make` の `Shed` でコピー削除が指示されます。
- シェッド対象は、他の durable store 上に検証済みかつ破損していない（undamaged）コピーを少なくとも 2 つ保持しているオブジェクトのコピーに限られます。
- サイズの大きいものから順に、1 回につき最大 50 件まで、不足分を補うのに必要な件数のみ削除します。レプリカ作成やフェッチで読み取り中のコピーは決してシェッドされません。
- 各削除では、まずそのストアを担当する Executor がシェッドを claim します（`claim-shed`: レコードを削除する前に他コピーの存在とアクティブな読み取り処理を再度検証し、イベント `copy_shed` を記録）。その後、pack・ファイル一覧・説明ファイルを削除します。
- レプリカ作成時は `reserve_free_bytes` の上に少なくとも 1 GiB の空きを残します。また、過去 24 時間以内に同じオブジェクトをシェッドしたストアにはレプリカを戻しません。

##### Durable Store の定期インベントリと採用・破損検知

レコードの記述にかかわらず、durable store のディレクトリ上の実体がそのストアの保持内容とみなされます。

- **定期一覧化**: 毎時、自身のファイルシステム上で durable store を担当する各 Executor がディレクトリを一覧化し（`<root>/objects/<YYYYMM>/<object id>.tar.zst` とその隣のファイル一覧。`.partial` ファイルは除外。ルートが存在しない場合は一覧化しないため、アンマウントされたディスクを空のストアと誤認しません）、結果を報告します（`inventory-durable`）。
- **未記録コピーの採用（Adopt）**: 古いコントロールスナップショットからデータベースを復元し、そのスナップショット以降に作られたコピーがレコードに欠けている場合など、レコード側に存在しないコピーは説明ファイルから採用されます（`adopt-copies`、1 回につき 200 件、イベント `copy_adopted`）。オブジェクトは説明通りに `packed`、ソースは `kept`（`"adopted from durable store ..."`）、コピーは `verified` として記録されます。Kairo のものではない説明ファイル、temporal オブジェクトの説明ファイル、あるいはすでに記録されているオブジェクトと食い違うコピーの説明ファイルは拒否されます（理由はエージェントログに出力されます）。
- **欠損コピーの破損マークと安全策**: 一覧化の開始前からそこに存在すると記録されていた `verified` コピーが一覧に見当たらない場合、リードバック（read-back）失敗時と同様に破損（damaged）としてマークされます（`check_problem`、イベント `copy_damaged`）。ただし、一覧が空である場合や、そこに記録されている `verified` コピーの大部分が欠けている一覧は信用せず、何も変更しません。

##### pack の転送とリードバック検証

コピー元の pack がその Executor の担当するストア（同一ノード、同一名前空間）上にある場合は、ハンドル経由でローカルから直接読み取ります。それ以外の場合は、pack が存在するノードのエージェントから、その Executor が通知するアドレス（エージェント設定の `pack_addr`）へ HTTPS（デーモンの CA を信頼）で接続し、デーモンがそのコピー用に発行したトークンを使って取得します（`GET /packs/<object id>.tar.zst` および `.files.tsv`、`Authorization: Bearer`）。

- **配信側の認可検証**: pack を配信するエージェントはリクエストごとにデーモンへ問い合わせます（`pack-read`）。その pack を作成した Executor のノードからの問い合わせであり、かつそのコピーの最新で有効期限内（12 時間、1 ラウンドの最大時間）のトークンである場合にのみ取得を許可します。それ以外は `403` または `404` を返し、情報を一切漏らしません。
- **再開とタイムアウト制御**: 途中で途切れた取得は Range リクエストで再開されます。すでに pack と同じ長さに達している `.partial` pack は再取得しません。レスポンスボディが pack より長い場合は失敗となり、5 分間まったくデータを受信できない取得は打ち切られます（クライアント側は接続確立・ハンドシェイク・レスポンスヘッダの受信にも時間上限を設け、サーバ側はアイドル接続を閉じるとともに、拒否したリクエストをオブジェクトごとに 1 分に 1 回警告します）。
- **キャッシュ迂回リードバックとリネーム**: pack とファイル一覧はいずれも `.partial` として書き込んで sync した後、キャッシュを迂回してディスクからリードバックします。
  - **Windows**: `FILE_FLAG_NO_BUFFERING` を使用し、パスから開いたファイルが開いたディレクトリ内の同一ファイルであることを確認します。
  - **Linux**: まずファイルのキャッシュ済みページを破棄します（ベストエフォートであり、ZFS の ARC のように独自のキャッシュを持つファイルシステムではメモリから応答される場合があります）。
- **検証の確定**: pack のサイズ・sha256 およびファイル一覧の sha256 が一致した場合にのみリネームし（ファイル一覧を先にリネーム）、リネームを sync します。最終ファイル名の下にあるファイルが先ほどリードバックした同一ファイルであることを確認できたら（exFAT のようにリネームでファイル ID が変わる環境では、最終ファイル名の下で再度リードバックして一致を確認します）、コピーを `verified` として記録し、デーモン側でも再度検証します。
- **不一致時の扱い**: 検証に一致しなかったコピーはファイルを削除し、後で再取得します。削除対象は `.partial` ファイル、あるいはリネーム済みだが未記録のコピーであれば最終ファイル名のファイルです（オーダーは未記録のコピーに対してのみ発行されます）。コピー処理において、Kairo 自身が書き込んだものの未記録であるこれらのファイルだけが、Kairo が durable store 上で削除する唯一のものです。なお、リネーム済みで未記録のファイルが検証に一致した場合は、そのままの状態で記録されます。

#### コピー済みソースの削除

オブジェクトのコピーが `verified` になると、そのオブジェクトを pack した Executor が、元のソース（Run の output ディレクトリまたは保持されていた scratch）と、それから作成した pack およびファイル一覧を削除します（毎分の `deletions-to-make`、`settle-deletion`、イベント `source_deleted`・`source_kept`）。削除は後述の scratch 削除ルール（ルート・種別・ID からディレクトリを導出し、マーカーで対象を識別し、リンク経由では一切到達しない）に従い、マシン宣言が引き続きそのディレクトリを Kairo の管理下としている間のみ実行されます。

削除に着手する前に、ソースは pack のファイル一覧と照合されます。

- ソース内のすべての通常ファイルが、pack 時と同じサイズ・mtime（ハンドル経由で stat して取得）で一覧に載っていなければならず、それ以外にはディレクトリしか存在してはなりません。
- pack 後に書き込みや追加がされたファイル、リンク、特殊ファイル、マウントポイント（いずれも pack には含まれません）が 1 つでもあれば、理由付きでソースを保持（keep）します。ただしその場合も pack は削除されます（コピーが内容を保持しているためです）。
- Windows では、ソース直下に予約済みデバイス名があり、マーカーを先に削除せずにディレクトリハンドル経由で削除できない場合もソースを保持します。
- 一覧に載っているがすでに存在しないファイルは問題にしません（途中で中断された削除がすでに消したものであるためです）。
- この Executor が起動した Attempt がまだ実行中のソースは待機します。
- マーカーが現在読み取れない場合（アクセス拒否、使用中、I/O エラー）や、オープン中にディレクトリが差し替えられた場合は、保持（keep）する理由とはせず、再試行対象の失敗として扱います。

削除時は、まず Executor が削除を claim します（状態 `deleting`）。claim が許可されるのは、リードバックで破損が見つかっていない `verified` コピーが存在し、そのコピー先ストアが宣言上依然としてそのルートで durable store であり、かつ宣言上そのソースが Kairo の管理下にある間のみです。その後、削除が `deleted` として確定するか、安全上の理由で拒否されて `kept`（理由付き、以後再試行しない）として確定するまで、`kairo machine apply` はそのディレクトリを Kairo の管理外とする宣言を拒否します。削除された output ディレクトリや scratch は `deleted` とマークされ、以後カウントされません。

#### Temporal データ

再生成できるデータは、再生成できないデータを保管する durable store の容量を消費する必要がありません。データオブジェクトのライフサイクル（`lifecycle` カラム）には、通常の `durable`（デフォルト: pack され、durable store へコピーされる）と `temporal` の 2 種類があります。

以下のいずれかに該当するオブジェクトが `temporal` になり、生成元の Task が `producer_task_scope_id` として記録されます。

- `regenerable` と宣言された Task（[アドミッション制御とプランニング](#アドミッション制御とプランニング)を参照）の Run による output
- `--regenerable-by TASK`（そのプロジェクトで `regenerable` と宣言された Task。`--delete-source` とは併用不可）を指定してインポートされたディレクトリ

`temporal` オブジェクトは pack も durable store へのコピーも行われません。

- 担当 Executor はそのアイデンティティ（identity）、すなわち pack した場合に得られるはずのファイル一覧のみを作成します（`packs-to-make` の `identity`: 全ファイルを読み取りますが、書き出すのは `<root>/packs/<object id>.tar.zst.files.tsv` のみです）。
- オブジェクトはファイル一覧そのものと共に記録され、ストアがそれを保持します（`manifest`、イベント `identified`）。
- ディレクトリは元の volatile store 上にそのまま留まり、それを入力として指定する Execution からその場（in place）で読み取られます。
- コピーされないためコピー後の削除は行われませんが、ストアの空き容量が不足した際にエビクト（evict）されます（後述のエビクションを参照）。
- `kairo data list` では各オブジェクトの `LIFECYCLE` を確認できます。

#### Temporal データのノード間転送

あるノード上で待機している Execution が必要とする temporal データが別ノード上にまだ残っている（ディレクトリが存在する）場合、再生成するのではなく転送して持ってきます。

1. データを保持する Executor が移動用に一時 pack を作成します（`transfer_state` は `packing`、pack オーダーの `transfer`、イベント `transfer_ordered`）。作成した pack のファイル一覧はそのオブジェクトのアイデンティティと一致しなければならず、一致しない場合は pack を削除して記録しません（識別後にディレクトリが変更されたため転送しません）。
2. pack が完了する（`packed`、pack のパス・サイズ・sha256 を記録、イベント `transfer_packed`）と、durable オブジェクトと同様に（後述の入力転送を参照）、pack したノードのエージェントから転送されます。ただし durable store にはコピーされません。
3. そのデータを持たないノード上で待機している Execution がなくなり、かつ転送中の処理もなくなると、一時 pack は削除されます（`dropping`、オーダーの `drop`、アイデンティティのファイル一覧は残る、イベント `transfer_dropped`）。この一連の処理中、元のディレクトリはエビクトされません。

待機中の Execution には、転送の進行状況が `being packed on N to be brought to M` や `being brought to M from N` のように表示されます。

#### Temporal データの再生成

あるノード上で待機している Execution が必要とする temporal データの入力が、どのノードにも存在しない（存在したすべての場所からエビクトされた）場合、そのデータを生成する Task が一度成功していれば、そのノード上でその Task により再生成されます。

- **再生成の発注**: 対象の Task が再実行され（再実行リクエスト `regenerate:<object id>`、アクター `kairo`、イベント `regeneration_ordered`）、その Execution は待機中 Execution のノード上へ計画されます。オブジェクトはその再実行の ordinal を保持します（`regeneration_ordinal`）。発注はオブジェクトごとに 1 回のみです。
- **実行可能なノードの制約**: そのノード上の有効な Executor のいずれも、対象 Task の最新 Execution の Executor ラベルを持たない場合（例: 特定マシン上にしかないファイルから構築されるデータセットを別ノードで読む場合）は、再生成は発注されず、読み取り側はその旨を表示して待機します（temporal データはノード間で転送されないため、そのようなデータは転送対象となる durable な output にしてください）。
- **Task 状態による扱い**: 対象 Task が `pending` または `running` の場合は完了を待ちます。`failed`、`stopped`、`cancelled`、または `blocked` の場合はプロジェクト側で再実行する必要があり（待機理由にその旨が表示されます）、自動リトライは行われません。
- **再生成結果の照合**: 対象 Task の Run が残した各 output は、識別（identified）されると、同じ Task のオブジェクトのうちエビクト済みまたは再生成発注済みのものとファイル一覧同士で照合されます（比較対象は各ファイルのパス・サイズ・sha256 であり、mtime は比較しません）。
  - **一致した場合**: その output が元のオブジェクトの再生成物として紐付けられます（`regenerated_as`、元のオブジェクトの名前を引き継ぐ、イベント `regenerated`）。以後、元のオブジェクトまたはその名前を指定するすべての入力は、代わりにこの再生成物を読み取ります（待機中はエビクションからも保護されます）。
  - **異なっていた場合**: 再生成のために発注された Run の output が元のオブジェクトと異なっていた場合、そのオブジェクトに問題が記録されます（`regeneration_problem`、イベント `regeneration_differs`）。読み取り側はどのように異なるかを表示しながら、後の手動再実行で同じファイル群が生成されるか、あるいは Task の最新 Run の output を直接読むよう入力を変更するまで待機します（`task = "T"` 形式の入力は中身にかかわらずその Task の最新 Run を読み取ります）。

`kairo data list` では、再生成されたオブジェクト（`made again as obj_...`）および記録された問題を確認できます。

#### データの手放し（drop）

durable store が保管するのは、作り直せないもの（学習の結果、チェックポイント、評価結果、Run スナップショット、退避したアーカイブなど）です。作り直せるもの・取り直せるものは durable に置きません。

- **Corpus/Mix**（raw データからプロジェクトのツールで作った生成物）は、raw がある限り作り直せるので durable にしません。データを作る Task は `regenerable` と宣言し、その output を temporal にします。
- **raw データ**（外から取ってきたもの）は、Kairo は中身を持たず、ファイル一覧（各ファイルの sha256）だけを持ちます。要るときはプロジェクトが取り直します。

すでに durable になっているものは `kairo data drop` で手放します。

- **CLI**: `kairo data drop --project P [--match NAME_PREFIX] [--task-match TASK_PREFIX] [--reason TEXT] [--confirm] [OBJECT|NAME...]`。ID か名前、名前の先頭（`--match`）、またはその名前で始まる Task の output（`--task-match`）で選びます。`--confirm` がなければ何も変えず、手放すもの・手放さないもの（とその理由）を表示するだけです。HTTP API は `POST /api/data/drops`（ボディ: `{"project", "objects", "name_prefix", "task_prefix", "reason", "confirm"}`）です。
- **残るもの**: オブジェクトはファイル一覧（pack のファイル一覧: 各ファイルのパス・サイズ・sha256・mtime）だけを持つ temporal データになります（`lifecycle` は `temporal`、`dropped_at`、イベント `data_dropped`）。
- **消えるもの**: durable store 上のすべてのコピーと、ノードへ取り寄せた（fetch した）すべての場所です。どちらも空き容量に関係なく消します。
  - ファイル一覧は durable store のコピーの横（`<copy>.files.tsv`）にしかないため、コピーを持つストアの Executor が先に送り（`copies-to-make` の `send_list`、`keep-identity`）、daemon は記録済みの sha256 と一致したときだけ保持します（イベント `identity_kept`）。保持するまで、どのコピーも消しません（`claim-shed` が拒否）。
  - コピーはシェッドと同じ手順で消します（`copies-to-make` の `shed` と `drop`、イベント `copy_shed`）。レプリカ作成やフェッチが読んでいるコピーは消しません。
  - 取り寄せた場所はエビクションと同じ手順で消します。入力として指定されている間は残します。
- **手放さないもの**: pack 済みの durable なインポートと Run の output だけを手放します。終わっていない Execution、または pending・running の Task が入力として指定しているもの、ノードへ取り寄せている途中のもの、ファイル一覧を取れる正常なコピーがないものは手放しません。
- **手放した後の読み手**: それを入力に指定した Execution は、プロジェクトが作り直す（または取り直す）までハード問題として待ちます（`dropped data ...: make it again or fetch it again, and import it`）。

#### 既存データのインポート

ストア宣言前やストア外に書き込まれたディレクトリなど、Kairo が作成したのではないデータを Kairo の管理下に取り込むには `kairo data import` を使用します。

- **CLI**: `kairo data import --project P --node N [--name NAME] [--delete-source | --regenerable-by TASK] PATH...`
- **HTTP API**: `POST /api/data/imports`
  - リクエスト: `{"project", "node", "paths", "name", "delete_source", "regenerable_by"}`
  - レスポンス: `201 {"objects": [...]}`（各オブジェクトの `id`、`name`、`path`、`store`、`executor` を含む）

各 `PATH` は指定プロジェクトの種別 `import` のデータオブジェクトになり（`source_id` はオブジェクト自身、イベント `import_ordered`）、名前は `--name NAME` 指定時が `NAME`（複数パス指定時は `NAME/<ディレクトリ名>`）、省略時が `<ディレクトリ名>` となります。

##### ストア選定・pack・パス制約

対象ノード上の Executor が、自身の担当する volatile store のうち対象ディレクトリのファイルシステム上のもの（宣言された `filesystem` がそのパスを含むもののうち最長一致: `F:/models` なら `F:`、`/srv/models/qwen` なら `/srv/models`）、該当がなければ ID 順で最初のストアへ pack します。output ディレクトリと同様に、ボリュームのルートからハンドル経由で 1 ディレクトリずつ開き（パス途中にリンクがないこと。内部のリンク・特殊ファイル・マウントポイントはスキップされカウントされます）、ストアにその内容を収める空き容量ができてから pack し、その後は他のオブジェクトと同様に durable store へコピーしてリードバック検証を行います。

指定する `PATH` は以下の条件を満たしている必要があります。

- 絶対パスであること
- 対象ストアの名前空間に属すること（Windows ストアではドライブパス、Linux ストアでは POSIX パス）
- ルート指定と同様に、1 つの場所を表す正規の単一表記であること
- ファイルシステムのルートそのものではないこと
- そのノード上のどのストアの内部にも含まれず、どのストアも包含しないこと
  - **例外**: durable store の `<root>/intake` の真下（strictly inside）にあるパスはインポートでき、`--delete-source` 指定時はコピー完了後に削除できます（保持したいデータを他から削除されない durable store 内に置き、Kairo がオブジェクトとして取り込むまで保護するための仕組みです）。
- そのノード上でインポート処理中（まだ pack されていない、またはコピー済みだが削除・保持が確定していない）のディレクトリの内部・同一パス・上位ではないこと

後からそのパスを覆うストアが宣言された場合、pack および削除は停止されます。また、削除中のディレクトリが「すべてのストアの外」ではなくなるようなマシン宣言は、削除中の scratch の場合と同様に拒否されます。

> [!NOTE]
> Git Bash から実行すると、CLI が受け取る前に POSIX パスが Git のディレクトリ配下に書き換えられます（例: `/srv/x` が `C:/Program Files/Git/srv/x` になる）。CLI はそのようなパスを検出して拒否するため、`MSYS_NO_PATHCONV=1` を付けて実行してください。

##### ソースの保持と `--delete-source`

インポート元のディレクトリは Kairo の所有物ではないため、読み取りのみを行います。

- **`--delete-source` なしの場合**: 元のディレクトリはそのまま残ります。コピー完了後は pack のみが削除され、ソースは `kept`（`"imported without delete_source"`）として確定します。確定する前（インポートのソースがまだ `present` の間）に、同じプロジェクトが同じディレクトリを `--delete-source` 付きで再度インポートすると、既存のインポートが削除ありへ切り替わり、再 pack は行われません（イベント `import_delete_source`）。すでに確定済みであれば改めて pack されます。
- **`--delete-source` ありの場合**: コピーが `verified` になった後、Executor が削除を claim し、通常のソースと同様にディレクトリを検証します（すべての通常ファイルが pack と同じサイズ・mtime で一覧に載っており、他にはディレクトリしか存在しないこと）。さらに、pack 先のストアと同じファイルシステム上にあることを検証します（Windows では同じドライブであり、削除対象として渡すドライブパスはそのストアのドライブでなければなりません。それ以外の環境では同一デバイスであり、マウントや別ディスクではなく、決して durable store ではないこと）。そのうえで親ディレクトリのハンドル経由で対象を削除し（対象ディレクトリ自体は通常のディレクトリであること。配下のリンクはリンク自体を削除し、リンク先は決して辿りません）、続けて pack を削除し、通常の削除後と同様にノードを trim します。
- pack 後に変更されていたディレクトリは理由付きで保持（kept）され、pack は通常通り削除されます。なお、検証から削除までの間に書き込まれたファイルは削除と共に失われるため、もう書き込みが行われないディレクトリをインポートしてください。

##### 単一ファイル・書き込み継続中ディレクトリ・放棄とキャンセル

- **通常ファイルのインポート**: `PATH` には通常ファイル単体も指定できます（公開済みチェックポイント: [実行中ジョブのチェックポイント保護](#実行中ジョブのチェックポイント保護)を参照）。親ディレクトリ経由で開き、自身のファイル名で単独 pack され、ファイルとして記録されます（`source_file`、pack 内にも記録）。ファイルオブジェクトは入力としてその場（in place）で読み取られることはありません（[アドミッション制御とプランニング](#アドミッション制御とプランニング)および入力仕様を参照）。pack にファイル種別が記載される以前に pack されたオブジェクトについては、ソースがファイルであることを検出した Executor が記録します。検出は、ノード上の in-place 読み取り対象パスを検査する際（`in-place-sources`、エージェント操作 `input-is-file`、イベント `input_is_file`）、またはそのオブジェクトを読み取ろうとした起動時に行われ、起動時の検出では再実行を促して `launch_aborted` で終了します。
- **書き込み継続中ディレクトリの扱い**: インポート対象ディレクトリに書き込みが続いている場合（コレクタの出力先など）、過去 1 時間以内に更新されたファイルは pack の隣に作成したコピーから pack されます。読み取り中に元ファイルへの書き込みが起きなくなるまで（1 秒間隔で最大 5 回）コピーをやり直し、それでも書き込みが続く場合は最後に読み取った内容をそのまま採用します（追記型ファイルであれば先頭のプレフィックスとなり、エージェントログで `changing` としてカウントされます）。一覧取得から読み取りの間に消えたファイルは除外されます（skipped としてカウント）。1 時間より古いファイルが読み取り中に更新された場合は、output ディレクトリの場合と同様にその pack を失敗させ、次のラウンドでは直近更新ファイルとして扱います。変更中に pack されたファイルは pack のファイル一覧と一致しなくなるため、`--delete-source` 指定時でもディレクトリは保持されます。
- **消失時の放棄とキャンセル**: ボリュームは存在するのにパスが消失しているインポート（マウントされていないディスクは削除済みソースとはみなしません）は `abandoned` となり（イベント `import_abandoned`）、以後は pack されず、ソースは理由付きで `kept` として確定するため、そのパスを再度インポートできます。`kairo data cancel OBJECT...`（`POST /api/data/objects/{id}/cancel`）も同様に、まだ pack されていないインポートを放棄します。キャンセル時点で作成中だった pack は、完了すればそのまま記録されてコピーされます。

#### データオブジェクト一覧の確認（`kairo data list`）

`kairo data list [--project P] [--kind output|scratch|import] [--task NAME] [--execution ID] [--limit N] [--json]`（`GET /api/data/objects`、レスポンス `{"objects": [...]}`、新しい順、デフォルト 200 件）でデータオブジェクトを確認できます。

- **表示内容**: 種別（kind）、プロジェクト、名前、ノード、状態（`packing`、`packed`、`abandoned`）、ファイル数、コンテンツおよび pack のバイト数、検証済みコピー数（およびそのうち 1 つのパス）、ソースの状態（`present`、`deleting`、`deleted`、`kept`、`evicting`、`evicted` とその理由）。
- **`--task`（クエリパラメータ `task`）**: 指定した Task 名の全 Run の output を選択します。特定プロジェクトに絞るには `--project` を併用します。
- **`--execution`（クエリパラメータ `execution`）**: 指定した Execution が属する Run の output を選択します。継続（continuation）の Execution であってもその Run 全体が対象になります（以前の Gang の leader を指定した場合は、各 rank の output も含まれます）。
- これらのフィルタは `--json` 指定時にも適用されます。

#### Run スナップショット

Kairo が起動した Run によって書き出されたデータは、Run の終了後だけでなく、書き込まれた瞬間から Kairo の保全対象となります（`[data] confine_from` を設定すると、各 Attempt は Kairo から割り当てられた場所以外には書き込めなくなります。コンファインメントの詳細は [アドミッション制御とプランニング](#アドミッション制御とプランニング) を参照してください）。

Run の実行が終了していない間、その output ディレクトリは繰り返し pack されます。

- **差分スナップショットの生成**: 各回では、その Run の前回スナップショット以降に変更されたファイルのみを pack します（初回は全ファイル）。生成されるデータオブジェクトの種別は `run-snapshot`、ソースは `<run id>~<n>`（`data_objects.snapshot_since`）です。
- **pack と複製**: 担当 Executor が pack を行い（インポート時と同様に、直近 1 時間以内に書き込まれたファイルはコピーを作成してから pack します）、他のデータオブジェクトと同じく durable store へコピー・複製されます。
- **発行タイミングと並列制御**: Run の最新 Attempt が実行中である間、`[data] run_snapshot_interval`（デーモン設定、Go duration 形式、デフォルト `"30m"`、`"0"` で無効）ごとに 1 回、また前回スナップショット後にその Attempt がチェックポイントを発行（`checkpoint` コマンドまたは suspend に対して `checkpointed` を応答）した直後に指示されます。1 つの Run につき同時に実行されるのは 1 件のみです（イベント `pack_ordered`、`reason` は `periodic` または `checkpoint`）。
- **ディレクトリの維持と完了時の処理**: output ディレクトリ自体は Run の所有物のまま維持され、スナップショット処理によって削除されることはありません。durable store へのコピー完了後は pack ファイルのみが削除され、ソースは `kept` として精算されます。Run が終了した時点で、Run 全体の output オブジェクトが丸ごと pack されます。
- **ストア内チェックポイントの扱い**: ストア内部（Run の output ディレクトリ）のパスとして発行されたチェックポイントは、インポートではなくこの Run スナップショットによって保全されます。
- **復元方法**: 特定のスナップショット時点の output を復元するには、その Run のスナップショットを先頭から順に展開します（途中で削除されたファイルも、それ以前のスナップショットに残っています）。

#### 実行中ジョブのチェックポイント保護

Run の output ディレクトリは Run の終了後に保全されますが、実行中の Run の作業内容はチェックポイントを通じて保全されます。

##### 外部パスチェックポイントの自動インポート

Attempt がチェックポイントを発行（suspend または `checkpoint` コマンドに対し、`continuation_ref` を添えて `checkpointed` を応答）した際、その `continuation_ref` が以下の条件を満たす場合、デーモンはそのパスを Attempt のプロジェクトへ自動インポートします。

- ノード上の絶対パスであること（例: `D:\ckpt\run\step_200.pt`、`/srv/ckpt/run`。URI や UNC パスは対象外）
- すべてのストアの外部に存在するファイルまたはディレクトリであること

インポート時の挙動は次のとおりです。

- インポート名は `checkpoints/<task>/<パスの末尾要素>` となります（名前が 200 文字を超える場合は `<task>` を省略します）。
- 通常のインポートと同様に pack され、durable store へコピーされます。元のパスは読み取りのみで、元の場所にそのまま残されます。
- 同一パスのインポートは 1 度のみ行われます（同じパスで繰り返し応答されても再インポートされません）。ただし、前回のインポートが `abandoned` になったパスは新たにインポートされます。
- インポートできないチェックポイント（ストア内にあるパス、他のインポートと重複するパスなど）はログに記録されてそのまま残されます。インポートの成否にかかわらず、`checkpointed` の応答自体は有効なまま維持されます。

##### 定期的なチェックポイント要求

長時間実行されるジョブが保全可能なチェックポイントを持つよう、デーモンは毎分チェックポイントの取得を要求します。

- **対象条件**: 実行中でチェックポイント対応（checkpointable）な Attempt のうち、開始から `[data] checkpoint_interval`（デーモン設定、Go duration 形式、デフォルト `"2h"`、`"0"` で無効）以上経過しており、その期間中に `checkpoint` コマンドを受けておらず、処理中（in flight）のコマンド（suspend を含む）も存在しないもの。
- **プロジェクト単位の限定**: 過去にパス形式のチェックポイントを発行した実績があるプロジェクト（いずれかのコマンドがパスを伴って `checkpointed` 応答されたプロジェクト）の Attempt にのみ要求します。チェックポイント時に何を行うかはワーカー固有であり、たとえばモデルを配信しているワーカーはサーバを停止して URI を返す場合があり、それでは何も保全されないためです。
- **要求の発行と応答**: 対象となった Attempt には、`kairo task checkpoint` で発行するものと同様の `checkpoint` コマンドが送信されます（origin は `data_protection`、イベント `checkpoint_requested`、actor は `kairo`）。Attempt は次の安全なタイミングでこれに応答し、そのまま実行を継続します。`checkpoint` コマンドを受け取らない Attempt はコマンドを保留（pending）にしたままとなり、その間は追加の要求は送られません。

##### インプレース入力の消失検知

インプレースで読み取る入力のパスが Attempt の起動時に存在しない場合（ボリュームは存在するのにパスが存在しない、すなわち Kairo の外部から削除された場合）、その起動は中止され、Executor が消失を報告します（エージェント操作 `input-gone`、イベント `input_gone`）。

- インポート元ソース: `deleted`（理由: `"gone from its path"`）として精算
- Run の output ディレクトリ: `missing` に更新
- 取得済みコピー（brought copy）: 配置レコードを削除（forgotten）

レコード上その場所は入力元として提示されなくなるため、再実行時は後述の仕組みによって durable コピーからオブジェクトが取得されるのを待つことになります。

また各 Executor は、インベントリ巡回（起動時および 1 時間ごと）のたびに、レコード上でそのノードのインプレース読み取り候補となっている全パス（`in-place-sources`）を検証し、消失しているものを同様に報告します。これにより、途中で削除されたパスの大半は、起動時に必要となる前に検知されます。

##### 入力データの取得（Brought inputs）

Execution が待機しているノード上に存在しない入力データは、オブジェクトが `packed` になり次第、そのノードへ取得（fetch）されます。取得は以下のいずれかで指示されます。

- **自動指示**: デーモンが Task をリコンサイルする際、ノード上で待機中の Execution に不足している各入力に対して自動で指示します。
- **手動指示**: `kairo data fetch --project P --node N [--data NAME]... [OBJECT...] [--wait [--timeout D]]`、または `POST /api/data/fetches`（ボディ: `{"project", "node", "data", "objects"}`）で指示します。

CLI オプションの挙動は次のとおりです。

- `--wait` なしの場合、取得を指示すると即座に終了します。
- `--wait` 指定時は、2 秒ごとにそのノード上の配置を確認し（`GET /api/data/objects/{id}/placement?node=N`）、展開が完了すると `OBJECT: PATH` を出力します。取得を完了できない場合（配置が指示されなかった、pack が消失した等）は非ゼロで終了します。回復可能なエラーは Executor がリトライします。
- `--timeout` は待機時間の上限を指定します（タイムアウト時は終了コード 2。`0` または省略時は無制限）。タイムアウトしても取得自体はキャンセルされません。

配置先の選定と転送・検証の流れは次のとおりです（テーブル `object_placements`: `fetching` → 完了後 `ready` とバイト数を記録。イベント `fetch_ordered`、`fetched`、`fetch_moved`。エージェント操作 `fetches-to-make`［毎分］、`record-fetch`）。

1. **配置先 volatile store の選定**: ノード上の Executor が担当する volatile store のうち、pack と展開後ツリーを収める空き容量が最も多いストアが選ばれ、その担当 Executor が取得を行います。空き容量は「最終報告時の空きバイト数 − `reserve_free_bytes`（最低 1 GiB） − そのストアで保留中の fetch の pack サイズおよび展開後ツリーサイズの合計」で算出され、同率の場合は ID 順で先頭のストアとなります。ノード上の volatile store 群は 1 つのプールとして扱われ、ID による優先順位はありません。
2. **容量不足時の移動**: 指示から 1 時間経過しても `ready` にならず、現在のストアに収まる空き容量がない場合、収まる空き容量を持つ別の volatile store のうち最も空きが大きいストアへ移動します（イベント `fetch_moved`）。収まらない入力を待つ Execution が永久に待機し続けるのを防ぐためです。
3. **取得元の選択**: オブジェクトの pack は、`verified` な durable コピー（破損が見つかったコピーは対象外）、またはソースが未精算の間はその pack が作成された元の場所から読み出されます。この Executor 自身が担当するストア上にある場合は、他ノードのエージェントより優先してハンドル経由で直接読み取ります。他ノードから取得する場合は、そのノードのエージェントの `[packs]` サーバから取得します。この際、デーモンがその fetch 専用に発行したトークン（有効期限 12 時間、そのコピーまたは pack を保持するノードのエージェントのみが受理可能）を使用します。durable コピーは `<root>/objects/<month>` から配信されます。
4. **ダウンロードと整合性検証**: pack とファイルリストを `<root>/inputs/<object id>.tar.zst.partial` に取得します（途中で切断された場合は Range リクエストで再開）。完了後、pack のサイズと SHA-256、およびファイルリストの SHA-256 を検証します。
5. **展開と検証**: `<root>/inputs/<object id>.partial` へ展開します。展開されるのは内部の相対パスにおけるディレクトリと通常ファイルのみであり、展開先ディレクトリへのハンドル経由で書き込むため外部へのエスケープは発生しません。各ファイルのサイズと SHA-256 をファイルリストと照合し、リストに記載されたすべてのファイルが存在することを確認したうえで、書き込んだファイルの mtime を pack 時の値に設定します。Windows に限り、ストアのハンドル経由で作成できない予約デバイス名を含むパスは検証のみ行って作成をスキップし、そのパスをログに記録します。
6. **確定とキャッシュ**: `<root>/inputs/<object id>` にリネームし、pack とファイルリストを削除して `ready` と記録します。Executor は事前に pack の残り部分と展開後ツリー分の空き容量を確認しますが、すでに展開済みのツリーであれば追加の空き容量なしに記録できます。失敗した fetch は 1 回警告を出し、後で再試行されます。

取得されたオブジェクトはノード上に保持され、同じオブジェクトを入力に指定する後続の全 Execution からインプレースで読み取られます。エビクトされるまでの間、そのストアの `CACHE` バイト数として計上されます。

##### エビクション（Evicting）

Executor が最後に報告した volatile store の空き容量が下限（宣言された `reserve_free_bytes`、未宣言の場合は総容量の 1/10）を下回ると、下限を上回るまでデータがエビクト（evict）されます（エージェント操作 `evictions-to-make`［毎分］、`settle-eviction`。イベント `eviction_ordered`、`evicted`、`eviction_kept`）。不足分の解消計算には、すでに claim 済みのエビクションと、その最終報告以降にエビクトされたバイト数の両方が算入されます。

- **対象と優先順位**: エビクトされるのは再取得または再生成が可能なデータのみです。まず取得済みオブジェクト（brought object: durable コピーから再取得可能）、次に stream 用に置いた raw ファイル（`<root>/raw/<sha256>`。上流から再取得可能。[raw キャッシュ](#raw-キャッシュ)）、次にその Executor の temporal データ（元の Task で再生成可能）の順に対象となります。raw ファイルは、読んでいる Loader があっても消せます（開いているファイルは、名前が消えても読み続けられます）。それぞれの区分内では LRU（最も長く使われていないものから）順で選ばれます。最終使用時刻は「作成または取得された時刻」と「最後に Attempt へ入力として渡された時刻（`data_objects.last_used_at`）」のうち遅い方です。
- **使用中データの保護**: 待機中（pending）または実行中（running）の Task、あるいは未終了の Execution が入力として指定しているオブジェクトは、決してエビクトされません。
- **claim による排他制御**: 対象はまず claim されます（配置を `evicting`、またはソースを `evicting` に変更）。claim 以降はその場所からのインプレース読み取りや Attempt への受け渡しは行われず、その対象が Kairo の管理対象外となるようなマシン宣言変更も拒否されます。
- **Executor による削除手順**:
  - 取得済みオブジェクト: `inputs` ディレクトリのハンドル経由で `<root>/inputs/<object id>` を削除します。
  - temporal な Run output ディレクトリ: その Executor が起動した Attempt がそのディレクトリ内でまだ実行中でない限り、コピー済みソースの削除と同様のストアルールに従って削除します。
  - temporal なインポート済みディレクトリ: そのストアのファイルシステム上で、親ディレクトリのハンドル経由で削除します。
  - temporal ディレクトリ共通の安全条件: identity のファイルリストと内容が完全に一致している間（リスト記載の各通常ファイルが識別時のサイズおよび mtime のままであり、他にはディレクトリしか存在しない状態）のみディレクトリを削除し、続けて identity のファイルリストを削除します。
- **精算とエラー時の扱い**:
  - 削除完了時は `evicted` として精算され（配置は削除されて再取得可能になり、temporal な Run output ディレクトリは `deleted` と記録されます）、通常の削除後と同様にノードのトリムが要求されます。
  - 安全チェックで拒否された場合（識別後に変更された、使用中である、Kairo の管理対象外になった等）は、理由付きで保持（`kept`）として精算され再び利用可能な状態（ソースは `present`、配置は `ready`）に戻り、その後 1 時間は再度エビクション対象に選ばれません。
  - それ以外のエラーで失敗した場合は claim 状態のまま維持され、再試行されます。

エビクトされた temporal オブジェクトは、その生成 Task が再実行されるまで失われた状態になります（Temporal データの詳細は [データストア](#データストア) を参照してください）。インポートした temporal ディレクトリも Kairo 自身が生成したものと同様にエビクト（削除）されるため、`--regenerable-by` には該当 Task が確実に再生成できるデータのみを指定してください。

##### Durable コピーのリードバック検査（Scrub）

durable store はデータを永続保管しますが、無言でビット破損が発生する可能性があります（exFAT にはジャーナルがありません）。そのため、durable store を担当する Executor は、キャッシュをバイパスして各 `verified` コピーの全量をリードバック（read-back）検査します（エージェント操作 `scrubs-to-make`［10 分ごと］、`record-scrub`）。

- **実行条件と帯域上限**: 前回のリードバックまたは検証から `[data] scrub_interval`（デーモン設定、Go duration 形式、デフォルト `168h`）以上経過したコピーが古い順に検査されます。上限は 1 ストアあたり 1 日最大 `scrub_bytes_per_day` バイト（デフォルト 200 GiB）です。この上限より大きい単一コピーは、1 日 1 回それ単体で読み出されます。
- **破損検知時の挙動**: ファイルが存在しない、全量を読み取れない、または pack のサイズや SHA-256 と一致しないコピーは破損（damaged）とマークされ（イベント `copy_damaged`）、`kairo store status` の `"damaged copies"` に一覧表示されます。破損コピーに対して自動削除や上書きコピーが行われることはありません。
- **オフラインストアの扱い**: `objects` ディレクトリ自体が存在しないストア（ドライブが取り外されている状態など）は、破損扱いとせず後回しにされます。

##### コンテナのトリム（Trimming containers）

Proxmox コンテナ内でファイルを削除しても、トリム（trim）を実行するまでホスト側のシン・プール（thin pool）へ領域は返還されません。

- ノード上で削除が発生するとトリムが要求されます（テーブル `node_trims` に前回トリム以降の削除バイト数を記録）。
- `containers` にそのノードを列挙しているホストのコントローラ（controller）がトリムを実行します（操作 `trims-to-make`［5 分ごと］、実行コマンド `pct fstrim <vmid>`、タイムアウト 1 時間、報告 `record-trim`、イベント `node_trimmed` / `node_trim_failed`）。デバイス設定の維持を妨げないよう、バックグラウンドで 1 コンテナずつ実行されます。
- 実行頻度は 1 コンテナあたり最大 1 時間に 1 回です。前回の指示または失敗から 1 時間未満では新たに指示されません。1 回のトリムはその指示より前の削除分を対象とし、指示後に発生した削除は再度トリムを要求します。失敗したトリムは 1 時間後に再指示されます。
- どのホストの `containers` にも列挙されていないノードはトリムされません。

##### scratch ディレクトリの精算と安全な削除

Attempt が静止（`quiesced`）または消失（lost）すると、担当 Executor がその scratch を精算します（エージェント操作 `scratch-to-settle`［10 秒ごと。各 scratch に `action` として `delete`・`keep`・`leave` を返す］、`settle-scratch`）。

**精算の判定ルール**

- **削除（`delete`）**: 成功時（シグナルなしの終了コード 0、またはチェックポイントを発行して終了した suspend）のみ削除されます。さらに、マシン宣言上その記録パスが引き続き該当 volatile store の scratch ディレクトリであることも条件となります。
- **保持（`keep`）**: 失敗時（0 以外の終了コード、シグナル終了、実行に至らなかった起動）は、空であってもなくても保持されます。消失（lost）した Attempt（リコンサイル後も含む）も、プロセスがまだ書き込みを続けている可能性があるためそのまま保持されます。保持された scratch は、pack・durable store へのコピー・削除が完了するまで（[データストア](#データストア) を参照）、`kairo store status` に Task・サイズ・理由とともに一覧表示されます。消失した Attempt の scratch はリコンサイル完了を待ちます。
- **手つかずで放置（`leave`）**: ストアが削除された、移動した（ルートパスの表記が変わった場合を含む）、durable store に変更された、あるいはノード上の別ストアがそのパスを管轄するようになった場合、scratch は手つかずのまま残されます（`leave`: 保持扱いとなり、サイズ計測すら行われません）。

**削除前の claim と設定変更のブロック**

削除に先立ち、Executor は scratch を claim します（`settle-scratch` で状態 `deleting` を取得。削除対象である間のみ受理されます）。claim されてから `deleted`（claim 済みの場合のみ受理）として精算されるか、安全チェックで拒否され `kept` として精算されるまでの間、`kairo machine apply` はその scratch が削除対象外になるような宣言変更を拒否します（HTTP 409 `machine_conflict`、対象の scratch と Executor 名を明示）。これにより、削除処理の途中で宣言が書き換わることを防ぎます。

claim 済みの scratch はエージェント再起動後も削除対象として維持されます。該当 Executor が二度と復帰しない場合、手動でレコードを精算するまでコンフリクト状態が残ります。

なお、`observe_only`（観測専用モード）のデーモンはエージェントにストアを返さず（`stores` は空リストを返す）、精算すべき scratch も返さないため、エージェントはルートディレクトリも scratch も作成せず、削除も一切行いません。

**Executor による安全な削除の実装ルール**

削除は Executor 自身が行い、与えられたパスに対して `os.RemoveAll` や `rm -rf` を直接呼び出すことは決してありません。

- **パス・リンク・マウントの厳格検証**: 対象パスは絶対パスのルートに基づく記録済みの `<root>/scratch/<attempt id>` と厳密に一致しなければなりません（それ以外はすべて拒否）。ルートパスの途中にリンクが含まれていてはならず、`<root>/scratch` および scratch ディレクトリ自体もリンクであってはなりません。`<root>/scratch` はマウントポイントであってはならず、scratch ディレクトリ自身およびその配下にもマウントポイントが存在してはなりません（Linux では `/proc/self/mountinfo` から確認し、bind マウントも含みます）。また、マーカーファイルに該当 Attempt が記録されている必要があります。
- **ハンドルベースの走査とエントリ単位の削除**: パス文字列を介した直接操作は一切行いません。ボリュームのルートから 1 ディレクトリずつ順に開き、ルート配下のすべての操作は `os.Root` ハンドル経由で行います（Linux では `openat` 相当、Windows ではハンドル相対操作）。開いた各ディレクトリは、事前に検証したディレクトリと同一ファイルであることをハンドル経由で確認します。削除には `os.Root.RemoveAll` を用い、シンボリックリンク・ジャンクション・その他のリパースポイントは参照先ではなくリンク自体を削除します。エントリごとに削除し、マーカーファイルを最後に消すことで、途中で中断された削除も再開できます。
- **属性無変更とファイルシステム種別の確認**: ファイルは属性を変更せずに削除されます（外部ファイルへのハードリンクはリンクが解除されるだけで、元のファイルには手を付けません）。Windows でこれを実現するには、Go が読み取り専用属性を無視して削除できる NTFS（他のファイルシステムでは先に読み取り専用属性を解除するため、ハードリンク先の共有ファイルに影響してしまう）か、ハードリンクもリパースポイントも存在せず属性解除が削除対象ファイルのみにとどまる exFAT である必要があります。これがファイルシステムチェックを行う理由です。
- **書き込み不可ディレクトリの復旧（Linux）**: Task によって書き込み不可にされたディレクトリ（scratch ディレクトリ自身またはその配下のディレクトリ）に限り、Linux 上で `O_PATH|O_NOFOLLOW|O_DIRECTORY` ハンドル経由で再度書き込み可能にします。このハンドルはそのディレクトリ以外を指せないため、ファイルにすり替えられていてもファイルのモードは変更されません。
- **マーカーファイルの安全性**: マーカーファイルは新規作成したディレクトリ内に排他的に作成され（既存ファイルを開くことはありません）、読み取りもノンブロッキングで行います（マーカー位置に FIFO が置かれていてもエージェントはハングしません）。そのため、途中で差し替えられた対象へ誤って書き込むことはありません。
- **拒否と再試行**: 以上の安全上の理由で拒否された削除は、その理由を記録して scratch を `kept` とします。それ以外のエラー（ファイルが使用中など）で失敗した場合は再試行されます。

なお、ストア機能より古いデーモンに対して新しいエージェントが接続した場合、古いデーモンが scratch を受け取ることはありません（`stores` に 404 を返すため、エージェントは scratch の作成も精算の問い合わせも行いません）。

#### データライフサイクル

ここまで述べた各レコードは、以下の状態遷移表（`internal/store/lifecycle.go`）に定義された遷移のみを行います。状態変更を行う箇所は必ずこのステートマシンを事前にチェックするため、表に存在しない遷移はエラーメッセージとともに失敗します。表中の `-` はレコードが存在しない状態を表し、`-` からの遷移はレコードの作成、`-` への遷移はレコードの削除を意味します。

| レコード | 遷移前（`from`） | 遷移後（`to`） |
|---|---|---|
| object（`data_objects.state`） | `-` | `packing`（pack 指示時）、`packed`（durable store からの adopt 時） |
| | `packing` | `packed`、`abandoned` |
| | `abandoned` | `packed`（インポートがキャンセルされた時点で作成中だった pack が完了した場合） |
| source（`source_state`） | `-` | `present`、`kept`（adopt 時） |
| | `present` | `deleting`（claim 時）、`kept`、`evicting`（claim 時）、`deleted`（元のパスから消失しているのを発見した場合） |
| | `deleting` | `deleted`、`kept` |
| | `evicting` | `evicted`、`present`（保持された場合） |
| | `kept` | `deleted`（保持していたインポート元が消失しているのを発見した場合） |
| | `deleted` | `kept`（消失発見後にインポートが abandon された場合） |
| transfer（`transfer_state`） | `-` | `packing` |
| | `packing` | `packed` |
| | `packed` | `dropping` |
| | `dropping` | `-` |
| copy（`object_copies`） | `-` | `copying`（コピー指示時）、`verified`（adopt 時） |
| | `copying` | `verified`、`-`（断念: 対象ストアがその場所でもはや durable ではなくなった場合） |
| | `verified` | `-`（シェッド［shed］時） |
| placement（`object_placements`） | `-` | `fetching` |
| | `fetching` | `ready` |
| | `ready` | `evicting`（claim 時）、`-`（パスから消失しているのを発見した場合） |
| | `evicting` | `ready`（保持された場合）、`-`（エビクト完了時） |
| scratch（`attempt_scratch`） | `-` | `active` |
| | `active`、`missing` | `deleting`（claim 時）、`kept` |
| | `active`、`kept` | `missing`（全体スキャンで発見できなかった場合） |
| | `missing` | `active`、`kept`（再発見された場合） |
| | `deleting` | `deleted`、`kept` |
| | `kept` | `deleted`（durable store へのコピー完了後） |
| Run output（`run_outputs`） | `-` | `active` |
| | `active` | `missing`、`deleted`、`moved`（ほかのストアへ持ち込まれた場合。ディレクトリは元の agent が消します） |
| | `missing` | `active`（再発見または再作成された場合）、`deleted` |
| | `moved` | `active`（移した先からまた持ち込まれた場合） |

`packed`、`evicted`、`deleted` は終端状態です。

### ハードウェア設定

ホスト上の各デバイス（UUID で指定する GPU、または `cpu`）には、以下のハードウェア設定を指定できます。

- **GPU**: 最大グラフィックスクロック（`graphics_clock_max_mhz`）、固定メモリクロック（`memory_clock_mhz`）、電力上限（`power_limit_w`）
- **CPU**: 最大周波数（`max_freq_mhz`、Windows および Linux の `cpufreq`）、または最大パフォーマンス率（`max_perf_pct`、Linux の `intel_pstate`）

省略された項目はハードウェアのデフォルト値となり、設定自体が存在しない場合はすべてデフォルトとなります。Kairo は望ましい設定状態（desired state）を保持し、ホスト上のコントローラがそれを適用して現在の適用状況を報告します。ハードウェア設定がアドミッションや実行中のジョブを妨げることはなく、設定に失敗した場合も単なる事実として記録されるだけです。

- **所有権（Owners）**: 各設定には `manual`（オペレータによる手動設定）、`bench`（ベンチマーク実行による設定）、`policy`（動作点ポリシーによる設定）のいずれかの所有種別があり、デバイスごと・所有種別ごとに 1 つ保持されます。実際に有効となる優先順位は `manual` > `bench` > `policy` であり、ベンチマーク実行は自身が Lease を保持するデバイス上でポリシー設定を上書きします。
  - `bench` 設定および GPU の `policy` 設定は Lease に紐づきます。設定するには対象ホスト上のノードに `reserved`、`prepared`、または `active` 状態の Lease が存在している必要があり（GPU の場合はその GPU を保持する Lease であること）、同じ所有種別で別 Lease の設定がすでに存在する場合は `409`（`device_setting_conflict`）で拒否されます。Lease がどの経路で解放された場合でも、紐づく設定は同一トランザクション内で削除されるため、設定が Lease より長く残ることはありません。
  - CPU はホスト上で稼働するすべてのジョブで共有されるため、CPU の `policy` 設定は Lease ではなくホストに紐づきます（`ref` は `host`）。この設定は、マシン宣言によってその CPU が制御対象外になったとき、ホスト自体が削除されたとき、または `max_slowdown` が `0` に設定されたときに削除されます。また、`max_slowdown` が `0` のデバイスに対する `policy` 設定は拒否されます。
- **ケイパビリティ（Capabilities）**: コントローラは、各デバイスで変更可能な項目と設定可能範囲を報告します（GPU は `nvidia-smi -q -d SUPPORTED_CLOCKS` や電力上限の範囲・デフォルト値、CPU は周波数範囲または `intel_pstate` の `min_perf_pct`）。デバイスがサポートしていない設定、範囲外の値、またはコントローラがまだ報告していないデバイスへの設定は `400` で拒否されます。設定可能な範囲の根拠となるのはこのケイパビリティ情報だけであり、ベンチマークの探索グリッドやポリシーのフォールバック値もここから算出されます。なお、Provider が `device_properties` に読み取る電力上限や最大クロックは、あくまで観測された値としてデバイスのサンプルとともに表示されるだけで、他の用途には使われません。
- **世代（Generation）**: デバイスの有効な設定が変化するたびに `generation` がインクリメントされます。コントローラは適用した `generation` を報告するため、`kairo hardware show` で `in sync`、`pending`、`failed`（エラー内容付き）を判別できます。
- **イベント**: `device_setting_requested`、`device_setting_cleared`（オペレータ操作、Lease 解放、マシン宣言の変更といった理由付き）、`device_setting_applied`、`device_setting_failed`（異なるエラー内容ごとに 1 回）が記録されます。

| コマンド | 説明 |
|---|---|
| `kairo hardware show [HOST] [--json]` | ホストごとにノード一覧とコントローラ（最終確認時刻）を表示し、続いてデバイスごとにポリシー、有効な設定とその所有者、適用済みの状態、ステータス（`in sync`、`pending`、`failed`、コントローラが自律的にデフォルトへ戻した `reset`、`not reported`、`observe only`、`not controlled`）を表示する。さらにエラー、上書きされている設定、各デバイスがサポートする設定、ポリシーの最新判断、最新のベンチマーク結果を表示する |
| `kairo hardware set HOST DEVICE [--graphics-clock-max MHz] [--memory-clock MHz] [--power-limit W] [--cpu-max-freq MHz \| --cpu-max-perf PCT]` | デバイスの `manual` 設定を一括で設定する（省略した項目はデフォルト値になる） |
| `kairo hardware reset HOST DEVICE` | `manual` 設定を解除する（`bench` または `policy` 設定があればそれに従い、なければデフォルトへ戻る） |
| `kairo hardware bench HOST DEVICE [--kinds K,...] [--node NODE] [--dry-run]` | グリッド上の各設定点でデバイスを計測する（[ベンチマーク実行](#ベンチマーク実行)を参照） |
| `kairo hardware profile HOST [DEVICE] [--json]` | 計測されたカーブ、各デバイスの `max_slowdown` に対応する動作点、前回ベンチマークからのドリフトを表示する |

HTTP API では、`POST /api/hardware/{host}/{device}/settings` が `{"settings", "owner": {"kind", "ref"}, "actor"}` を受け付けます（`owner` を省略した場合は `manual` となり、`ref` には `actor` が入ります。`bench` や `policy` の `ref` は Lease ID、CPU の `policy` の `ref` は `host` です）。`DELETE /api/hardware/{host}/{device}/settings` は `settings` を除いた同じリクエストを受け付けます。いずれのエンドポイントも `{"host_id", "device_id", "generation", "effective", "cleared"}` を返します。また、`GET /api/hardware` は `kairo hardware show --json` と同じ形式で `{"hosts"}` を返します。インプロセスコンポーネントからは、ストアの `SetDeviceSetting`、`ClearDeviceSetting`、`DevicePolicies` を使用します。

#### デバイスコントローラ

`kairo controller --config kairo-controller.toml`（設定例は `examples/kairo-controller.toml` を参照）は物理ホスト上で稼働します。エージェントと同様に `server_url`、`token`、`ca_file`、`insecure_http` を指定し、さらに自身の `[node]` セクションを持ちます。セットアップ手順は以下のとおりです。

1. ノードトークンを発行します（例: `kairo token create --role node --node host-a-host`）。
2. コントローラを一度起動して、そのノードをデーモンに認識させます。
3. そのノード名をホスト宣言の `controller` に指定します。

コントローラ用のノードは Executor を持たず、ジョブがスケジュールされることはありません。すでに存在するノード（エージェントも併用しているノードなど）を指定した場合は、既存ノードの設定はそのまま維持されます。

コントローラは `poll_interval_seconds`（デフォルト `10`）秒ごとにポーリングを行い、次の処理を実行します。

1. 自身が制御するデバイス（自身を `controller` に指定しているホストに属し、`control = true` になっているすべてのデバイスのみ）の設定を取得します。
2. 現在の状態と異なる設定を変更します。
3. 適用済みの設定、失敗した内容、および各デバイスがサポートする設定を報告します（この報告がハートビートを兼ねます）。
4. 前回のポーリング以降におけるホストの CPU パッケージ消費電力を報告します（[電力と消費電力量](#電力と消費電力量)を参照）。デーモンに受け入れられなかった計測値は破棄されます。

各デバイス種別の制御方法は以下のとおりです。

- **GPU**: UUID 指定で `nvidia-smi -i` を使用します。グラフィックスクロック上限は `-lgc LOWEST,MAX` / リセット `-rgc`、メモリクロック固定は `-lmc M,M` / リセット `-rmc`、電力上限は `-pl W` / リセット `-pl` に `power.default_limit` を指定します。カード側がリセット操作を提供していない場合（終了コード `3`）は、すでにデフォルト状態であるとみなします。
- **Linux CPU**: ドライバが `intel_pstate` の `max_perf_pct` を提供している場合はそれを使用します（デフォルトは `100`）。それ以外の場合は、全 CPU コアの `cpufreq` の `scaling_max_freq` を設定します（デフォルトは各 CPU の `cpuinfo_max_freq`）。
- **Windows CPU**: アクティブな電源プランの `PROCFREQMAX`（および Windows が備えている場合は `PROCFREQMAX1`）の AC 電源時の値を変更し、電源プランを再アクティブ化します。一度変更するとユーザーが元々設定していた値をリードバックできないため、コントローラはある電源プランを初めて変更する前に元の値をステートファイル（`state_file`、デフォルトは設定ファイルと同じディレクトリに置かれる `<config name>.state.json`）へ記録し（自身が設定した値で上書きすることはありません）、リセット時にその値を書き戻します。ステートファイルを読み込めない場合、コントローラは停止するため、手動で修復または削除してください。

コントローラは安全側（ハードウェアのデフォルト値へ戻す方向）に倒れるよう設計されています。

- **起動時**: すべての制御対象デバイスを望ましい設定に合わせるか、設定がなければデフォルトへリセットします（過去のクラッシュで設定が残っている可能性があるためです）。
- **終了時（`SIGTERM` または `Ctrl-C`）**: 変更した可能性のあるすべてのデバイスをリセットしてから、結果を報告します。
- **デーモン無応答時**: デーモンからの応答が `reset_after_seconds`（デフォルト `120`）秒間途絶えると、すべてのデバイスをリセットし、デーモンが復帰した時点で再度望ましい設定を適用します。ステートファイルには制御対象デバイスの一覧も保存されているため、デーモン停止中にコントローラが再起動された場合でもリセットが実行されます。
- **変更失敗時**: エラーを報告したうえで 5 秒後にリトライし、以降リトライ間隔を最大 5 分まで倍増させます（新しい設定要求が来た場合は即座に試行します）。失敗によってメインループが停止することはありません。
- **制御対象からの離脱時**: デバイスが制御対象から外れた場合（ホストからの削除、`control = false` への変更、別コントローラへの切り替え）、自身が変更した可能性があれば即座にリセットし（リセット失敗時は最大 5 回までリトライ）、以降は一切触れません。

ドライバの再読み込みや別コントローラによるリセットで設定が元に戻ってしまうケースに備え、適用済みの設定は 10 分ごとに再適用されます。また、デーモンが `observe_only` モードのときは、コントローラには制御対象デバイスが存在しないものとして通知されます。このときコントローラは新たな設定変更を一切行わず、自身が変更した可能性のあるデバイス（ステートファイルに記載されているデバイス）のみをデフォルトへ戻します。

GPU クロック、電力上限、プロセッサ設定の変更には、デーモン本体には付与されていない特権が必要です。OS ごとの常駐構成は以下のとおりです。

- **Linux**（Proxmox ホストなどで `root` として実行。コンテナ内から GPU は見えても設定変更はできません）: 以下のような systemd ユニットを作成します。

  ```ini
  # /etc/systemd/system/kairo-controller.service
  [Unit]
  Description=Kairo device controller
  After=network-online.target nvidia-persistenced.service
  Wants=network-online.target

  [Service]
  ExecStart=/usr/local/bin/kairo controller --config /etc/kairo/kairo-controller.toml
  Restart=on-failure
  RestartSec=10
  KillSignal=SIGTERM
  TimeoutStopSec=150

  [Install]
  WantedBy=multi-user.target
  ```

  作成後、`systemctl enable --now kairo-controller` で有効化・起動します。NVIDIA の persistence デーモンは稼働させておいてください。これが動いていないと、GPU を使用しているプロセスが存在しない間にドライバがクロックや電力上限をリセットしてしまう場合があります。

- **Windows**（管理者権限で実行。デーモン本体は非昇格権限で動作します）: 起動時に最高特権レベルで実行されるタスクスケジューラのタスクを登録します。

  ```powershell
  $action = New-ScheduledTaskAction -Execute "D:\Kairo\kairo.exe" -Argument "controller --config D:\Kairo\kairo-controller.toml" -WorkingDirectory "D:\Kairo"
  $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
  $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
  Register-ScheduledTask -TaskName "kairo-controller" -Action $action -Trigger (New-ScheduledTaskTrigger -AtStartup) -Principal $principal -Settings $settings
  ```

  `Stop-ScheduledTask` や OS のシャットダウン時はグレースフル停止を経ずに終了します。この場合、GPU は次回起動時にドライバによってデフォルトへ戻り、プロセッサ設定は次回コントローラ起動時にステートファイルから復元されます。コンソール上での `Ctrl-C` はグレースフル停止となります。

なお、コントローラは起動時に管理者権限（Windows）または `root` 権限（Linux）で実行されていない場合、警告を出力します。その状態で設定変更を試みると失敗し、エラーとして報告されます。

#### 動作点ポリシー

デーモン（`observe_only` モードでないもの）は 1 分ごとに、コントローラが変更可能かつ `max_slowdown`（`s`）が `0` より大きいすべてのデバイスを評価し、性能低下を `s` 以内に抑えつつ消費電力量が最小となる設定をデバイスの `policy` 設定として要求します。この処理がアドミッションを遅延させたり何かを待機したりすることはありません。コントローラが設定を適用できなかった場合も単なる事実として扱われ、次の評価が通常どおり継続されます。

デバイスのプロファイルは、ベンチマーク種別（kind）ごとに、その種別の有効なリファレンス計測値を持つ直近の終了済みベンチマーク実行（`finished`、`partial`、または `invalid`）のカーブで構成されます。あるベンチマーク実行で一部の種別が無効（`invalid`）になっても、同一実行内の他の種別のカーブはそのまま利用されます。

- **GPU のポリシー設定とワークロード分類**: GPU の `policy` 設定は、その GPU を保持している Lease に紐づきます（独自のハードウェア設定を行うベンチマーク用の Lease や、Attempt がまだ開始していない Lease は対象外です）。ポリシーは、Attempt 開始以降かつ直近 5 分間の GPU 挙動（observation の `utilization.gpu` とサンプルの `utilization.memory`。それぞれ 5 点以上あり、変動幅が 15 ポイント以内であること）を、ベンチマークで各スイープ種別が示したフィンガープリント（リファレンス計測時の同じ使用率）と比較します。両方の使用率が 15 ポイント以内に収まるすべての種別を候補とし、すべての候補に適合する設定（最悪ケース基準）を選択します。
- **CPU のポリシー設定**: CPU の `policy` 設定はホストに紐づき、3 種類の CPU ベンチマーク種別すべてに適合する設定が選ばれます。そのホストの CPU に対するベンチマークが Lease を保持している間は、ベンチマークがデフォルト状態を計測できるよう `policy` 設定は削除されます。
- **動作点の選定（`internal/hwbench`）**: 単一の種別と許容低下率 `s` に対して、リファレンス比の性能が `1 - s - 0.01` 以上（計測精度は約 1%）である有効な点の中から、単位作業量あたりの消費電力量が最小のものを選びます。条件を満たす点がなければデフォルトとします。候補が複数種別ある場合は、すべての種別でその条件を満たす設定の中から、各種別のリファレンスに対する相対消費電力量の平均が最小のものを選択します。
- **フォールバック**: 有効なプロファイルがない場合、または確信ある分類ができない場合（サンプル数不足、使用率の変動が大きい、該当する種別がないなど）は、フォールバックとして GPU の最大グラフィックスクロックをサポート上限の `(1 - s)` 倍（サポートされているクロック値へ切り上げ）に制限します（CPU の場合は最大周波数または最大パフォーマンス率の `(1 - s)` 倍）。デフォルトクロックがサポート上限を超えることはなく、性能低下もクロック低下率を上回ることはないため、どのようなワークロードであっても性能低下は最大 `s` に収まります。なお、周波数範囲が報告されない CPU（Windows）はデフォルト設定のまま維持されます。
- **ヒステリシス**: 新しい選択結果は 3 回連続の評価で同じ結果になったときに初めて適用され、デバイスの設定変更は 5 分間に最大 1 回までに制限されます。新しい Lease はデフォルト設定から開始します。
- **判断結果の記録**: 変更のたびに `device_policy_decision` イベントが記録されます。ここには判断の入力（候補種別、観測されたフィンガープリント、参照したベンチマーク実行、種別ごとのデフォルト比での想定相対性能・消費電力、またはフォールバック理由）が含まれ、デバイスの最新判断として保持されます（`kairo hardware show --json` の `policy` フィールドに出力され、`kairo hardware show` では `policy graphics_clock_max_mhz=1800MHz (profile, for gemm perf 0.970, 250 W vs 300 W) for lease ...` のように表示されます）。
- **削減電力量の集計**: GPU 上でポリシー設定が適用されている（コントローラにより反映済みである）各評価サイクルごとに、その GPU を保持する Attempt に対して経過時間と、プロファイルに基づく同一作業量あたりの削減電力（全候補種別における「相対性能 × デフォルト消費電力 - 動作点消費電力」の最小値）が計上されます。フォールバック設定には推定に使えるプロファイルがないため、適用時間は未推定（unestimated）としてカウントされます。

### ノード隔離

`kairo node quarantine --reason ... <NODE_ID>`（または `--all`）を実行すると、`kairo node release <NODE_ID>`（または `--all`）で解除されるまで、対象ノード（または全ノード）上のすべてのジョブが停止されます。

- 対象ノードの Executor は新たなリソース予約を行わず、優先度に基づくプリエンプションも発生させません。
- 実行中のチェックポイント対応 Attempt には `suspend` Command（起点 `node_quarantine`）が送信され、後日チェックポイントから継続実行されます。
- それ以外の実行中 Attempt は、ランチャーおよび子孫プロセスを含めて Executor によって強制終了され（Windows は `taskkill /T /F`、Linux は Attempt のプロセスグループへの `SIGTERM` の後に `SIGKILL`）、Task は失敗扱いではなく最初から再実行されます（オーケストレーション理由 `quarantine_restart`）。

`kairo node quarantine-status` は、各隔離対象とそこで依然として生存している Attempt 数を表示します。この数がゼロになると、隔離は完全に完了したことになります。一連のステップはすべてコーディネーションイベントとして記録されます。

### ノードフェンス

マシンが失われた、コンテナが破棄されたなどの理由でエージェントが永久に停止した場合、そのエージェントが再登録することはないため、担当していた Attempt の Lease は `active` なまま残り、GPU などのリソースを占有し続けます。

`kairo node fence NODE_ID --confirm-agent-gone [--reason R]`（`POST /api/nodes/{id}/fence`、管理者権限）は、エージェントが再登録した際と同じようにこれらの Lease を清算しますが、既存プロセスの引き継ぎ（adopt）は一切行いません。具体的には、対象ノードの Executor に紐づくすべての Attempt をロスト扱いとし、各 Execution を終了状態（`executor_identity_lost`）にして、それらの Lease をすべて `stale` に遷移させます（Executor ごとの `stale` Lease 一覧を含む `node_fenced` イベントを記録します）。

対象ノードの Executor のいずれかが直近 1 分以内にポーリングしている間は、フェンス操作は `409`（`executor_live`）で拒否されます（稼働中のエージェントは再起動時に自身の Lease を清算するためです）。`stale` な Lease はリコンサイルされるまでリソースを保持し続けます。エージェントが OS 再起動後に復帰した場合は、前述のとおり（[エージェント再起動と引き継ぎ](#エージェント再起動と引き継ぎ)を参照）、起動時刻より前の Attempt に紐づく Lease をデーモンが解放できます。一方、単なるエージェント再起動ではフェンスによってすでにロスト扱いとなった Attempt を引き継ぐことはできません。残存しているプロセスは終了するか手動停止されるまで監視外で動き続けるため、`kairo resource reconcile RESOURCE_ID --confirm-process-absent` でプロセス不在を確認する必要があります。

リコンサイルではすべてのリソースについて最新の観測情報を待ちますが、もはや存在しないノードから観測情報が送られることはありません。コンテナを削除した、あるいは物理ホストを廃止したなど、ノード自体が永久に失われた場合は `kairo node retire NODE_ID --confirm-node-gone [--reason R]`（`POST /api/nodes/{id}/retire`、管理者権限）を使用します。この操作は次の処理を単一トランザクション内で実行します。

1. 上記と同様にノードをフェンスします（Executor が直近でポーリングしている場合は同様に拒否されます）。
2. 新たな観測情報を待たずに、対象 Executor のすべての `stale` な Lease を解放します（`stale_lease_reconciled` および `node_retired` イベントを記録します）。
3. ロスト扱いとなった Attempt を静止（quiesce）させます。
4. 対象ノードおよびその Executor を無効化します。

なお、そのノードに紐づくストアは、マシン宣言から削除されるまで宣言されたまま残ります。

### 強制停止

`kairo task pause --force|-f [--grace 30s] [--reason ...] PROJECT QUEUE TASK`（`queue pause --force` および `project pause --force` も同様）は、チェックポイント対応の有無にかかわらず、対象となった実行を一時停止ではなく終了させる pause 操作です。

- **開始済み Execution**: その Attempt に対して強制停止が発行され、担当 Executor（ノードエージェント）が Attempt のコンテナとランナーを停止します。まずグレースフル停止を試み（ランナーのプロセスグループへ停止を送ります。Linux は `SIGTERM`、Windows は `CTRL_BREAK` です。ランナーがランタイムでコンテナを止め、ランナーがいなければエージェントがランタイムで直接止めます）、グレース期間経過後も残っているものを強制終了します（ランタイムでコンテナを kill し、ランナーのプロセスグループを kill します。Windows ではジョブオブジェクトの終了と `taskkill /T /F` です）。Execution は終了原因 `force_stopped` で終了し、進行中の `suspend` は待機されません。
- **未開始 Execution**: 取り下げられ（`withdrawn_before_start`）、予約済みの Lease は通常の pause と同様に取消（revoke）されます。
- **Lease 解放と静止判定**: Lease は通常の静止証明（quiescence proof）を経てのみ解放され、Executor がプロセスツリーの完全消滅も確認した時点で操作全体が `quiesced` となります。終了途中のプロセスは、ファイル（GPU のデバイスファイルを含む）を閉じてゾンビ状態になるよりも先に自身のメモリを解放するため、その段階で `/proc/PID/environ` から環境変数を読み取れなくなります（空になるかアクセスが拒否されます）。このため、`KAIRO_ATTEMPT_ID` によって特定されたプロセスは `starttime` とともに記憶され、同じ `starttime` で存在し続けており、ゾンビではなく、かつ環境変数が空または読み取り不可である間も生存中としてカウントされます。
- **Task の最終状態**: Task は `stopped` 状態で終了し、継続実行や再起動は計画されません。また、後続 Task は失敗した Task の場合と同様にブロックされます。Task 結果には操作内容、実行者（actor）、および理由が記録されます。

強制停止指示は他の Executor 作業と同様にエージェント API（`force-stop-orders` / `force-stop-ack`）で配信され、`signalled` に続いて `terminated` の順に確認応答されます。30 秒以内にどの Executor にもピックアップされない場合、対象の pause 操作は次のいずれかの理由でブロックされます。

- `force_stop_not_picked_up`: Executor はポーリングしているにもかかわらず強制停止指示を取得していない状態（対象ノードのエージェントログを確認してください）。
- `executor_unreachable`: Executor との通信が確認できていない状態。

### Gang（廃止）

複数のノードにまたがって 1 つの Task を rank ごとに走らせる Gang（`[tasks.execution.gang]`）は、なくなりました（2026-11、すべての実行をコンテナ 1 つにそろえたため）。Gang を宣言する Task は `kairo project apply` で理由つきで拒否されます。過去の Gang の実行の記録はデータベースに残りますが、rank ごとの表示（`task show` の `gang`、`task logs --rank`）はありません。

## Loader（ストリーム）

データセットは、できるだけネットワークとメモリの中だけで扱います（issue #43）。Corpus/Mix は作り置きせず、学習しながら raw データから作って流します（[データの手放し](#データの手放しdrop)）。Loader はそのための仕組みで、raw データを取り寄せて sha256 で確かめ、決まった順番のレコード列にして、学習プロセス（の各 rank）にソケットで渡します。どこまで渡したか（位置）を指定すれば、そのちょうど次のレコードから同じ順番で続きます。

Task は宣言した stream を読みます（Windows の GPU は wslc のコンテナから使います）。まだないもの（#43）:

- raw キャッシュの LAN での共有（いまは各ノードが上流から 1 回ずつ取り寄せます）と、メモリでの保持
- data object（`object = ...`）の source、非公開の Hugging Face ファイルの取得
- 状態表示（`task show`）

### 宣言（`[raws]`・`[streams]`）

```toml
[raws.wiki]
urls = ["https://dumps.example.org/jawiki/20261101/jawiki-pages.xml.bz2"]   # 1 URL が 1 ファイル（名前は URL の最後の要素）

[raws.web]
hf = { repo = "owner/name", revision = "<40 桁のコミット>", files = ["data/*.jsonl.zst"], type = "dataset" }

[streams.mix]
sources = [ { raw = "wiki", weight = 3, format = "text.bz2" }, { raw = "web", format = "jsonl", field = "text" } ]
shuffle = { seed = 7, block = 65536, files = true }
stages  = [ { argv = ["bin/normalize"], workers = 8 } ]
epochs  = 1                 # 最初の source の周回数（既定 1、0 で終わりなし）
memory_bytes = 4294967296   # Loader が持つ分。Task の RAM に足される

[tasks.execution]
image = "py"
streams = ["mix"]           # → KAIRO_STREAM_MIX
```

- **Hugging Face**: `revision` はコミット（40 桁の 16 進）に限ります。同じファイルの組を、いつまでも同じに指すためです。`kairo project apply` が、そのコミットのファイル一覧（パスが `files` のどれかに一致するもの。各 LFS ファイルのサイズと sha256 も）を取り、宣言に添えて送ります（環境変数 `HF_TOKEN` があれば使います）。
- **Task への固定**: Task には、読む stream の宣言と、その raw のファイル一覧（解決済み）が入り、Task の digest の一部になります。stream を変えるなら、Task も新しい名前で宣言し直します。
- **検証**: 宣言にない raw・stream、コミットでない revision、未知の形式、`field` を取らない形式（text・lenbin・file）の `field`、Parquet・Arrow の外側の圧縮、image のない Task は拒否されます。

### raw キャッシュ

学習側に渡すのは、照合が済んだバイトだけです。そのために、raw ファイルはノードの volatile store を一度経由します。識別情報とレコード数は、他の Run も頼りにするため、すべて agent が自分で計算したものだけを記録します（学習コードが送るものは記録しません）。

- **置き場所**: Attempt の volatile store に、取り寄せたファイル（`<root>/raw/<sha256>`）と、取り寄せ中のもの（`<root>/raw/.partial`）を置きます。どちらも、どの Attempt にも見せません。
- **Attempt ごとの窓口**: stream を読む Attempt には、起動時に専用の窓口 `<root>/raw/views/<attempt id>/` を作り、そのコンテナに読み取り専用で `/kairo/raw` としてマウントします。中身は、その Attempt 専用の agent のソケット（`.sock`）と、渡したファイルのハードリンクだけです。そのソケットへの依頼はその Attempt のものとして扱い、起動時（デーモンから来た Launch）にその Attempt の stream が読むと宣言されたファイルしか受け付けません。窓口の一覧は `<root>/raw/.attempts/<attempt id>.json` に残し（書き換えは丸ごと置き換えるので、途中で落ちても前の一覧が残ります。渡したファイルの識別情報もここに書き足します）、agent が再起動しても、走り続けている Attempt の窓口を開き直します（開けなかった窓口は掃除のたびに開き直します）。Attempt がそのノードで 10 分見えなくなったら、窓口を消します。
- **手順**: Loader はファイルを読む前に、URL を示して agent に頼みます（`POST /ensure`）。その Attempt にすでに渡したファイル（窓口にリンクがあるもの）は、そのまま渡し直します（上流が落ちていても、置き場所から追い出された後でも）。そうでなければ、agent はその URL についてデーモンが持つ識別情報（最初の 1 つ）を先に聞き（エージェント操作 `raw-identity`）、それと同じファイルを置いてあれば使います。識別情報がまだなく、公開済みの sha256 と同じファイルを（別の URL のために）置いてあれば、読み通して照合してから使います。どれもなければ上流から `.partial` へ 1 回だけ取り寄せながら、書いたバイトそのものからファイル全体とチャンクごとの sha256 を計算し、デーモンの識別情報（あれば）と、公開済みの sha256・サイズと照合します。初めての取り寄せが途中で切れたら、続きからではなく最初から取り直します（続きが別の版のファイルである恐れがあるためです）。上流が応答のヘッダーを 2 分待っても返さないとき、本文を 2 分間何も送らないときは、切れたものとして扱います。上流から 3 分続けて 1 バイトも進まなければ、その取り寄せはいったん終わり、Loader が頼み直すときに、そのあいだに（別の URL のために）置かれた同じファイルがないかを先に見ます。置けずに上流から読んでいるファイルも、3 分進まなければ Loader が agent に頼み直し、置かれていればそちらから読みます。取り寄せたら、まず識別情報をデーモンに記録し（`raw-placed`。デーモンが応えない間は 1 時間まで記録し直します）、それから置き場所へ入れ（同じ大きさのファイルがすでにあればそれを使い、大きさが違えば置き換えます）、最後にファイルを置いたことを記録します。この順なので、途中で agent が落ちても、置いたファイルは URL から見つけ直せます。渡そうとしているファイルは、見に行った時点から、待っているすべての依頼がそれぞれの窓口へリンクし終えるまで追い出しません。すでにあるファイルと同じ大きさでも、sha256 が違えば置き換えます。置いたことの記録が追い出しの途中とぶつかっても、そのまま渡します（追い出しは、リンクされたファイルを残して終わります）。最後に、依頼した Attempt 自身の宣言（起動時の識別情報、または公開済みの値）とも照合してから、その窓口へハードリンクして渡します。Loader はそのファイルを、チャンクごとにもう一度照合しながら読みます。照合が合わない、公開済みの値と違う、デーモンが別の識別情報を記録している、のいずれかなら、何も残さずに stream を止めます（`raw data changed`）。宣言にないファイルの依頼は拒否され、stream は止まります。
- **レコード数**: 置いたファイルは、その Attempt の source が読む形式・フィールドごとに、agent が数えてデーモンに記録します（`raw-records`。最初の数が残り、違う数は `raw data changed`）。以後の起動は、この数でファイルを丸ごと飛ばして途中から始められます。
- **大きすぎるファイル**: store の空き（追い出しの下限と、取り寄せ中のほかのファイルの分を引いた量）より大きいファイルと、上流が大きさを示さないファイルは置きません。取り寄せの途中でディスクが一杯になったとき、示された大きさを超えたときも同じです。まだ識別されていなければ agent が識別のためだけに一度読み（記録はする）、Loader はそのファイルを上流から照合しながら読みます（初回は 2 回取り寄せることになり、コンテナから上流へ出られる必要があります）。
- **追い出し**: 置いたファイルは、store の空きが下限を下回ると、最後に使われたのが古いものから消されます（[エビクション](#エビクションevicting)）。ただし、いずれかの Attempt の窓口にリンクがあるファイルは消しません（名前を消しても領域が空かないため。Linux でリンク数を見ます）。窓口へのリンクと追い出しは agent の中で順番に行うので、渡す途中のファイルが消されることはありません。消えたファイルが再び要るときは、上流から取り寄せ直します。
- **同時の依頼**: 同じ URL への依頼が重なったら、取り寄せは 1 回にまとめ、結果は依頼した Attempt ごとに、それぞれの宣言と照合します。まとめた取り寄せが別の宣言の公開値に合わずに失敗したときは、自分の宣言でもう一度取り寄せます。デーモンは、その URL を読む宣言のどれか 1 つの公開値に合う識別情報を記録します（どの宣言にも合わなければ `raw data changed`）。1 つのプロジェクトの中で、同じ URL を違う公開値で宣言すると拒否されます。
- **Loader 単体**: `kairo loader serve --raw DIR` で、このキャッシュ（その Attempt の窓口）を使います。`--raw` なしでは上流（`--origin`）から直接読み、まだ識別されていないファイルは読みながら識別します（そのときは、末尾で照合するまで照合前のバイトが渡り、識別情報はどこにも記録されません）。

### 起動、識別、位置

- **起動**: Attempt のコンテナの中で、本体より先に stream ごとに `kairo loader serve` が起動します。spec と位置は `/kairo/run/streams/<名前>/`、ソケットは同じ場所の `socket` です。ソケットができてから本体が `KAIRO_STREAM_<NAME>=unix:...` 付きで起動し、本体が終わると Loader（とその stage）も終わります。stage は Task の image とソースで、Task の `cwd` から動きます。Loader のログは Attempt の stderr に出ます。
- **公開値**: Hugging Face の一覧が示す sha256 とサイズ（サイズ 0 も示された値として扱います）は、識別情報とは別にファイルの宣言に残ります。公開値と識別情報が食い違うファイルを含む stream は、始まった時点で `raw data changed` で止まります。そのファイルのレコード数を使って読み飛ばすこともしません。デーモンは、受け取った一覧のファイルがパターンに合うこと、名前順に 1 度ずつ並ぶことを確かめます。
- **識別**: ファイルの識別情報とレコード数は、raw キャッシュの agent が計算して記録します（上記。テーブル `raw_identities`・`raw_records`）。レコード数は、その Attempt の窓口のリンクを読み、識別情報と照合しながら数えたものだけを記録します。受け付けるのは、その Executor に Lease のある Execution の stream が読むファイルのものだけです。URL ごとに最初の識別情報が残り、公開済みの値と違うもの、すでにあるものと少しでも違うもの（サイズ、sha256、チャンク）は `raw data changed` です。レコード数は、区切り・実際の圧縮・フィールドごとに残します。以後の起動では、どのチャンクもこれと照合され、長さが違えば（短くても長くても）変わったものとして扱います。
- **位置と checkpoint**: SDK の `safe_point(..., streams=[train])`（torch の `DistributedAdapter.safe_point(..., streams=[train])` は全 rank の位置を集めます）は、checkpoint を公開するとき、各 stream について rank の数（`world`）、順番の版（`order`、いまは 1）、各 rank の位置を `checkpointed` の payload に `"streams": {NAME: {"world": W, "order": 1, "positions": [...]}}` として入れます。stream を読む Execution の checkpoint は、読むすべての stream について、`world` と `order` があり、順番の版が同じで、rank 0〜W-1 の位置がちょうど 1 つずつ（`rank`・`record`・`sub` をすべて書いたもの。負の値なし、上限内）なければ拒否されます（`safe_point` に stream を渡し忘れると、公開されません）。
- **続き**: continuation（suspend 後、ロスト後、`rerun --from-checkpoint`）は、続けるもとの checkpoint を、その command で覚えています（`input_checkpoint_command_id`）。Loader は、その command の受理された payload が言う位置から始め、rank の数は payload の `world` に固定されます（別の数で接続した rank は拒否されます）。同じ参照で公開された別の checkpoint の位置が使われることはありません。あるレコードの途中の位置（`sub`）は、そのレコードの出力がそれより少なければ誤りとして stream を止めます（stage が変わったということなので、調整はしません）。stream を読む Task の continuation は、その checkpoint を公開した Execution と同じコードスナップショットとイメージで動きます（stage も位置の一部だからです）。そのため、そうした Task に `rerun --from-checkpoint` と `--source` を同時に指定すると拒否されます。stream を読まない Task の continuation は、前の Execution と同じスナップショットとイメージで動きます（`rerun --source` で替えた Run は、その続きもそのスナップショットです）。
- **ストリームの終わりと失敗**: 終わり（`end`）と失敗（`error`）は、取り逃した rank が接続し直したときにも、もう一度渡されます。失敗した stream は、巻き戻しで作り直されることはありません。stream の終わりより先の位置（そこに立ったことはありえない）は、終わりとして扱わず、stream のエラーにします。Loader のプロセスが終わったら、コンテナの init が起動し直し（5 回まで。同じセッションで）、各 rank は自分の位置で接続し直します。
- **メモリ**: 各 rank のために先に作っておく分は、件数（4096 件）とバイト数（`memory_bytes` の 4 分の 1 を rank で分けた分、最低 1 MiB。宣言がなければ rank ごとに 64 MiB）の両方で抑えます。生データは rank の数だけ読みます（各 rank の作り手が順番全体を読むため。stage に通すのは自分の rank の分だけです）。

### 順番

stream（`loader.Spec`、JSON）は、source（raw データとその重み・形式・ファイル）、seed、`block`、`shuffle_files`、`epochs`、`stages` から成ります。順番は次の手順で決まり、同じ宣言からはいつでも同じ列になります（並べ替えの乱数は `internal/loader` 自身の splitmix64 と Fisher–Yates で、Go の版に左右されません）。

1. **ファイル**: source ごとに宣言順です。`shuffle_files` なら epoch ごとに (seed, source, epoch) で並べ替えます。
2. **レコード**: 形式の区切りで切り出します。圧縮は形式の接尾辞（`jsonl.zst`）か、ファイル名の拡張子（`.zst`・`.gz`・`.bz2`）で判断します。`field` は、ない行・null・文字列でない値をエラーにします。
   - `text`: 行（末尾の `\r` も除く）
   - `jsonl`: 行。`field` を指定するとその文字列フィールド
   - `lenbin`: 4 バイトのリトルエンディアンの長さとバイト列
   - `file`: ファイル全体で 1 件
   - `csv`・`tsv`: 見出し行の次からの各行。`field` を指定するとその列の値、なければ見出しの名前をキーにした JSON オブジェクト（値は文字列）
   - `parquet`: 各行（行グループの順）。`field` を指定すると、その最上位の文字列（またはバイナリ）列だけを読みます。なければ行を JSON オブジェクトにしたもの。レコード数はファイルのメタデータの行数です
   - `arrow`: Arrow IPC のファイル（`ARROW1` で始まるもの）またはストリーム（Hugging Face datasets の `save_to_disk` が書くもの）の各行。`field` は `parquet` と同じ
   - `webdataset`: tar の中で同じキー（名前の最初の `.` までのパス）を持つ連続したメンバーの組を 1 件とします。`field` を指定するとその拡張子（`jpg`・`json` など）のメンバーのバイト列だけ（ないサンプルはエラー）。なければサンプル全体を「4 バイトの個数、各メンバー（名前順）の 4 バイトの名前の長さ・名前・4 バイトのデータの長さ・データ」で 1 件にしたもの（メンバー名は `__key__` と各拡張子。SDK の `streams.sample(item)` が辞書に戻します）

   Parquet と Arrow は索引がファイルの末尾にあるため、ストリームではなく、照合済みのチャンク（4 MiB）単位で好きな位置から読みます（直近の 8 チャンクを持ちます）。そのため識別情報のあるファイルに限り（raw キャッシュを通せば必ずあります）、外側の圧縮も付けられません。
3. **混合**: source の列を重み（`weight`、合計 65536 まで）の滑らかな重み付きラウンドロビンで交互に並べます。乱数は使わず、重みの合計ごとに同じ並びを繰り返すため、どの位置でも各 source の何件目かが計算で分かります。source ごとに epoch を数え、尽きたら次の epoch に進みます。最初の source が `epochs` 回りきったところで stream は終わります（0 なら終わりません）。
4. **ブロックシャッフル**: `block` 件ずつ (seed, ブロック番号) で並べ替えます。メモリはブロック 1 つ分で済み、どの位置からでも正確に再開できます。
5. **rank への分割**: 並べた列の g 番目は rank `g mod W` の `g / W` 番目のレコードです。
6. **stage**: プロジェクトのプロセスです。stdin で長さ付きのレコードを受け取り、レコードごとに 0 件以上の出力（長さ付き）と終わりの印 `ff ff ff ff` を stdout に書きます。`workers` 本を rank で分けて並列に動かし（rank ごとに `workers` ÷ rank 数、最低 1 本）、出力は入力の順に戻します。同じ入力には同じ出力を返すことが条件で、落ちた stage は起動し直して、答えていなかったレコードから渡し直します（続けて 3 回落ちたら stream のエラー）。

位置は rank ごとの `{rank, record, sub}`（その rank のレコードを `record` 件取り終え、次のレコードの出力を `sub` 件取った）です。再開では、W は変えられません。

### 取得と照合

各ファイルは、サイズ、sha256、チャンク（既定 4 MiB）ごとの sha256 で識別します（`kairo loader identify PATH|URL...` がこの一覧を JSON で出します）。どのチャンクも照合してから使います。取得元の失敗は 1 秒から 5 分まで間隔を延ばしながら取り直し、その間、受け取り側は待ちます。照合に合わないチャンクはもう一度取り、2 度合わなければ `raw data changed` で stream を止めます。ファイルを最後まで読むとレコード数が分かり、それがあれば途中から始めるときにそのファイルを読まずに飛ばせます。

### 受け渡し

`kairo loader serve --spec SPEC.json --listen unix:PATH|tcp:HOST:PORT [--world N] [--positions POSITIONS.json] [--session S] [--state DIR] [--origin http|dir:DIR]` が stream を作ります。各 rank は 1 本の接続でつながります。フレームは「4 バイトのリトルエンディアンの長さ、種別 1 バイト、中身」です。

- 受け取り側から: `hello`（`{"version":1,"rank":R,"world":W,"position":{"rank":R,"record":K,"sub":S},"session":...}`。`position` と `session` は接続し直すときだけ）、`credit`（あと何件受け取るか）
- Loader から: `ready`（その rank の開始位置、rank の数、セッション）、`item`（レコード番号、出力番号、そのレコードの最後かどうか、中身）、`skip`（出力のなかったレコード）、`end`、`error`

開始位置は Loader が決めます（checkpoint の位置、なければ最初）。一度でも項目を渡した rank が位置なしで接続し直すと拒否します（先に送って受け取られなかった分が抜ける、または繰り返すおそれがあるためです。rank ごとに stream を 1 つ開いたままにします）。渡した rank と rank の数は `--state DIR`（init は stream ごとの実行ディレクトリを渡します）に残すので、Loader を起動し直しても覚えています（残せなければ stream のエラーです）。位置なしの接続を拒否するかどうかは、前の接続が離れるのを待ってから判断します。位置は自分の rank のものでなければならず（`position` に `rank` を書きます）、SDK の `Stream.rank`・`Stream.world`・`Stream.position` は読み取り専用です（`position` は写しを返します）。rank ごとの項目は、その rank の作り手（producer）が作ります。どの rank の作り手も順番全体を読み、自分の rank のレコード（g mod W = r）だけを stage に通して、自分の rank のキューにだけ入れます。そのため、受け取っていない rank や遅い rank（DDP で allreduce に待たされているなど）のキューが満杯になっても、待つのはその rank の作り手だけで、他の rank を止めることはありません。2 つの rank が同じ件数を受け取っても、そのために要るレコード数は違い（1 レコードの出力は 0 件にも多数にもなります）、その差は走るほど広がるので、1 つの作り手が全 rank の分を順に作ると、どちらかのキューが満杯になった時点で両方が止まってしまうからです。どの rank も接続しておらず、どの rank のキューもいっぱいなら、誰かが戻るまで何も作りません。`hello` に位置を書けるのは、`ready` で受け取ったセッションを添えて接続し直すときだけです。セッションは Attempt ごとに init が決め、Loader を起動し直しても変わりません（`--session` なしなら起動ごとにランダム）。そのため、学習コードが別の Attempt で覚えた位置で始めることはできません。Loader が始めた位置より前へ戻る位置も拒否します。渡したのと違う位置で接続し直した rank があれば、各 rank を渡した位置から作り直します。Python では次のように受け取ります。

```python
from kairo_sdk import streams
with streams.open("mix-v9") as train:   # KAIRO_STREAM_MIX_V9、RANK、WORLD_SIZE から
    for item in train:                  # bytes
        ...
    where = train.position              # {"rank", "record", "sub"}
```

接続が切れたら、その位置で自動的に接続し直します。torch では `streams.dataset(name, decode)` が IterableDataset を返します（1 つの rank は 1 つのプロセスで受け取るため、`num_workers=0` で使います）。

## Worker SDK

```python
from kairo_sdk import AttemptSession, ProgressEnvelope, SuspendResult

with AttemptSession.from_environment() as kairo:
    for step in train():
        if kairo.safe_point(
            progress=ProgressEnvelope("step", step, total_steps, detail={"loss": loss}),
            checkpoint=lambda command: SuspendResult(save_checkpoint(command.command_id)),
        ):
            break
```

### 管理対象 Attempt に渡される環境変数

Kairo が管理する Attempt には、以下の環境変数が渡されます。

- **基本接続・認証**: `KAIRO_API_URL`、`KAIRO_EXECUTION_ID`、`KAIRO_ATTEMPT_ID`、`KAIRO_ATTEMPT_TOKEN`（デーモンが TLS を提供している場合は `KAIRO_API_CA` も付与）
- **配置・リソース割り当て**: `KAIRO_NODE_ID`、`KAIRO_EXECUTOR_ID`、`KAIRO_RESOURCE_IDS`、`KAIRO_RESOURCE_BINDINGS`、および `CUDA_VISIBLE_DEVICES`（Lease された GPU の UUID が設定され、インデックス番号は決して使用されません）
- **再開時**: `KAIRO_CONTINUATION_REF`
- **volatile store**: Executor がvolatile store（[データストア](#データストア)を参照）を提供している場合は `KAIRO_SCRATCH` と `KAIRO_OUTPUT_DIR`

SDK は、Attempt を Linux のコンテナで動くものとして扱います（プロセスの身元は `/proc/<pid>/stat` から読みます）。Python SDK の `DistributedAdapter` は、1 つの Attempt の中の torch の分散（1 つのコンテナの中の DDP）を協調させます。rank 0 だけがポーリングと応答を行い、チェックポイントは全 rank がそろって作ります。`local_continuation` は、Kairo が渡した継続の参照をそのまま返します（全 rank が同じコンテナのファイルを見るため、別ノードへの受け渡しはありません。相対パスは各 rank の作業ディレクトリから読まれるので、rank ごとに作業ディレクトリを変えるなら絶対パスで公開します）。Gang 用の補助（`GangContext`、rank ごとの Attempt）はなくなりました。

### セーフポイントの挙動と Python・Rust SDK

`safe_point` およびワーカー制御の仕様は次のとおりです。

- **`suspend` Command**: `accepted` と `checkpointing` を応答してチェックポイント用コールバックを実行し、得られた継続（continuation）を `checkpointed` として公開したうえで `True` を返します。
- **`checkpoint` Command（`kairo task checkpoint`）**: `suspend` と同じコールバックを実行し（両者は `command.kind` で判別できます）、同じ手順で応答しますが、`safe_point` は `False` を返し、ワーカーはそのまま処理を継続します。
- **コールバック失敗時**: コールバックが失敗した場合、または継続を返さなかった場合は `rejected` を応答します。このとき、`suspend` であればエラーが送出（raise）されますが、`checkpoint` Command であればワーカーは処理を継続します（実行の終了ではなくコピーの作成を求めているに過ぎないためです）。
- **未知の Command**: 未知の Command 種別（command kind）はスキップされます。
- **一時的な通信障害**: デーモンやネットワークが一時的に利用できない場合（ポーリング・ハートビート・応答における一時的エラーまたはリトライ対象エラー）は、そのセーフポイントでは Command が届いていないものとして扱われます。ワーカーは処理を継続し、エラーを `last_control_error` に保持したまま進み、Command は後から再送されます。

- **停止シグナル**: `install_stop_signals()` を呼び出した後に `SIGTERM`（強制停止のグレースフルフェーズ。コンテナの init がランナーからの停止を Attempt に渡します）を受信すると、次のセーフポイントでコールバックが 1 回だけ実行され（reason は `stop_signal`、Command ID なし、応答送信なし）、`True` を返します。これにより、作業を手動で再開できる状態を保てます。
- **退役・フェンス時**: トークンが拒否されたワーカー（Attempt が `quiesced` すなわち退役（retired）状態になった場合）や、フェンスされた（fenced）ワーカーは報告を停止します。

Python SDK には、このほか `ProgressReporter`、`continuation_or`、`CommandJournal`（Command ID ごとにべき等なチェックポイントを管理）、`provenance.capture`、およびオペレータ向けの `KairoClient` が用意されています。

Rust クレート `sdk/rust/kairo-sdk`（ブロッキング実装、`ureq` + `rustls`）も同じワーカー側機能を実装しています。`Attempt::from_env`、1 つのハートビートスレッドと `safe_point` を備えた `Session`、プロセス全体で共有される `reporter(unit)` / `phase` 進捗ハンドル、および `example-app` の旧 `kairo-progress` と互換性のある `kairo_sdk::Progress` を提供します。依存関係は git 経由で指定します。

```toml
kairo-sdk = { git = "https://github.com/esehehelp/Kairo", rev = "<commit>" }
```

Rust の停止シグナルの処理（feature `signals`、`install_stop_handlers`）は Unix の `SIGTERM` だけを扱い、ほかの OS では何もしません（`request_stop` はどこでも使えます）。

すべての SDK は、テストスイート内で `sdk/conformance/*.json` をリプレイして適合性を検証します。SDK のテストは Linux で走らせます（CI はコンテナの中）。Python のテストは Linux 以外で走らせると、プロセスの身元を Linux の形の仮の値に差し替えます（torch の複数 rank のうち、プロセスを登録するテストは Linux でだけ走ります）。Rust のテストのうち、自分のプロセスを登録するものは Linux でだけ走ります。

### プロジェクト管理の低レベルコントローラポリシー

Execution API へ直接投入するプロジェクトでも、`ResumeAfterKairoPreemption` をオプトインで利用できます。ポリシースナップショットと正規化された投入内容（canonical submission）は、最初の API 呼び出しの前にプロジェクト側のコントローラジャーナルへ書き込まれます。投入内容には、ダイジェスト（`@sha256:` と 64 桁の 16 進数）で終わる `image` と、イメージの中の絶対パスの `cwd` が要ります。この 2 つがないもの、または形が違うものは、ジャーナルに書く前に拒否します。参照のそれ以外の部分はデーモンが確かめます。

```python
from kairo_sdk import ControllerJournal, KairoControllerClient, ProjectController

controller = ProjectController(
    KairoControllerClient(),  # KAIRO_API / KAIRO_TOKEN / KAIRO_CA_FILE、または設定ディレクトリを使用
    ControllerJournal("local/llm-controller.db"),
)
execution_id = controller.submit_with_resume_after_kairo_preemption(
    activity_id="jalm-300m-v3",
    project="example-train",
    submission=execution_request,
)
controller.run_forever()
```

コントローラジャーナルは 1 プロジェクトにつき 1 つ使用してください。activity ID はそのプロジェクト管理のジャーナル内でローカルに扱われます。

このポリシーを発動できるのは、以下の条件をすべて満たした場合に限られます。

- `priority_preemption` Command であること
- まさにその Command に対して応答済みの継続（continuation）が存在すること
- Attempt が `quiesced` になっていること
- Lease が解放されていること

コントローラは後続の Execution を投入する前に、決定論的な判定結果（deterministic decision）をジャーナルに記録します。API のリトライ時には同一のクライアントリクエスト ID（client request ID）を再利用するため、コントローラのクラッシュやイベントの完全リプレイが発生しても、別の後続 Execution が重複して作成されることはありません。

したがって、イベントカーソルはあくまで最適化手段に過ぎず、永続化された判定記録（durable decision）こそが正当性の境界（correctness boundary）となります。委任の有効性（delegation validity）は、サーバ側が保持するワークフロー状態や暗黙のデフォルト仕様ではなく、論理アクティビティ（logical activity）の現在の Execution に紐づけられた不変のポリシースナップショットによって決まります。

## パイロット受け入れ契約

1. 既存の LLM 事前学習（pretrain）を `observe_only`（観測専用モード）で検査しても、Lease が作成されないこと。
2. 1 GPU ジョブが `run -> suspend -> checkpointed -> process exit -> quiesced -> lease release` の遷移を完了でき、その後、明示的に選択された Task ポリシーによって再開用の不変 Execution を投入できること。
3. どの状態遷移のタイミングでデーモンまたはプロセスに障害が発生しても、GPU の重複所有や、応答済み継続（acknowledged continuation）の喪失が起きないこと。
4. プロジェクトが選択した短時間・高優先度の Execution によって協調的プリエンプション（cooperative preemption）を発動できること。オーケストレーション対象のジョブを再開するかどうかはバージョン管理された Task ポリシーが決定し、Execution API を直接利用するユーザーにはプロジェクト管理コントローラの選択肢が残されていること。
5. 同一 Command が再送されても、チェックポイントの公開処理が破損しないこと。
6. 静的な Task DAG では、すべての依存先が成功した後にのみ対象ノード（Task）が投入され、リプレイを行っても 1 つのポリシー判定に対して重複した Execution が作成されないこと。
7. （廃止）2 GPU の DDP Execution は all-or-nothing の Gang Lease を取得し、Lease 解放前にすべての rank が静止（`quiesced`）すること。Gang はなくなりました（[Gang（廃止）](#gang廃止)）。

クラウドのプロビジョニング、上記 4 種類以外のロール、サービスの監視・常駐管理（service supervision）、cron / フェアシェアスケジューリング、および CPU / RAM の厳格な強制制限（hard enforcement）は、このパイロットの対象外です。

## ハードウェアベンチマーク

### ベンチマーク実行

`kairo hardware bench HOST DEVICE [--kinds K,...] [--node NODE] [--dry-run]`（admin 権限）は、設定値のグリッド（grid）上の各設定において、デバイスがどの程度の性能を発揮し、どれだけの電力量（energy）を消費するかを計測します。`DEVICE` には、GPU の UUID、`cpu`、`disk:FILESYSTEM`（ホストに属するいずれかのノード上の filesystem Provider のパス）、または `net:NODE`（ホスト側のノードから `NODE` へのネットワーク）を指定します。

ベンチマーク実行（bench run）は、Kairo 管理下のプロジェクト `kairo`、キュー `hardware-bench` に属する通常の Task として実行されます。Task 名は実行 ID にちなんだもの（`kairo task show kairo hardware-bench bnc_...`）となり、`project apply` で宣言した場合と同じ形式（`RunToCompletion@v1`、最初の Execution が即座に投入される）で宣言されます。優先度は `-1000000`、チェックポイント可能かつプリエンプション可能であるため、他のすべての Execution の完了を待ち、他の Execution をプリエンプトすることはありません。また、対象デバイスを単独で占有し、そのデバイスを必要とする別のジョブが到着すれば中断（suspend）されます（後述の「[中断と再開](#中断と再開)」を参照）。

| デバイス | Lease | 種別（Kinds: 記載のあるものはスイープ対象、それ以外はデフォルト設定で計測） |
|---|---|---|
| GPU | 対象 GPU（排他リクエストで対象 `device` を指定可能）、1 CPU、2 GiB | スイープ対象: `gemm`（bf16）、`copy`、`gemv-b1`、`gemv-b8`／デフォルト計測: `pcie-h2d`、`pcie-d2h`、`pcie-h2d-latency`、`pcie-d2h-latency` |
| `cpu` | ノードの全 CPU（`cpu_millis_min` = 全量: 同じノードで他ジョブは並行実行されない）、4 GiB | スイープ対象: `cpu-compute`、`cpu-single`、`cpu-stream` |
| `disk:FS` | `FS` 上の 9 GiB（テスト用ファイル） | `disk-seq-read`、`disk-seq-write`（qd 32）、`disk-rand-read`（qd 32）、`disk-rand-read-qd1`、`disk-rand-write`、`disk-fsync`、`disk-sustained` |
| `net:NODE` | なし（Task にならず、2 つのノードの agent どうしで測ります。下記） | `net-latency`、`net-bandwidth-send`、`net-bandwidth-recv`（4 ストリーム） |

#### 実行条件・競合・コンテナ

- `--kinds` には個別の種別名、またはプレフィックスによるグループ指定（`pcie`、`gemv` 等）を渡せます。
- GPU および CPU のベンチマークは設定をスイープするため、以下の場合は拒否されます。
  - `control = false` と宣言されたデバイス
  - コントローラのないホスト
  - コントローラが対象デバイスのサポート機能をまだ報告していない段階
  - 対象デバイスに手動設定（manual setting）が適用されている間
- ストレージおよびネットワークのベンチマークはデフォルト設定で計測するため、コントローラは不要です。
- 1 デバイスにつき待機中または実行中にできるベンチマークは 1 つまでです（競合時は `409 bench_conflict`）。
- `--dry-run` を指定すると、実行を投入せずに run の内容、Lease、および argv を出力します。

ベンチマークは、ほかの仕事と同じくコンテナで実行されます。対象のデバイスを持ち、コンテナの Executor を持つノードで走り、ノードでの準備や設定（以前の `[bench]`）は要りません（`[bench]` が残っている設定は拒否されます）。

- **イメージ**: CPU と storage のベンチマークは制御スナップショットと同じ小さなイメージ（`store.BenchImage`）、GPU のベンチマークはダイジェスト固定の CUDA 13.3 devel イメージ（`store.BenchGPUImage`。GPU ノードのドライバが対応する版）で走ります。
- **GPU のプログラム**: `kairo-bench-gpu` のソースは `kairo` のバイナリに埋め込まれています（`bench/gpu`）。ドライバは最初の GPU の種別の前に、イメージの CUDA ツールキット（`nvcc`、`bench/gpu/Makefile` の `CUBLAS_STATIC=1` と同じフラグ）でビルドし、プロジェクト `kairo` のキャッシュ（`KAIRO_CACHE_DIR`）に、ソースのダイジェストの名前で置きます。同じノードの次のベンチマークは、ソースが変わるまでそれを使います。ビルド文字列は `src-<ダイジェスト>` です。
- **storage のベンチマーク**: Executor が、測るファイルシステムの上にディレクトリ `.kairo-bench` を作り、コンテナの `/kairo/bench-disk`（`KAIRO_BENCH_DISK`）に書き込み可能でマウントします。テスト用のファイルはそこに書かれます（コンテナ自身のディスクではなく、測るファイルシステムに）。

Execution の argv は、コンテナの中の Kairo のバイナリで動くドライバ `/kairo/bin/kairo hardware bench-run --run ID` となります。ドライバは Attempt トークン（`KAIRO_API_URL`、`KAIRO_API_CA`、`KAIRO_ATTEMPT_TOKEN`）を使ってワーカー API 経由でデーモンと通信します。このトークンがアクセスできるのは自身の Attempt が担当する run のみであり（他の run へのアクセスは `403`、ベンチマーク以外の Attempt からのアクセスは `404`）、変更できる設定も自身の Lease に紐づく `bench` 設定のみです。

#### ネットワークのベンチマーク（agent どうし）

`net:NODE` は、ホスト側のノード（`--node`、既定はホストのノードのうち、転送の口（Executor の `pack_addr`）を持つ最初のもの）と `NODE` の間の通信を、データの転送（packs）と同じ経路で測ります。Task も Execution も作らず、何も予約しません。

1. デーモンは run を作り、トークンを 1 つ発行します（`bench_runs.net_token`）。
2. `NODE` の agent は 15 秒ごとに自分が測る run を尋ね（エージェント API の `net-benches`）、見つけた run ごとにドライバ（`hwbench.Driver`。Task のベンチマークと同じもの）を自分の中で動かします。ワークロードは agent 自身のバイナリ（`kairo bench run net-* --pack-peer ADDR --pack-run RUN --pack-ca CA`、トークンは環境変数 `KAIRO_BENCH_NET_TOKEN`）で、結果はエージェント API で報告します。
3. 測られる側の agent は、転送の口（7475、TLS、HTTP/1.1）の `/bench/net/<run>` で接続を受け、トークンをデーモンに確かめてから（`net-bench-authorize`）、接続をベンチマークの対向（`kairo bench serve` と同じ処理）に切り替えます（`Upgrade: kairo-bench`）。測った値には TLS の分が含まれます。転送が実際に通る経路の値だからです。
4. 作ってから 2 時間で終わらない run は失敗にします（測る側の agent がいない場合など）。失敗にしたあとで測る側の agent が書き込んでも、run は失敗のままです。
5. ノード 1 つにつき、生きている network ベンチは 1 本だけです（2 本が同じリンクを分け合って測ると、どちらの値も正しくないため）。測られる側の転送の口は、ベンチの接続を同時に 16 本まで受けます。
6. `observe_only` の agent は、測ることも測られることもしません。`ca_file` を書いていない agent は、転送と同じく、システムの信頼するルートで相手の証明書を確かめます。

#### ドライバの実行手順

ドライバは種別（kind）ごとに 1 つのプリミティブをその種別全体にわたって実行し（種別が完了次第すぐに停止、`--seconds` で上限時間を制限）、時間基準（クロック）にはプリミティブ自身のインターバルストリームを使用します。

1. **デバイス情報の記録**: `start` レコードからデバイスの基本情報（`device`、ワークロードのバージョンとビルド、GPU ドライバ）を記録します。最初の種別を開始する前に、GPU のアイドル時の観測温度（temperature at rest）も記録します。
2. **ウォームアップと初回リファレンス計測**: 自身の設定をクリアし、コントローラがデフォルト設定の適用完了を報告するまで待機します（ポリシー設定も適用されていない状態）。その後、定常状態に達するまでウォームアップを行います（温度変化が 60 秒間で `0.5 °C/min` 未満、温度を取得できない場合はスループット変化が `0.5%/min` 未満になるまで。最大 5 分間。達しない場合は `not steady`）。続いて 1 回目のリファレンスを計測します（安定化待ち 20 秒、計測 30 秒）。
3. **グリッドのスイープと第 2・第 3 リファレンス**: コントローラが報告した機能（capabilities）に基づき、以下のグリッドをスイープします。
   - **GPU**: まずグラフィックスクロック上限（graphics clock cap）を最大サポートクロックからその 50% まで、最大サポートクロックの 5% 刻みで下げながらスイープします（各値は最も近いサポートクロックへ丸められます）。次に、最大値より低い各サポートメモリクロックについて、グラフィックスクロック上限 100% および 85% でスイープします。さらに（`gemm` のみ）、デフォルト電力上限の 90%、80%、70%、60% のうちサポート範囲内に収まる電力上限をスイープします。
   - **CPU**: `max_perf_pct`（`intel_pstate`）または `max_freq_mhz`（`cpufreq`）を 100% から 50% まで 5% 刻みでスイープします（Windows は周波数範囲を報告しないためスイープは行われません）。
   - 各計測点では、設定を要求し、コントローラがその generation の適用完了を報告するまで待機（60 秒、超過時は `not applied`）したうえで、安定化待ち 20 秒、計測 30 秒を行います。
   - グリッドの半分を終えた時点で 2 回目のリファレンスを、すべてのグリッドを終えた最後で 3 回目のリファレンスを計測します。
4. **計測点の算出指標とエネルギー計測**: 各計測点について、単位時間あたり処理量（work/s）、3 回のリファレンス平均に対する相対性能、平均電力、単位処理あたり消費電力量（energy per unit of work）、実測クロック、温度、クロックイベント理由（clock event reasons）、および稼働率を算出します。
   - GPU の消費電力量は、各インターバルにおける NVML の `energy_mj` の合計です。
   - CPU の消費電力量はデーモンから取得され、計測ウィンドウにわたるホストの CPU パッケージサンプルから算出されます（コントローラまたはノードの系列のうち計測ウィンドウの大部分をカバーする系列を使用し、同じパッケージが二重加算されることはありません）。各サンプルは直前サンプルからの経過区間を対象として計上され、サンプルの欠落が計測ウィンドウの 10 分の 1 を超えると `cpu energy not covered` となります。
5. **有効性判定と種別ごとの記録**:
   - **計測点が無効（invalid）になる条件**:
     - 設定が適用されなかった、または計測終了時点で維持されていなかった
     - サーマルイベント理由（`0x20`、`0x40`）を伴い、実測クロックがグラフィックスクロック上限を 2% 以上下回った（この状態ではクロック上限の意味をなさないため。一方、上限を設定しない（uncapped）計測点は発熱を含めたデバイスの実挙動を計測します）
     - 他の何かがデバイスを使用していた（GPU 上の Kairo 以外が起動したプロセス、CPU ベンチマーク時の同一ホスト内ノード上の別 Lease、対象ファイルシステムを予約している別 Lease）
     - 手動設定（manual setting）またはポリシー設定が有効だった
     - プリミティブが一切処理を行わなかった、または途中で終了した
   - **種別全体の判定と記録**:
     - 3 回のリファレンスのばらつき `(max - min) / mean` が 1% を超える場合、またはリファレンス自体に無効なものが含まれる場合、その種別全体が無効となります。無効な種別のカーブは使用されませんが、他の有効な種別のカーブは使用されます。
     - 1 回のみ計測する種別は、その計測結果自身がリファレンスとなります。`disk-sustained` は書き込み上限に達するまで計測を行います。
     - 各種別のリファレンスは、その種別が完了した時点で直ちに記録されます（`POST /api/worker/bench/kinds`）。
6. **設定解除と run の完了**: 自身の設定をクリアし（もっとも Lease の解放時にも自動的にクリアされます）、run を以下のいずれかの状態で終了します。
   - `finished`: すべての種別が有効な場合
   - `partial`: 一部の種別のみ有効な場合（ポリシーとプロファイルは有効な種別のカーブを使用します。理由欄には無効となった他の種別とその原因が記載されます）
   - `invalid`: 有効な種別が 1 つもない場合
   - `failed`: ドライバが完了させることなく Task が終了した場合（強制停止、クラッシュ、キャンセルなど）。理由として `the bench execution ended without its results (CAUSE)` が記録され、それまでに記録されたデータは保持されますが利用されることはありません。

#### 中断と再開

ドライバは SDK の `safe_point` と同様に協調的ワーカーとして動作します。すべての種別・すべての計測点の開始前、ウォームアップ中の 15 秒ごと、および他のジョブを待機している間に、Attempt の Command をポーリングします（`GET /api/worker/commands`）。

- **`suspend` の処理**: プリエンプション、`pause`、ノード隔離などによる `suspend` を受け取ると、`accepted` と `checkpointing` を応答し、プリミティブを停止して bench 設定をクリアします（いずれにせよ Lease 解放時にデバイスはデフォルト設定へ戻ります）。その後、継続 `{"run":"bnc_...","done":["pcie-h2d",...]}` とともに `checkpointed` を応答し（同一 Command が再送された場合も同じ応答を返します）、Attempt は終了コード `0` で終了します。その間に取り下げられた `suspend`（`404`）や、他の種別の Command はスキップされます。
- **継続 Execution による再開**: Task の継続 Execution（continuation execution）は同じ run を引き継ぎます（イベント `hardware_bench_continued`）。run は再び待機状態になり（`hardware profile` では完了済み種別とともに `suspended` と表示されます）、中断された種別の計測点は破棄されます。新しい Attempt は起動時に `hardware_bench_resumed` を発行し、完了済みの種別をスキップして残りの種別を計測します。中断された種別については最初から（ウォームアップとリファレンス計測も含めて）やり直し、自身の Lease の下で設定を要求します。中断と再開を経た run も、最終的には通常の run と同じように終了します。
- **強制停止・`task rerun`**: 強制停止のグレースフルフェーズ（`SIGTERM`、`CTRL_BREAK`、Ctrl-C）を受信した場合、ドライバは 1 秒以内に終了します。その際、プリミティブを停止（プリミティブにもシグナルが届きます）して設定をクリアし、継続などの公開は一切行いません。結果として Task は `stopped`、run は `failed` になります。2 回目のシグナルを受信すると、実行中の停止処理自体がキャンセルされます。なお、ベンチマークの Task に対して `task rerun` を実行しても、`failed` になった run は復活しません（ドライバが run の終了済み状態を検知して失敗します）。やり直す場合は新しいベンチマークを実行してください。

#### 安全制限・保存データ・プロファイルと API

プリミティブ自身の安全制限（空き容量、1 プリミティブ実行あたり書き込み上限 20 GiB、メモリ上限）はそのまま適用されます。run と計測点のデータは無期限に保持されます（`bench_runs`、`bench_points`）。

ベンチマーク実行では、以下のイベントが発行されます。

- `hardware_bench_requested`
- `hardware_bench_started`
- `hardware_bench_continued`
- `hardware_bench_resumed`
- `hardware_bench_finished`（`state` と `valid_kinds` を含む）
- `hardware_bench_invalid`（有効な種別が 1 つもない run、および `failed` になった run）

`kairo hardware profile HOST [DEVICE] [--json]` は、デバイスごとに直近の run の状態（部分的に有効なものは `valid for 7 of 8 kinds: gemm: references spread ...`、中断中のものは `suspended since ..., 3 of 8 kinds done` など）を表示し、さらに種別ごとに、有効なリファレンスを持つ直近の終了済み run のカーブを表示します。出力には以下の情報が含まれます。

- 各計測点の設定、相対性能、電力、単位処理あたり消費電力量、クロック、温度、有効性
- そのデバイスの `max_slowdown` に対してポリシーが選択する計測点（`*` マーク付き）
- 同一デバイスの前回同一種別 run からのドリフト（`drift from bnc_... (10-20 09:00): work/s -0.8%, energy/work +1.2%`）

仕様値（スペック）や他のデバイスとの比較は一切行いません。HTTP API としては、`GET /api/hardware/{host}/profile` が `{"host", "runs"}`（各 run に `references` と `points` を含む）を返し、`POST .../bench` が `{"kinds", "node", "dry_run", "actor"}` を受け取って `{"run", "execution", "dry_run"}` を返します。

### プリミティブ

ハードウェア効率を計測するためのプリミティブ群です。各プリミティブは固定かつ決定論的なワークロードを指定時間実行し、実行結果をストリーム出力します。これにより、オーケストレータはインターバルを切り出し、リファレンス計測と比較し、スイープする各デバイス設定（クロック上限、電力上限）での消費電力量と組み合わせることができます。プリミティブが記録するのは日付とワークロードバージョンを付与した実測の事実のみであり、ベンダーの公称スペックとの比較は一切行いません。

ベンチマーク本体は通常の Execution として実行でき、コマンドライン（argv）は次のとおりです。

```text
kairo bench run WORKLOAD [--seconds S] [--interval S] [workload flags]
kairo-bench-gpu WORKLOAD --device GPU-UUID [--seconds S] [--interval S] [flags]
kairo bench serve --listen HOST:PORT        # net-latency / net-bandwidth の対向サーバ
kairo bench run net-latency --pack-peer HOST:PORT --pack-run RUN --pack-ca CA   # agent の転送の口に対して（トークンは KAIRO_BENCH_NET_TOKEN）
```

実行は `--seconds`（デフォルト `60`、`0` を指定すると停止されるまで無制限実行）の経過、または `SIGTERM`・Ctrl-C・`CTRL_BREAK` によって終了します。終了時は新規処理の発行を止め、実行中（in-flight）のリクエスト完了を待ってから、最終インターバルと終了レコードを書き出し、クリーンアップを行って終了コード `0` で終了します。

使い方エラー（不正または危険なパラメータ指定）では終了コード `2` で終了し、レコードは一切書き出しません。マシン側の条件により拒否された場合（空き容量やメモリの不足、該当デバイスの不在など）や実行中に失敗した場合は、終了レコードを書き出して終了コード `1` で終了します。各ワークロード固有のフラグは `kairo bench run WORKLOAD --help` で一覧できます。

| ワークロード | 処理内容 | 単位 |
|---|---|---|
| `cpu-compute` | 1 イテレーションあたりレジスタ上で FP64 FMA チェーン 8 本 + xorshift64 を 1 ステップ実行（22 ops）、`--threads`（デフォルト: 全論理 CPU） | `op` |
| `cpu-single` | 上記と同じ処理を `--cpu` にピン留めした 1 スレッドで実行（`start` レコードにピン留めが維持されたかを記録） | `op` |
| `cpu-stream` | 3 つの `--bytes` 配列（デフォルト各 1 GiB）に対する STREAM triad `a=b+3c`（1 要素あたり 24 バイト: 読み込み 2 + 書き込み 1、write-allocate は計上しない） | `byte` |
| `disk-seq` | 1 MiB シーケンシャルアクセス（`--mode read\|write`、`--qd 1\|32`） | `byte` |
| `disk-rand` | 4 KiB ランダムアクセス（`--mode read\|write`、レイテンシ計測は `--qd 1`、IOPS（`ops`）計測は `32`） | `byte` |
| `disk-fsync` | 4 KiB 書き込み + `fdatasync` / `FlushFileBuffers` を 1 回ずつ逐次実行 | `byte` |
| `disk-sustained` | `--max-write-bytes` に達するまで 1 MiB シーケンシャル書き込み（デフォルト `--qd 32`）を継続: インターバルごとの帯域で SLC キャッシュ切れによる速度低下（cliff）を観測可能 | `byte` |
| `net-latency` | 64 バイトの TCP ピンポン通信（`TCP_NODELAY`）、各エコー内容を検証 | `request` |
| `net-bandwidth` | `--streams 1\|4` 本のコネクションで 128 KiB の書き込み（`--dir send`）または読み込み（`--dir recv`）を実行、アプリケーション層バイト数を計測 | `byte` |
| `gemm`（GPU） | cuBLAS bf16 Tensor Core GEMM（fp32 累算）、`--n` 8192: 1 op あたり `2n³` | `flop` |
| `copy`（GPU） | デバイス間（device-to-device）コピー、合計フットプリント `--bytes`（デフォルト 2 GiB）: 読み込み + 書き込みを計上 | `byte` |
| `gemv`（GPU） | bf16 の `n×n` 行列と `--batch 1\|8` 本のベクトルの積（LLM デコードのようなメモリ帯域律速処理） | `byte`, `flop` |
| `pcie`（GPU） | ピン留めホストメモリによる `--dir h2d\|d2h` 転送、`--mode bandwidth` は 64 MiB 転送、`latency` はパーセンタイル付き 4 KiB 転送 | `byte` |

### 記録とプロファイル

出力は 1 行につき 1 つの JSON オブジェクトです。すべてのレコードに共通して、`schema`（`kairo.bench/v1`）、`type`（`start`、`interval`、`end`）、`run_id`（16 桁の 16 進数）、`program`（`kairo` または `kairo-bench-gpu`）、`workload`、`wall`（UTC、ナノ秒精度の RFC 3339）、および `mono_ns`（`start` レコードからの単調増加ナノ秒、Windows では Go の標準クロックがタイマーティックに従うため `QueryPerformanceCounter` を使用）が含まれます。

- **`start`**: セットアップ完了後（ファイルのプリフィル、メモリ確保、コネクション確立、GPU ウォームアップ完了後）、計測対象処理の開始直前に 1 度だけ出力されます。
  - `version`（ワークロード定義のバージョン: 1 単位の処理内容が変更されたときに更新）
  - `build`（VCS リビジョン）
  - `params`（有効なすべてのパラメータ）
  - `units`（主要単位が先頭）
  - `host`（`hostname`、`os`、`os_version`（`kairo` のみ）、`arch`、`cpu_model`、`logical_cpus`）
  - `device`（テスト対象デバイスを識別する情報、詳細は後述）
  - `setup`（セットアップ時の事実情報: `direct_io`、`prefill_ns`、`pinned`、GPU のバッチサイズなど）
- **`interval`**: `--interval` 秒ごと（デフォルト 1 秒）に固定境界 `start + k·interval` で出力されます。
  - `index`、`begin_ns`、`end_ns`（各インターバルは直前の終了位置から開始。最後のインターバルは端数（partial）となり、`kairo` は停止時点で実行中だったリクエスト分をここに計上するが、`kairo-bench-gpu` は期限またはシグナル時点でインターバルを区切り、実行中のバッチは計上せずにドレインする）
  - `work`（そのインターバル内に完了した `{unit: count}`、操作完了時に計上）
  - `ops`（操作回数: リクエスト数、往復回数、カーネルチャンク数、GPU 操作数）
  - `device_busy_ns`（GPU 側で計測されたビジー時間、CUDA イベントによる計測。`kairo` では `null`）
  - `latency`（`count`、`min_ns`、`mean_ns`、`p50_ns`、`p99_ns`、`p999_ns`、`max_ns`、または `null`）
  - GPU のインターバルにはさらに `nvml` が追加されます: `samples`、`energy_mj`（インターバル間のエネルギーカウンタ差分）、`power_mw`、`sm_clock_mhz`、`mem_clock_mhz`、`temperature_c`、`util_gpu_pct`、`util_mem_pct`（それぞれ `--sample-ms` 周期サンプルの `{min, mean, max}`）、`pstate`（`{min, max}`）、および `clocks_event_reasons`（全サンプルの論理和（OR））。NVML で読み取れない値は `null` になります。
- **`end`**: 必ず最後に出力されます。
  - `reason`（`duration`、`signal`、`limit` は終了コード `0`、`error`、`refused` は終了コード `1`）
  - `limit`、`limit_detail`（上限到達による終了時）
  - `error`
  - `elapsed_ns`（計測ウィンドウ長: 最後のインターバルの終了時刻。`mono_ns` はクリーンアップ後のため、これより後になる）
  - `intervals`
  - 実行全体の `work`、`ops`、`device_busy_ns`、`latency`
  - `energy_mj`（GPU のみ）
  - ストレージ向け: `bytes_written`（プローブとプリフィルを含む）および `file_removed`
  - `start` レコード出力前に拒否または失敗した実行では `end` レコードのみが出力され、その場合は `params` と（判明している場合）`device` が含まれます。

パーセンタイルは対数線形ヒストグラム（log-linear histogram）上の最近傍順位法（nearest-rank）で算出されます（128 ns 未満は厳密値、それ以上は 2 のべき乗あたり 128 バケット: 誤差 0.4% 以内）。両プログラムで同じバケット定義を使用します。

`device` の内容は `kind` ごとに次のように構成されます。

- `cpu`: `model`、`logical_cpus`、`gomaxprocs`、`fma`
- `memory`: `cpu` の各フィールドに加え、`total_bytes`、`available_bytes`
- `storage`: `path`、`volume_path`、`filesystem`、`free_bytes`、`total_bytes`、セクタサイズ、`disk_model`、`disk_serial`、`disk_revision`、`disk_bus`。さらに Windows では `volume_serial`、Linux では `mount_source`、`block_device`、`rotational`
- `network`: `local_addr`、`remote_addr`、ローカルアドレスを保持する `interface`（ルーティングで選択されたインターフェース）、その `mtu`、および OS が報告する `link_speed_tx_bps` / `link_speed_rx_bps`（仮想 NIC では無意味な値が報告される場合があります）
- `gpu`: 後述（「[GPU ベンチマーク](#gpu-ベンチマーク)」を参照）

### 安全設計

- **ストレージ**:
  - **空き容量チェック**: テスト対象ファイルシステム上の既存ディレクトリ `--path` に作成するテストファイル `kairo-bench-<id>.tmp` は、`--file-size`（デフォルト 1 GiB、`disk-fsync` は 64 MiB、`disk-sustained` は 8 GiB）が「空き容量 − `--keep-free`（デフォルト 5 GiB） − 1 GiB」以下でない限り拒否されます。
  - **書き込み上限と空き容量監視**: 書き込まれたすべてのバイト（プリフィルを含む）は `--max-write-bytes`（デフォルト 20 GiB）に対して計上され、プリフィルだけで上限を超える場合は開始前に拒否されます。実行中に上限を超える書き込みを行う直前で `reason` を `limit` として終了し、書き込み実行中に空き容量が `--keep-free` を下回った場合も終了します。
  - **ダイレクト I/O**: OS キャッシュはバイパスされます（Windows は `FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH`、Linux は `O_DIRECT`、4 KiB またはデバイスのセクタサイズ境界にアラインされたバッファを使用）。これを拒否するファイルシステム（`tmpfs` 等）ではバッファ付き I/O で実行され、`setup.direct_io` が `false` になります。
  - **確実なファイル削除とデータ特性**: ファイルはどの終了経路でも必ず削除されます（Windows は `FILE_FLAG_DELETE_ON_CLOSE` でオープンし、Linux はオープン直後に `unlink` するため、強制終了されても残骸が残りません）。書き込みデータは非圧縮であり、4 KiB セクタごとに内容が異なります。
- **`cpu-stream`**: 2 GiB のマージンを加えた配列サイズが使用可能メモリを超える場合は拒否されます。
- **`kairo bench serve`**: 1 コネクションにつき 1 テスト分のエコー・シンク（受信）・ソース（送信）を実行します（同時接続数は最大 16、各接続は `--max-conn-duration`（デフォルト `1h`）でクローズ）。コネクションごとに 1 行の JSON を出力し、接続がない状態が `--idle-timeout`（デフォルト `5m`）続くと終了します。接続してきた任意のクライアントに応答するため、信頼できるネットワーク上で待ち受けてください。

### GPU ベンチマーク

`bench/gpu/kairo-bench-gpu.cu`（CUDA C++、cuBLAS、NVML）は、`compute_86` PTX フォールバック付きの Ampere アーキテクチャ（`sm_86`、RTX 3080 / 3060）向けのプログラムです。ベンチマークではコンテナの中でビルドされます（上記）。手でビルドする場合は次のとおりです。

```text
cmd /c bench\gpu\build-windows.cmd     # CUDA 13.x + MSVC 2022 x64（vswhere 経由で vcvars64 を検出）
make -C bench/gpu                      # Linux、CUDA_HOME=/usr/local/cuda、CUBLAS_STATIC=1 で cuBLAS を静的リンク
```

どちらのビルドでもリビジョンが埋め込まれます（`BUILD_REV=...` で上書き可能）。`cudart` は静的リンクされ、NVML は実行時に動的ロードされます（System32 の `nvml.dll`、`libnvidia-ml.so.1`）。NVML は任意であり、存在しない場合はすべての `nvml` 値が `null` になります。

Windows では cuBLAS は遅延ロード（delay-load）されます。`gemm` と `gemv` には `PATH` 上または実行ファイルと同じディレクトリに `cublas64_13.dll` と `cublasLt64_13.dll` が必要です（見つからない場合はクリーンに `error` 終了します）。`copy` と `pcie` は cuBLAS を必要としません（`cublas_version` は `null` になります）。Linux では、静的リンクしない限り `--help` の実行にも共有ライブラリの cuBLAS が必要です。ドライバはビルドに使用した CUDA ツールキットをサポートしている必要があります。

- **デバイス指定**: `--device GPU-UUID`（必須、`nvidia-smi -L` が出力する形式）を、可視デバイスの UUID 一覧と照合します（`CUDA_VISIBLE_DEVICES` の値にかかわらず可視デバイスに対して照合）。一致するデバイスがない場合は、可視な UUID 一覧を出力して拒否されます。
- **フラグ**:
  - 共通: `--seconds`、`--interval`、`--sample-ms`（NVML サンプリング周期、デフォルト `100`）、`--reserve-mib`（デフォルト `512`: 空きとして残すデバイスメモリ容量）
  - ワークロード固有: `--n`（`gemm`、`gemv` 用: 64 の倍数、`256..32768`）、`--bytes`（`copy` 用: 2 進接尾辞対応、2 MiB の倍数）、`--batch 1|8`（`gemv` 用）、`--dir h2d|d2h` および `--mode bandwidth|latency`（`pcie` 用）
  - 対象ワークロードに適用されないフラグは使い方エラーになります。
  - `--help`、`--version`（JSON: `build`、ワークロードのバージョン）、および `--validate-only`（有効なパラメータを 1 行の `"type":"params"` として出力）は、CUDA・cuBLAS・NVML の呼び出しを一切行う前に終了します。
- **メモリ事前チェック**: メモリは確保を試すのではなく事前に検証します。コンテキスト作成前に、NVML の空きメモリが「バッファ容量 + reserve 容量 + コンテキスト推定分 400 MiB」を収容できる必要があります。コンテキスト作成後は、`cudaMemGetInfo` が「バッファ容量 + reserve 容量」を収容でき、さらにバッファ確保後の空きメモリにも依然として reserve 容量が残っている必要があります。いずれかを満たさない場合、実行は拒否されます。
- **入力データとバッチ実行**: 入力データはデバイス上で `[-1, 1)` の範囲の固定疑似乱数パターンで初期化されます（データ値によって消費電力が変化するため）。処理は約 20 ms にキャリブレーションされたバッチ単位で、2 バッチを同時に in-flight にして発行し、CUDA イベントで挟んで計測します。バッチはその終了が観測されたインターバルに計上され、`device_busy_ns` は CUDA イベント時間の合計となります。`pcie` の `latency` モードでは、4 KiB 転送を 1 件ずつ発行して同期し（スピンウェイト、`setup.sync` は `spin`、他はブロッキング待機）、ホスト側で計測したレイテンシとその合計（`device_busy_ns`）を報告します。
- **`device`（`kind`: `gpu`）**: `uuid`、`name`、`pci_bus_id`、`compute_capability`、`sm_count`、`memory_total_bytes`、`memory_free_bytes`、`driver_version`、`cuda_driver_version`、`cuda_runtime_version`、`cublas_version`、`nvml_available`、`power_limit_mw`（現在強制されている上限値（enforced））、`applications_clock_mhz`、`max_clock_mhz`（それぞれ `sm`、`mem`、デバイスが報告する値）、`vbios_version`。メインスレッドがすべてのインターバル境界でエネルギーカウンタを読み取るため、`energy_mj` はそのインターバルの処理ウィンドウと厳密に一致します。
- **シグナルによる停止**: `SIGTERM`、`SIGINT`、`CTRL_C`、`CTRL_BREAK`、`CTRL_CLOSE` のいずれでもクリーンに停止します（終了コード `0`）。非常に時間のかかる 1 操作（例: `n=32768` の `gemm` は約 2 秒）では、停止まで最大 2 操作分の遅延が生じることがあります。

## データベース

デーモンの状態は単一の PostgreSQL データベースに保存されます（PostgreSQL 17。LAN 上の別ホストでも構いません）。接続先はデーモン設定で指定します。

```toml
database_url = "postgres://kairo@host-b:5432/kairo?sslmode=require"
database_password_file = "local/kairo-db-password"   # 任意: ファイルの 1 行目をパスワードとして使用
```

`database_url` には `postgres://` 形式の URL または `key=value` 形式の接続文字列を指定します。代わりに環境変数 `KAIRO_DATABASE_URL` で指定することもできます（両方同時には指定できません）。これにより、設定ファイル内にパスワードを記述せずに済みます。どちらにもパスワードが含まれていない場合は、まず `database_password_file` が参照され、それでも得られない場合は通常の `PGPASSWORD` および `.pgpass` が使用されます。ログに出力される URL ではパスワード部分がマスクされます。

データベースは UTF-8 エンコーディングかつ `C` 照合順序（collation）である必要があります。時刻や識別子をテキストとしてバイト単位で比較するためであり、デーモンはそれ以外の照合順序を拒否します。初回のみ、空のデータベースを次のように作成してください。

```sql
CREATE ROLE kairo LOGIN PASSWORD '...';
CREATE DATABASE kairo OWNER kairo TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C';
```

初回の `kairo serve` 起動時にスキーマが作成されます。以降の起動では `schema_version` を参照して未適用の番号付きマイグレーションを適用します。Kairo のスキーマを持たない既存テーブルが存在するデータベースや、自身のバージョンより新しいスキーマを持つデータベースに対しては起動を拒否します。

トランザクションの排他制御とアクセス特性は次のとおりです。

- **書き込みトランザクションの直列化**: プランニングパス、Lease の予約、起動、一時停止、ハートビート、観測バッチの記録、およびトークンの発行・失効を行う書き込みトランザクションは、単一のトランザクションスコープのアドバイザリロックを取得します。これにより、データベースを共有する接続数やプロセス数にかかわらず、Kairo の意思決定は常に 1 つずつ実行されます。
- **ロックを取らない読み取り・一部書き込み**: 読み取り処理（ステータス確認、一覧取得、ログ取得）はロックを取得せず並行実行されます。また、ストアレポートの記録や、トークンの最終使用時刻の 1 分ごとの更新もアドバイザリロックなしで書き込みます。
- **認証と読み取り専用接続**: 認証処理ではリクエストごとにトークンの失効を確認します。データベース接続はデフォルトで読み取り専用であり、トランザクション側で明示的に書き込み可能に設定した場合のみ書き込みが行われます。
- **ラウンドトリップとポーリング最適化**: 各 SQL 文はサーバとの 1 回のラウンドトリップになります。プランニングパスは待機中の Execution 数にかかわらず固定で約 20 回（全スコープではなく、対象の Lease と候補 Execution が属するスコープ名のみを読み取るため）、ハートビートは約 5 回のラウンドトリップです。各 Executor は 1 秒あたり 4 回ポーリングするため、アイドル状態の環境ではプランニングパスをスキップします（[アドミッション制御とプランニング](#アドミッション制御とプランニング)を参照）。

バックアップには `pg_dump` を使用してください（データベースにはトークンハッシュが保存されているため、ダンプファイルはパスワードと同等に厳重に管理してください）。

## 稼働中環境のアップグレード

デーモン単体の入れ替えは稼働中の Attempt に一切影響しません。データベースを `pg_dump` でバックアップしたうえでバイナリを差し替え、デーモンを再起動します。

エージェントを起動すると、その Executor が起動した Attempt のうち現在も動作しているものを引き継ぎ（adopt）、それ以外はロスト扱いとします（[エージェント再起動と引き継ぎ](#エージェント再起動と引き継ぎ)を参照）。そのため、shim 経由で Attempt を実行しているエージェントの入れ替えではジョブを失いません。OS ごとの手順は次のとおりです。

- **Linux**: 新しいバイナリを古いバイナリへ上書きリネームし（稼働中の shim は引き続き旧ファイルを実行し続けます）、systemd ユニットを再起動します。
- **Windows**: 実行中のバイナリは上書きできないもののリネームは可能であるため、現在のエージェントバイナリを別名に退避し、新しいバイナリを元の位置へコピーしたうえで、エージェントプロセスのみを停止します（リネーム済みのイメージを実行している shim は停止しません）。
- **shim 導入前のエージェントからの移行**: shim なしで Attempt を起動したエージェントを置き換えると、それらの Attempt は失われます。あらかじめ対象ノード上に `authorized`、`running`、または `quiescing` 状態の Attempt がなくなるまでプロジェクトを一時停止してドレインし、入れ替え完了後に再開してください。また、キュー内で待機している Execution が読み込むコード（スクリプトや editable インストールされた SDK）は、待機中に変更してはいけません。

アップグレードは **必ずデーモンを先に、エージェントを後に** 行ってください。

- **エージェントの方がデーモンより古い場合**: エージェントは宣言された `env` を渡さずに Attempt を起動してしまいます。
- **デーモンの方がエージェントより古い場合**: デーモンはエージェントからのログ障害レポート（エージェント側でログに記録されます）、電力サンプルを含む観測データ（その結果、そのノードのリソースが `stale` になります）、および引き継ぎ対象の Attempt を持つエージェントの登録（デーモンが入れ替わるまでリトライし続けます）を拒否します。
- **データベーススキーマ**: 古いデータベースを開いたデーモンは不足しているスキーママイグレーションを適用します（[データベース](#データベース)を参照）。古いバイナリは新しいスキーマを拒否するため、バックアップは必ず残しておいてください。

ベンチマークのドライバは、コンテナにマウントされた Kairo のバイナリです。ベンチマークが Task になる前の古いドライバは Command をポーリングしないため中断（suspend）に応答せず、それをプリエンプトする処理や一時停止操作は以前と同様に完了まで待機します（強制停止による終了は引き続き可能です）が、ベンチマーク自体はそのまま正常に完了します。逆に古いデーモンは中断されない Execution としてベンチマークを投入しますが、新しいドライバでも従来どおり実行できます。

### 運用スクリプト

`ops/` ディレクトリには運用者が実行するスクリプト群が収められており、稼働中の環境自体がチェックアウト内のファイルを必要とすることはありません。運用者マシン固有のファイルは、Go の規約に沿ったユーザーディレクトリ配下に配置されます（`ops/site.sh`）。

- **設定ディレクトリ**（Windows では `%APPDATA%\kairo`、それ以外では `${XDG_CONFIG_HOME:-~/.config}/kairo`）: CLI 用の `token` および `ca.pem`、`site.env`、Windows エージェントの `agent.toml`、マシン宣言
- **状態ディレクトリ**（Windows では `%LOCALAPPDATA%\kairo`、それ以外では `${XDG_STATE_HOME:-~/.local/state}/kairo`）: `builds/`、CI 用ソースツリー、`logs/`、Windows エージェントの `attempts/`

これらのパスは環境変数 `KAIRO_CONFIG_DIR` および `KAIRO_STATE_DIR` で上書きできます。`site.env`（シェル変数形式）にはサイトへの接続情報を記述します。

- `KAIRO_PVE_HOST`: デーモンおよびデータベースのコンテナが動作する Proxmox ホスト
- `KAIRO_DAEMON_VMID` / `KAIRO_DB_VMID`: デーモンおよびデータベースの VMID
- `KAIRO_NODES`: `ops/` でエージェントを入れ替える各 Linux ノード（`NODE=ssh:USER@HOST` または `NODE=pct:USER@HOST:VMID` 形式）
- `CI_*`: [Kairo 上の CI](#kairo-上の-ci) で使用する各種設定

主な運用スクリプトは次のとおりです。

- `bash ops/build.sh`: クリーンなチェックアウトの `HEAD` を Windows 向け・Linux 向けにビルドし、`builds/kairo-<sha>.exe` および `kairo-<sha>-linux` を生成します。
- `bash ops/deploy-daemon.sh SHA NAME`: まず `pg_dump` を実行し、続いてデーモンバイナリを置き換えて（旧バイナリは `kairo.before-NAME` として退避）再起動したのち、`PATH` 上の CLI を更新します。
- `[ADOPT=1] bash ops/replace-linux-agents.sh SHA NODE...`: 各 Linux ノードのエージェントを入れ替えます（旧バイナリは `bin/kairo.before-SHA` として退避）。Lease を保持しているノードは `ADOPT=1` を指定しない限り拒否されます。各ノードは 2 分以内に報告を返す必要があります。
- **Windows（管理者権限）エージェント導入・更新**: `ops/windows/install-agent.ps1 -New BUILD` は、エージェントを `%LOCALAPPDATA%\Programs\kairo\kairo-agent.exe` に、監視用スクリプト `supervise-agent.ps1` と並べてインストールし（ログは `<state>/logs`、設定は `%APPDATA%\kairo\agent.toml`）、起動時に実行される S4U タスク `kairo-agent` を登録します（ノードが Lease を保持している間は拒否されます。異なるログディレクトリで起動されたエージェントは既存の Attempt を引き継げないためです）。既存エージェントの差し替えには `ops/windows/replace-agent.ps1 -New BUILD [-Adopting]` を使用します。エージェントは対象ユーザー権限で動作するため（Attempt もそのユーザーとして実行されます）、関連ファイルもすべてユーザー所有になります。
- **Windows（管理者権限）コントローラ導入**: `ops/windows/install-controller.ps1 -New BUILD [-Config TOML] [-State JSON] [-CA PEM]` は、`SYSTEM` 権限で動作するデバイスコントローラを `%ProgramFiles%\Kairo\kairo-controller.exe` にインストールします。設定・状態・CA 証明書は `%ProgramData%\Kairo` に配置され（設定ファイルにトークンが含まれるため、`SYSTEM` と `Administrators` のみ読み取り可能）、タスク `kairo-controller` が登録されます。

### 自己バックアップ

コントロールプレーンのデータも他のデータと本質的に変わらないため、Kairo 自身が同じ仕組みでこれを保全します。デーモン設定の `control_snapshot_minutes`（デフォルト `60`、負の値で無効）ごとに、デーモンはプロジェクト `kairo`、キュー `control-snapshot`、名前 `snapshot-<UTC slot>`（例: `snapshot-20261116T0100Z`）の Task を宣言します。

この Task は `kairo control snapshot` をコンテナの中で実行します。コンテナ Executor のあるノードに属する最初の durable store を選び、そのノードのコンテナ Executor で、固定したイメージ（`python:3.12-slim` をダイジェストで固定。どのコンテナにもマウントされるエージェント自身のバイナリ `/kairo/bin/kairo` を動かすだけなので、イメージは何でもかまいません）から動かします。durable store が 1 つも宣言されていない間、または対象ノードにコンテナ Executor がない間は何も実行されません。優先度は他のあらゆるジョブより高く、CPU・RAM・GPU のいずれも予約しないため（ダンプ処理はデーモン側で行い、Task は受け取った内容を書き出すだけのため）、他ジョブに待たされることも、空きを作るために他ジョブを中断することもありません。

Task は自身の Attempt トークンを用いてデーモンからスナップショットを取得し（`GET /api/worker/control-snapshot`。このキューの Attempt のみに許可）、Run の `output` ディレクトリに `kairo-control-<time>.tar` として書き出します。Run 終了時には通常の出力と同様に pack され、すべての durable store へコピーされます。tar アーカイブの中身は次のとおりです。

| エントリ | 内容 |
|---|---|
| `SNAPSHOT.txt` | 取得日時およびデーモンのバージョン |
| `kairo.dump` | データベースのダンプ（`pg_dump -Fc`。デーモンが `pg_dump`、または設定の `pg_dump` で指定されたコマンドを `PGPASSWORD` にパスワードを渡して実行） |
| `sources/...` | コードスナップショット（`source_directory`） |
| `config/...` | デーモンの設定ファイル、TLS 証明書、および CA（ベース名で格納） |

コマンドは、ファイル全体が完全に書き出され、かつ空でないダンプを含む場合にのみファイルを保持します（それ以外の場合は失敗し、Task も失敗します）。また、`output` ディレクトリを持たない Attempt 内で実行された場合は、書き出した内容を保全できないため実行を拒否します。運用者が手動で取得する場合は、管理者トークンを使用して `kairo control snapshot --out DIR`（`GET /api/control/snapshot`）を実行します。

スナップショットはすべての durable store にコピーされるため、機密情報は一切含まれません。サーバの TLS 秘密鍵（`tls_key_file`）と `database_password_file` は決して含まれず、`database_url` 内にパスワードが書かれている場合はデーモンの設定ファイル自体も除外されます（除外した旨がデーモンのログに出力されます。パスワードは `database_password_file` に分離してください）。秘密鍵とパスワードは別途保管するか、環境再構築時に新規作成してください。

### Durable Store からの環境再構築

Kairo が保持するデータはすべて durable store 上に残ります。各コピーは `<root>/objects/<YYYYMM>/<object id>.tar.zst`（tar + zstd）として保存されており、同じ場所にファイル一覧（`.files.tsv`）とメタデータ記述（`.object.json`: 所有者や内容の説明。`kairo store catalogue ROOT` で確認可能）が置かれているため、Kairo なしでも読み取れます。

Kairo 自身のコントロールプレーンもこの中に含まれています（[自己バックアップ](#自己バックアップ)を参照）。対象となるのはプロジェクト `kairo` の最新の `kairo-control-<time>.tar` です。（この機能が組み込まれる前は、`control-backup` キューの Task が `kairo-<time>.dump` と `daemon-srv-kairo-<time>.tar.gz` を書き出しており、`kairo-control-<time>` という名前のインポートにはこれらに加えて各ノードの `/srv/kairo`、各ホストの `/etc/kairo`、および Windows ホストの設定ディレクトリが収められていました。）

データベース、デーモン、ノードのすべてが失われた状態から再構築する手順は次のとおりです。

1. **最新スナップショットの特定と展開**: ファイル一覧から該当するオブジェクトを検索し（`grep -l 'kairo-control-.*\.tar\|kairo-.*\.dump' <root>/objects/*/*.files.tsv`）、オブジェクトを展開して（`zstd -dc OBJ.tar.zst | tar -x`）中のスナップショットを取り出します。
2. **バイナリのビルドとデータベースの復元**: このリポジトリから `kairo` をビルドし、空の PostgreSQL データベースを作成したうえで（[データベース](#データベース)を参照）、`pg_restore -d kairo` でダンプを復元します。
3. **デーモンとエージェントの起動**: 設定ファイルの指定どおりに `config/` と `sources/` を戻し、別途保管しておいた TLS 秘密鍵とデータベースパスワードを配置して（あるいは新しい鍵の作成・証明書の再発行および `ALTER ROLE kairo PASSWORD ...` による新パスワードの設定を行って）`kairo serve` を起動します。続いて各ノードのエージェント設定を復元し（または新しいトークンを発行して）、エージェントを起動します。
4. **ストアの再宣言**: 以前と同じようにストアを宣言します（マシン宣言はダンプ内および Windows 設定ディレクトリの `machines.toml` にも保存されています）。復元されたレコードが示す場所に各コピーが既に存在するため、再コピーは発生しません。

復元したダンプの作成以降にデーモンが記録したインポートやコピーはダンプには含まれませんが、そのコピー自体はファイル一覧や説明ファイルとともに durable store 上に存在します。1 時間以内に各エージェントによる durable store のインベントリ処理によって自動的に取り込まれます（[データストア](#データストア)を参照: durable store のディレクトリそのものが保持内容の正となります）。ただし、当時進行中だった Run・Task・インポートは自動取り込みされないため、再度宣言してください。

## 検証とテスト

Go のテストスイートを実行するには、テスト側でデータベースを作成できる PostgreSQL 17 サーバが必要です。各テストはランダムな名前の専用データベース（照合順序 `C`）を作成し、終了時に削除します。これによりテストを並列実行でき、実行後にゴミが残りません（何らかの理由でクリーンアップできなかった `kairo_test_*` データベースは、6 時間後の別テスト実行時に削除されます）。

接続先サーバは環境変数 `KAIRO_TEST_PG` で指定します。デフォルトは下記のローカルテスト用コンテナです（`fsync` 無効化はテスト専用設定です）。サーバに到達できない場合、テストはスキップされず失敗します。

```powershell
docker run -d --name kairo-pg-test -p 127.0.0.1:55432:5432 -e POSTGRES_USER=kairo -e POSTGRES_PASSWORD=kairo-test -e POSTGRES_DB=kairo postgres:17 -c fsync=off -c full_page_writes=off -c synchronous_commit=off
$env:KAIRO_TEST_PG = "postgres://kairo:kairo-test@127.0.0.1:55432/kairo?sslmode=disable"   # デフォルト値
go test ./...
go test -race ./...
go vet ./...
uv run --project sdk/python pytest -q
git diff --check
```

### GitHub 上の CI

`main` へのすべてのプッシュおよびすべてのプルリクエストでは、実稼働環境を必要としないチェック群が GitHub ホストランナー上で自動実行されます（`.github/workflows/ci.yml`。各ジョブは `ci/check.sh` のチェックを実行します）。

- **Ubuntu（PostgreSQL 17 サービス付き、ロールはスーパーユーザーの `kairo`）**: `linux`、`linux-race`、`linux-root`
- **Windows（ランナーイメージ付属の PostgreSQL を使用）**: `windows`
- **Ubuntu**: `sdk-python`、`sdk-rust`

セルフホストランナーもシークレットも設定していないため、GitHub 側から稼働環境へ到達することはできません。リポジトリは公開されているため、GitHub ホストのランナーの実行時間は課金されません。実環境（`host-b` 上の LXC や実ハードウェア）を必要とするチェックは、デプロイ前や大規模ブランチのマージ前に Kairo 自体の上で実行します（[Kairo 上の CI](#kairo-上の-ci) を参照）。なお、GitHub からのデプロイは行いません。

### Kairo 上の CI

Kairo 自身の CI は Kairo 上で実行します。`bash ci/kairo-ci.sh [REV] [--wait]` を実行すると、指定コミットのチェック群をプロジェクト `kairo-ci`（キュー `ci`、`ci/tasks.toml.in` にコミットの短縮 SHA を埋め込んだ定義）の Task として宣言します。これにより、コードが実際に動作する稼働環境自身のノード上で、他の作業とまったく同じアドミッション制御の下でチェックが走ります。

スクリプトは対象コミットのツリー（`git archive`）をコードスナップショットとして送り、`site.env`（[運用スクリプト](#運用スクリプト)を参照）が指す CI ノードのコンテナ Executor で、チェックごとにコンテナを作って実行します。

```sh
CI_LINUX_NODE=host-b        # チェックを走らせるノード（コンテナ Executor のあるもの）
CI_LINUX_SITE=host-b        # そのノードの site ラベル
```

各 Task はスナップショット（`/kairo/src`）を自身の `scratch`（`KAIRO_SCRATCH`）にコピーし、そこから `ci/check.sh CHECK` を実行します（チェック内容もそのコミット時点のものです）。Go・`uv`・`cargo` のキャッシュはプロジェクトのキャッシュディレクトリ（`KAIRO_CACHE_DIR`）に置きます。

- `<sha>-linux`（`gofmt`、Linux / Windows 向け `go vet`、`go test`）と `<sha>-linux-race`: golang のイメージで、テスト用の PostgreSQL 17 を自分で立て（`ci/check.sh start-pg`）、非特権ユーザー（nobody）で実行します。
- `<sha>-linux-root`: Executor・プロセス・Provider のテストを `root` で実行します。
- `<sha>-sdk-python`・`<sha>-sdk-rust`: uv のイメージと Rust のイメージで、各 SDK のテストを実行します。
- Windows 向けのビルドとテスト（`windows`）は GitHub の CI だけで実行します。Kairo は Windows ネイティブの仕事を動かさないためです（Windows では wslc のコンテナで Linux の仕事を動かします）。

スケジューリングと運用上の操作は次のとおりです。

- **優先度とメモリ予約**: すべてのチェックは優先度 `-500` で実行されるため、他のすべての作業の完了を待ち、何もプリエンプトしません。各チェックは CPU と RAM を予約し、入りきらない分は順番を待ちます。
- **待機・ログ確認・再実行**: `--wait` を付けると全チェックの完了を待機し（`kairo task wait --match`）、すべて成功した場合のみ終了コード `0` で終了します。失敗内容の確認には `kairo task logs kairo-ci ci <sha>-<check>` を、個別チェックの再実行には `kairo task rerun` を使用します（宣言済みの Task は不変であるため、1 つのコミットに対する宣言は 1 回のみです）。

なお、実機の NVIDIA GPU、プロセス強制終了（process-kill）、および DDP 実行のテストは通常の単体テストではなくホスト固有の統合テストであり、コントローラによる実際のクロック・電力上限・プロセッサ設定の変更テストも同様です（コントローラの通常テストではフェイクのアクチュエータを使用します）。ベンチマークテストは極小のワークロード（`TempDir` 上の数 MiB のファイル、ループバック通信）のみを実行し、`kairo-bench-gpu` はビルド検証のみが行われます（GPU ベンチマークの実測はアイドル状態の GPU 上で実行する前提です）。ベンチマークドライバと動作点ポリシーはスクリプト化された JSON 行とフェイクのデーモン・コントローラを用いてテストされており、各種カーブ特性・定常状態・熱制限がフェイクの想定どおりに振る舞うかどうかは、実際のアイドル状態のハードウェア上でのみ確認できます。
