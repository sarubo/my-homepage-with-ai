# FileShare Server

F# / ASP.NET Core Minimal API で作られた、家庭内ネットワーク用のファイル共有サーバーです。UIはサーバーから直接配信し、Node.jsや外部CDNを必要としません。

## 実行

`.NET 10.0.11` Runtime が必要です。

```bash
dotnet run --project FileShareServer.fsproj -p:MSBuildEnableWorkloadResolver=false -- --ip 192.168.1.10 --port 5080
```

`--ip` と `--port` は必須です。IPv4、IPv6、`0.0.0.0`、`::` を指定できます。起動完了時には、アクセス用の index URL をコンソールに一度だけ表示します。ワイルドカードアドレスを指定した場合は、検出したローカルアドレスを表示します。

現在の開発環境では .NET SDK のworkload manifestが欠けているため、上記の `-p:MSBuildEnableWorkloadResolver=false` を付けています。`dotnet workload repair` でSDKを修復した環境では、この指定は不要です。

## 挙動

- `/` はページ一覧、`/app/hello_world` は `hello_world`、`/app/fileshare` は共有画面です。
- ファイルはRAMだけに保存され、サーバーの停止・再起動時に失われます。
- 1ファイルと保存済みファイル全体の論理上限は、それぞれ4 GiBです。上限超過またはRAM不足時は理由をコンソールへ出し、プロセスを停止します。
- 同名のアップロードは、アップロード完了後に既存ファイルを上書きします。
- 拡張子は制限しません。選択削除には確認・取り消し機能はありません。
- OpenAPI 3.0 JSON は `/openapi/v1.json`、ブラウザ用のAPI参照は `/docs`（`/swagger` からも移動）です。

保存機構は `IFileStore` で抽象化されています。現行の `InMemoryFileStore` は4 MiBチャンクでファイルを保持するため、CLRの単一配列サイズ制約を越える4 GiBファイルにも対応できます。将来はこのインターフェースを実装して、ファイルシステムやSQLiteへ置き換えられます。

## 単一ファイルとして発行

単一ファイルはOSごとに発行します。どちらも対応する .NET 10.0.11 Runtime を持つ環境で実行できます。

```bash
dotnet publish FileShareServer.fsproj -c Release -r linux-x64 --self-contained false -p:MSBuildEnableWorkloadResolver=false -p:PublishSingleFile=true -o publish/linux-x64
dotnet publish FileShareServer.fsproj -c Release -r win-x64 --self-contained false -p:MSBuildEnableWorkloadResolver=false -p:PublishSingleFile=true -o publish/win-x64
```
