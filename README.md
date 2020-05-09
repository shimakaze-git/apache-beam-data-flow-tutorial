# apache-beam-data-flow-tutorial
apache beam data flow tutorial

# Apache Beam SDK 入門

### Pipeline

処理タスク全体（パイプライン）をカプセル化します。処理タスクには、入力データの読み取り、変換処理、および出力データの書き込み等が含まれます。

### PCollection

分散処理対象のデータセットを表すオブジェクトです。通常は、外部のデータソースからデータを読み取り、PCollection を作成しますが、インメモリから作成することも可能です。

### Transfor

データ変換処理の機能を提供します。すべての Transform は、1つ以上の PCollection を入力として受け取り、その PCollection の要素に対して何らかの処理を実行して、0個以上の PCollection を出力します。

### I/O Transform

様々な外部ストレージシステム（GCS や BigQuery など）に対してデータの読み書きができる機能（Read/Write Transform）を提供しています。
