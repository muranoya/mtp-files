# MTP Files

MTP デバイス（Garmin のスマートウォッチ、Android スマートフォンなど）とファイルをやり取りする、Linux 向けのシンプルな GUI アプリです。libmtp などのライブラリや `mtp-connect` は不要で、ビルドしたバイナリだけで動きます。

## ビルドと起動

```sh
cargo build --release
./target/release/mtp-files
```

ビルドに必要なのは Rust（1.91 以上）と fontconfig の開発ファイル（Ubuntu では `libfontconfig-dev`）です。

## 使い方

左の一覧からデバイスを選ぶと、ファイルの一覧が表示されます。USB の抜き差しは自動で検出します。

| 操作 | 方法 |
|---|---|
| フォルダを開く | ダブルクリック / Enter |
| 上の階層へ戻る | パンくずをクリック / Backspace |
| 複数選択 | Ctrl+クリック / Shift+クリック / Ctrl+A |
| PC からデバイスへコピー | 「デバイスへコピー…」「フォルダをコピー…」 |
| デバイスから PC へコピー | 選択して「PC へ保存…」（フォルダは中身ごと） |
| 名前の変更 | 「名前変更」 / F2 |
| 削除 | 「削除」 / Delete（フォルダは中身ごと） |

デバイス側の同名ファイルを上書きするときは、MTP に上書きの操作がないため、古いファイルを削除してから転送します。そのため、転送に失敗すると古いファイルは元に戻りません。

## 接続できないとき

- **他のプログラムが使用中と表示される**: MTP デバイスを同時に使えるのは 1 つのプログラムだけです。ファイルマネージャーでデバイスを取り出す（アンマウントする）か、`mtp-connect` などを終了してください。
- **アクセス権がないと表示される**: udev ルールが必要です。たとえば Garmin なら `/etc/udev/rules.d/99-mtp-files.rules` に次の行を追加し、`sudo udevadm control --reload-rules` を実行してデバイスを接続し直します。

  ```
  SUBSYSTEM=="usb", ATTR{idVendor}=="091e", TAG+="uaccess"
  ```

- **一覧に表示されない**: デバイス側で USB 接続を「ファイル転送」にしてください。

## 開発

```sh
cargo test
```

テストは `mtp-rs` の仮想デバイスを使うので、実機がなくても動きます。ローカルのフォルダを仮想デバイスとして GUI を試すこともできます。

```sh
MTP_FILES_VIRTUAL_DIR=/path/to/dir cargo run --features virtual-device
```

## ライセンスに関する注意

Slint は GPLv3 または Royalty-free ライセンスで利用できます。このリポジトリのライセンスは未設定です。バイナリを配布する場合は、Royalty-free なら帰属表示（`AboutSlint` の表示またはバッジの掲載）が必要です。
