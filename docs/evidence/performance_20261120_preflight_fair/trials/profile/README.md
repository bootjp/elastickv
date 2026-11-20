# 走査粒度を調べるための診断

8 ページ、1 MiB chunk、4 MiB scan、200 ms の候補で、preflight-only に mutex、block、CPU profile を付けた別実行。
通常比較の成功回数には含めない。
65.57 秒の全実行の待ち時間であり、preflight 区間だけの内訳ではない。
mutex の計上は75.60秒で、主にPebbleのRaft適用時の書き込みロックに集中した。
ExportVersionsのfocusから走査が待ちの主因とは確認できない。
原本は3つのprofile、集計はmutex-top.txtに保存した。
