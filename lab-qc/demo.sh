#!/usr/bin/env bash
# 全流程 curl 演示：首版规则 → 上传接收/拒收 → 规则替换（失败与成功）→ 状态查询。
# 用法：bash demo.sh [BASE_URL]
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:8000}"
DIR="$(cd "$(dirname "$0")" && pwd)"

post_json() {  # $1=path  $2=json body
  curl -s -X POST "${BASE_URL}$1" -H "Content-Type: application/json" -d "$2"
}

say() { printf '\n==== %s ====\n' "$1"; }

say "0. 健康检查"
curl -s "${BASE_URL}/health"; echo

say "1. 无规则时上传 → 409 拒收"
curl -s -X POST "${BASE_URL}/uploads" -F "file=@${DIR}/samples/batch_ok.csv"; echo

say "2. 提交首版规则"
post_json "/rules" '{"note":"演示首版","limits":[{"project":"pH","lower_int":6,"upper_int":9},{"project":"COD","lower_int":10,"upper_int":300}]}'; echo

say "3. 合格批次接收（batch_ok.csv，含边界值 6/9）"
curl -s -X POST "${BASE_URL}/uploads" -F "file=@${DIR}/samples/batch_ok.csv"; echo

say "4. 违规批次整批拒收（batch_bad.csv，无半批写入）"
curl -s -X POST "${BASE_URL}/uploads" -F "file=@${DIR}/samples/batch_bad.csv"; echo

say "5. 当前生效规则与批次列表"
curl -s "${BASE_URL}/rules/active"; echo
curl -s "${BASE_URL}/batches"; echo

say "6. 规则替换（过紧规则必失败，旧规则保留并列违规样品）"
RID=$(post_json "/rules/replacements" '{"note":"过紧","limits":[{"project":"pH","lower_int":6,"upper_int":7},{"project":"COD","lower_int":10,"upper_int":300}]}' | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
sleep 1
curl -s "${BASE_URL}/rules/replacements/${RID}"; echo

say "7. 规则替换成功（兼容全部历史，新增 NH3N 项目）"
RID2=$(post_json "/rules/replacements" '{"note":"新增氨氮","limits":[{"project":"pH","lower_int":6,"upper_int":9},{"project":"COD","lower_int":10,"upper_int":300},{"project":"NH3N","lower_int":0,"upper_int":50}]}' | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
sleep 1
curl -s "${BASE_URL}/rules/replacements/${RID2}"; echo
curl -s "${BASE_URL}/rules/active"; echo

say "8. 新规则同样校验上传（缺规则项目仍会整批拒收）"
printf '样品号,项目,读数\nD1,NH3N,30\nD2,NH3N,99\n' > /tmp/lab_qc_demo.csv
curl -s -X POST "${BASE_URL}/uploads" -F "file=@/tmp/lab_qc_demo.csv"; echo

say "完成。可访问 ${BASE_URL}/docs 继续交互操作"
