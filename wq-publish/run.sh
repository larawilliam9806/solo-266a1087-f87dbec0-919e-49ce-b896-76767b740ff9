#!/usr/bin/env bash
# 本地启动脚本：自动创建 venv 并安装依赖（仅 Flask），随后启动开发服务器
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv --without-pip .venv
  if [ ! -x .venv/bin/pip ]; then
    python3 - <<'PY'
import urllib.request, pathlib, subprocess, sys
p = pathlib.Path(".venv/get-pip.py")
p.write_bytes(urllib.request.urlopen("https://bootstrap.pypa.io/get-pip.py").read())
subprocess.check_call([".venv/bin/python", str(p)])
p.unlink()
PY
  fi
  .venv/bin/pip install -q flask
fi

export WQ_DATA_DIR="${WQ_DATA_DIR:-$PWD/data}"
export WQ_DOWNSTREAM_DIR="${WQ_DOWNSTREAM_DIR:-$PWD/downstream}"
export FLASK_APP=wsgi:create_app
exec .venv/bin/flask run --host 127.0.0.1 --port 5000
