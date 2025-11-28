#!/bin/bash

# crontab -e
# 41 5 * * * /bin/bash ${CRONTAB_DIR}/coupang_login_advertising.sh $1 >> ${CRONTAB_DIR}/output.log 2>&1

CRONTAB_DIR="$(dirname "$0")"
ROOT_DIR="$(dirname "$CRONTAB_DIR")"
CREDENTIALS_FILE="$1"

source ${ROOT_DIR}/.venv/bin/activate
python ${CRONTAB_DIR}/coupang_login.py \
    --credentials "$ROOT_DIR/src/env/$CREDENTIALS_FILE" \
    --domain "advertising" \
    --chdir "$ROOT_DIR/src" \
    --logfile "$ROOT_DIR/src/var/coupang_login.log"
