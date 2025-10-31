#!/bin/bash

CRONTAB_DIR="$(dirname "$0")"
SRC_PROJ_DIR="$(dirname "$CRONTAB_DIR")/src"
CREDENTIALS_FILE="$1"

# cd ${CRONTAB_DIR}/env
# python -m venv playwright
# source playwright/bin/activate
# pip install playwright
# pip install ruamel.yaml

# crontab -e
# 20 9 * * * /bin/bash ${CRONTAB_DIR}/coupang_login_wing.sh $1 >> ${CRONTAB_DIR}/output.log 2>&1

source ${CRONTAB_DIR}/env/playwright/bin/activate
python ${CRONTAB_DIR}/coupang_login.py \
    --credentials ${SRC_PROJ_DIR}/env/${CREDENTIALS_FILE} \
    --playwright ws://172.28.0.50:3000/ \
    --domain wing \
    --chdir ${SRC_PROJ_DIR} \
    --logfile ${SRC_PROJ_DIR}/var/coupang_login.log
