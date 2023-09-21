#!/bin/sh
#

stock_code=$1

echo "python3 ./main.py transaction $stock_code 1month"
python3 ./main.py transaction $stock_code 1month
