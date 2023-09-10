#!/bin/sh

start_date=$1
end_date=$2
days=$3

for ((i=$start_date;i<=$end_date;i++))
do
    python3 ./evaluator.py $i $days
done
