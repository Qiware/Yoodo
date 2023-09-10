#!/bin/sh


start_date=$1
end_date=$2

for ((i=$start_date;i<=$end_date;i++))
do
    python3 ./potential.py $i
done
