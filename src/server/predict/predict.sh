#!/bin/sh


start_date=$1
end_date=$2

for ((i=$start_date;i<=$end_date;i++))
do
    #python3 ./main.py $i 7
    #python3 ./evaluator.py $i 7

    #python3 ./main.py $i 5
    #python3 ./evaluator.py $i 5

    #python3 ./main.py $i 3
    #python3 ./evaluator.py $i 3

    #python3 ./main.py $i 2
    #python3 ./evaluator.py $i 2

    #python3 ./main.py $i 1
    python3 ./evaluator.py $i 1
done
