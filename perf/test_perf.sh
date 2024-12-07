#!/bin/bash


worker_mode=$1
echo $worker_mode

dispatcher_url=$2

if [[ "$worker_mode" == "push" ]]; then
    workers=(1 0 1 2 4)
    jobs=(1 5 10 20 40)
    array_length=${#workers[@]}
    echo "$worker_mode"
    for ((i=0; i < ${array_length}; i++)); do
        item=${workers[$i]}
        echo $item
        for ((j=0; j < ${item}; j++)); do
            echo $dispatcher_url
            python3 push_worker.py 5 "$dispatcher_url" &    
        done
        echo ${jobs[$i]}
        python3 perf/perf.py $worker_mode ${jobs[$i]} 
    done
elif [[ "$worker_mode" == "pull" ]]; then
    workers=(1 0 0 0 0)
    jobs=(1 5 10 20 40)
    array_length=${#workers[@]}
    echo "$worker_mode"
    for ((i=0; i < ${array_length}; i++)); do
        item=${workers[$i]}
        echo $item
        for ((j=0; j < ${item}; j++)); do
            echo $dispatcher_url
            python3 pull_worker.py 5 "$dispatcher_url" &    
        done
        echo ${jobs[$i]}
        python3 perf/perf.py $worker_mode ${jobs[$i]} 
    done
else
    echo local
    jobs=(1 5 10 20)
    array_length=${#jobs[@]}
    for ((i=0; i < ${array_length}; i++)); do
        python3 perf/perf.py $worker_mode ${jobs[$i]} 
    done
fi

