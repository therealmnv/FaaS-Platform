#!/bin/bash


worker_mode=$1
echo $worker_mode

port=$2
url="127.0.0.1"

# You need to open up uvicorn main:app !

if [[ "$worker_mode" == "push" ]]; then
    workers=(1 0 1 2 4)
    jobs=(1 5 10 20 40)
    array_length=${#workers[@]}
    echo "$worker_mode"
    for ((i=0; i < ${array_length}; i++)); do
        item=${workers[$i]}
        echo $item
        for ((j=0; j < ${item}; j++)); do
            echo ${url}:${port}
            python3 push_worker.py -w 5 -d ${url}:${port} &    
        done
        echo ${jobs[$i]}
        python3 perf/perf.py $worker_mode ${jobs[$i]} 
    done
elif [[ "$worker_mode" == "pull" ]]; then
    workers=(1 0 0)
    jobs=(1 5 10)
    array_length=${#workers[@]}
    echo "$worker_mode"
    for ((i=0; i < ${array_length}; i++)); do
        item=${workers[$i]}
        echo $item
        for ((j=0; j < ${item}; j++)); do
            echo ${url}:${port}
            python3 pull_worker.py -w 5 -d ${url}:${port} &    
        done
        echo ${jobs[$i]}
        python3 perf/perf.py $worker_mode ${jobs[$i]} 
    done
    # python3 task_dispatcher -m $worker_mode -p $port
    # python3 pull_worker.py -w 4 -d ${url}:${port} &
else
    echo local
    jobs=(1 5 10 20)
    array_length=${#jobs[@]}
    for ((i=0; i < ${array_length}; i++)); do
        python3 perf/perf.py $worker_mode ${jobs[$i]} 
    done
fi

