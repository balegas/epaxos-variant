#!/usr/bin/env bash

LOGS=logs

NSERVERS=5
NCLIENTS=1
CMDS=300
PSIZE=32
TOTAL_OPS=$(( NCLIENTS * CMDS ))
failure=0

MASTER=bin/master
SERVER=bin/server
CLIENT=bin/client

#HACKS
mkdir -p logs
if [[ $OSTYPE == 'darwin17' ]];
then
    PS_AUX="ps aux"
else
    PS_AUX="ps -aux"
fi

master() {
    touch ${LOGS}/m.txt
    ${MASTER} -N ${NSERVERS} > "${LOGS}/m.txt" 2>&1 &
    tail -f ${LOGS}/m.txt &
}

servers() {
    echo ">>>>> Starting servers..."
    for i in $(seq 1 ${NSERVERS}); do
	port=$(( 7000 + $i ))
	${SERVER}\
	    -lread \
	    -exec \
	    -thrifty \
	    -o \
	    -port ${port} > "${LOGS}/s_$i.txt" 2>&1 &
    done

    up=-1
    while [ ${up} != ${NSERVERS} ]; do
	up=$(cat logs/s_*.txt | grep "Waiting for client connections" | wc -l)
	sleep 1
    done
    echo ">>>>> Servers up!"
}

clients() {
    echo ">>>>> Starting clients..."
    for i in $(seq 1 $NCLIENTS); do
	${CLIENT} -v \
		  -q ${CMDS} \
		  -w 100 \
		  -c 50 \
		  -l \
		  -psize ${PSIZE} > "${LOGS}/c_$i.txt" 2>&1 &
    done

    ended=-1
    while [ ${ended} != ${NCLIENTS} ]; do
	ended=$(tail -n 1 logs/c_*.txt  | grep "Test took" | wc -l)
	sleep 1
	if (( ${failure} > 0 ));
	then
	    sleep 20
	    leader=$(grep "new leader" ${LOGS}/m.txt | tail -n 1 | awk '{print $4}')
	    port=$(grep "node ${leader}" ${LOGS}/m.txt | sed -n 's/.*\(:.*\]\).*/\1/p' | sed 's/[]:]//g')
	    pid=$(ps -ef | grep "bin/server" | grep "${port}" | awk '{print $2}')
	    echo ">>>>> Injecting failure... (${leader}, ${port}, ${pid})"
	    kill -9 ${pid}
	    failure=$((failure - 1))
	else
	    sleep 20
	fi
    done
    echo ">>>>> Client ended!"
}

stop_all() {
    echo ">>>>> Stopping All"
    for p in ${CLIENT} ${SERVER} ${MASTER}; do
	$PS_AUX | grep ${p} | awk '{ print $2 }' | xargs kill -9 >& /dev/null
    done
    $PS_AUX | grep "tail -f ${LOGS}/m.txt" | awk '{ print $2 }' | xargs kill -9 >& /dev/null
    true
}

start_exp() {
    rm -rf ${LOGS}/*
    stop_all
}

end_exp() {
    all=()
    for i in $(seq 1 ${NSERVERS}); do
        f="${LOGS}/$i.ops"
        cat "${LOGS}/s_$i.txt" | grep "Executing" | cut -d',' -f 2 | cut -d' ' -f 2 > ${f}
        all+=(${f})
    done

}

trap "stop_all; exit 255" SIGINT SIGTERM

start_exp
master
servers
clients
end_exp

stop_all
