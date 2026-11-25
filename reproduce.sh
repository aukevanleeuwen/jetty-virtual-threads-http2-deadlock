#!/usr/bin/env bash
#
# Starts the application twice, with Spring Boot's defaults and with the
# workaround, runs trigger/TriggerDeadlock.java against each and summarises a
# thread dump taken at the end. Needs a JDK 25 (java, jcmd) on the PATH.
#
#   ./reproduce.sh [give-up|send-rest] [port]
#
# give-up:   the client gives up on the uploads when their timeout expires.
# send-rest: the client sends the rest of the bodies.
set -uo pipefail

cd "$(dirname "$0")"
ending=${1:-give-up}
port=${2:-18080}
logs=build/reproduce
mkdir -p "$logs"

./gradlew -q bootJar
jar=$(ls build/libs/*.jar | grep -v plain | head -1)

run() {
    local label=$1 workaround=$2
    echo
    echo "=================== $label (reproducer.workaround=$workaround, client: $ending)"

    java -jar "$jar" --server.port="$port" --reproducer.workaround="$workaround" > "$logs/$label.log" 2>&1 &
    local pid=$!
    for _ in $(seq 1 60); do
        curl -s -o /dev/null "http://localhost:$port/hello" 2> /dev/null && break
        sleep 0.5
    done

    java trigger/TriggerDeadlock.java "http://localhost:$port" "$ending"
    local result=$?

    jcmd "$pid" Thread.dump_to_file -format=text -overwrite "$PWD/$logs/$label-threads.txt" > /dev/null
    echo "thread dump at the end ($logs/$label-threads.txt):"
    echo "  $(grep -c 'BlockingContentProducer.nextChunk' "$logs/$label-threads.txt") handlers blocked reading a request body"
    echo "  $(grep -c 'java.util.concurrent.Semaphore.acquire' "$logs/$label-threads.txt") tasks waiting for a permit of Spring Boot's VirtualThreadPool"

    # A hung server does not shut down either, so do not wait for that.
    kill -9 "$pid"
    wait "$pid" 2> /dev/null
    return $result
}

run spring-boot-defaults false
defaults=$?
run workaround true
workaround=$?

echo
echo "spring boot defaults: $([[ $defaults -eq 0 ]] && echo 'answers' || echo 'HANGS')"
echo "workaround:           $([[ $workaround -eq 0 ]] && echo 'answers' || echo 'HANGS')"
