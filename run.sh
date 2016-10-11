#!/bin/bash
if [ $# -lt 2 ] || [ "$1" == "-h" ]; then
    echo "Usage $0 inputFile outputFile [temporaryFolder]"
    echo "If temporaryFolder not provided, a tmp folder will be created and used within outputFile"
    exit 1
fi
date
INPUT_FILE="$1"
OUTPUT_FILE="$2"
TMP_FOLDER="$3"
time java -Xmx4000M -cp target/classes/ org.dpinol.BigFileSorter "${INPUT_FILE}" "${OUTPUT_FILE}" "${TMP_FOLDER}"
date
