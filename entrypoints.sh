#!/bin/bash

function help() {
	echo "Usage: $0 [option...] {test|}

Options:
  -h, --help            this message
  -p, --path            path to spec test file"
}

POSITIONAL=()

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -h|--help)
    shift # past argument
	help
	exit 0
    ;;
    -p|--path)
	if [[ -e $2 ]]; then
    	TARGET_PATH="$2"
	else
		>&2 echo "ValueError: '${2}' path is not exist"
		exit 1
	fi
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done

set -- "${POSITIONAL[@]}"

case $1 in
	test)
	if [[ -n $TARGET_PATH ]]; then
		python -m unittest discover -p "${TARGET_PATH}"
	else
		python -m unittest
	fi
	;;
    *)
	help >&2
esac
