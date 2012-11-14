#/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_DIR="$DIR/etc"

$DIR/bin/chronos-api --config-dir=$CONFIG_DIR