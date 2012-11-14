#/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_DIR="$DIR/etc"

$DIR/bin/qonos-api --config-dir=$CONFIG_DIR