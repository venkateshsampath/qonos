#/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_FILE="$DIR/etc/qonos/qonos-api.conf"

$DIR/bin/qonos-api --config-file=$CONFIG_FILE