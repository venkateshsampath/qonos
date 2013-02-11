#/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_DIR="$DIR/etc/qonos"
CONFIG_FILE="$CONFIG_DIR/qonos-api.conf"

if [ ! -f $CONFIG_FILE ]
then
    cp $CONFIG_FILE.sample $CONFIG_FILE
fi

$DIR/bin/qonos-api --config-file=$CONFIG_FILE