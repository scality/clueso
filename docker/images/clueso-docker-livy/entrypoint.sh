#!/bin/bash

# use simpler way to get ip address
# try just setting to 0.0.0.0
if [ -z ${HOST+x} ]; then 
# if host isn't known then check outbound IP when contacting google
  export LIBPROCESS_IP=$(ip route get 8.8.8.8 | head -1 | cut -d' ' -f8)
else
  export LIBPROCESS_IP=$HOST
fi

$LIVY_APP_PATH/bin/livy-server $@
