#!/bin/bash

USERNAME=root
DST_IP=a.b.c.d
DST_DIR=/tmp
PASSWORD=123456

for FILE in $(ls ./); do
    expect -c "
        set timeout 300
        spawn scp -r $FILE $USERNAME@$REMOTE_IP:$DST_DIR
        expect {
            \"(yes/no)?\" {
                send \"yes\r\"
                expect \"*?assword:*\"
                exp_continue
            }
            \"*?assword:*\" {
                send \"$PASSWORD\r\"
            }
        }
        expect eof
    "
done
