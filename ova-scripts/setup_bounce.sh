#!/bin/bash

set -e

BASE_VM=bounce-base
BOUNCE_IMAGE=bounce:latest
BOUNCE_KEY=bounce.rsa
BOUNCE_USER=bounce
BUILD_VM=bounce-build
SSH_PORT=2222
VBOX=VBoxManage
IP_QUERY="$VBOX guestproperty get $BUILD_VM /VirtualBox/GuestInfo/Net/0/V4/IP"

SSH="ssh -p $SSH_PORT -i $BOUNCE_KEY ${BOUNCE_USER}@localhost"

clone_vm() {
	$VBOX clonevm $BASE_VM --name $BUILD_VM --register
	$VBOX modifyvm $BUILD_VM --natpf1 "bounce-ssh,tcp,127.0.0.1,${SSH_PORT},,22"
}

start_vm() {
	$VBOX startvm $BUILD_VM
	while [ "`$IP_QUERY`" = "No value set!" ]; do
		sleep 1
	done
	echo "Started ${BUILD_VM}..."
}

setup_bounce() {
	archive=/tmp/bounce-image.tar
	echo "Exporting the $BOUNCE_IMAGE image..."
	docker save -o $archive $BOUNCE_IMAGE
	scp -P $SSH_PORT -i $BOUNCE_KEY $archive ${BOUNCE_USER}@localhost:/tmp/
	$SSH "docker load < $archive"
	docker run --net=host -d $BOUNCE_IMAGE
}

export_ova() {
	echo "Shutting down ${BUILD_VM}..."
	$SSH "sudo poweroff"
	while [ -n "`$VBOX list runningvms|grep $BUILD_VM`" ]; do
		sleep 1
	done
	echo "Exporting ${BUILD_VM}..."
	$VBOX export $BUILD_VM -o /tmp/${BUILD_VM}.ova
	$VBOX unregistermvm $BUILD_VM --delete
}

clone_vm
start_vm
setup_bounce
start_bounce
export_ova
