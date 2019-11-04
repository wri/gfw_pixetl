#!/bin/bash

# Source: https://forums.aws.amazon.com/thread.jspa?threadID=289427

# This script is used for user data in the launch template of EC2 instance. Use together with an amazon-ecs-optimized AMI
# It will make sure that ephemeral storage is mounted and used for /tmp and docker directories
# Currently only mounts one ephemeral storage device
# for that is can be used with any instance type which comes with ephemeral storage

yum install -y rsync

# mount the ephemeral storage
mkfs.ext4 /dev/nvme1n1
mount -t ext4 /dev/nvme1n1 /mnt/

# make temp directory for containers usage
# should be used in the Batch job definition (MountPoints)
mkdir /mnt/tmp_ext
rsync -avPHSX /tmp/ /mnt/tmp_ext/

# modify fstab to mount /tmp on the new storage.
sed -i '$ a /mnt/tmp_ext  /tmp  none bind 0 0' /etc/fstab
mount -a

# make /tmp usable by everyone
chmod 777 /mnt/tmp_ext

service docker stop

# copy the docker directory to the ephemeral storage
rsync -avPHSX /var/lib/docker/ /mnt/docker_ext/

# set the data directory to the ephemeral storage in the config file of the docker deamon
DOCKER_CFG_FILE=/etc/docker/daemon.json
if [ ! -e "${DOCKER_CFG_FILE}" ]; then
    # need to create a non empty file for sed to work
    echo "{" > ${DOCKER_CFG_FILE}
else
    # replace the last } of the file by a ,
    sed -i s/}$/,/ ${DOCKER_CFG_FILE}
fi
sed -i '$ a "data-root": "/mnt/docker_ext/"' ${DOCKER_CFG_FILE}
sed -i '$ a }' ${DOCKER_CFG_FILE}

service docker start