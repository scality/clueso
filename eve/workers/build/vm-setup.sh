#!/bin/bash
set -e -x -o pipefail

### Dependencies 
yum update -y
yum group install "Development Tools" -y
yum install https://centos7.iuscommunity.org/ius-release.rpm -y
yum install curl java-1.8.0-openjdk java-1.8.0-openjdk-devel \
	    python27u python27u-pip git htop -y
 
curl -fsSL get.docker.com | sh -
systemctl start docker
sudo pip install docker-compose
sudo usermod -aG docker eve
## reload user session for usermod to take effect
exec sg docker newgrp `id -gn`

echo -e "Host github.com\n\tStrictHostKeyChecking no\n" >> ~/.ssh/config

