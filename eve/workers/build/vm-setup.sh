#!/bin/bash
set -e -x -o pipefail

### Dependencies 
curl https://dl.yarnpkg.com/rpm/yarn.repo -o /etc/yum.repos.d/yarn.repo
yum update -y
yum group install "Development Tools" -y
yum install https://centos7.iuscommunity.org/ius-release.rpm -y
yum install wget java-1.8.0-openjdk java-1.8.0-openjdk-devel python27u \
	    python27u-pip python36u python36u-pip git htop tmux zsh nodejs \
	    R yarn -y
 
curl -fsSL get.docker.com | sh -
systemctl start docker
sudo pip install docker-compose
sudo usermod -aG docker eve
## reload user session for usermod to take effect
exec sg docker newgrp `id -gn`

echo -e "Host github.com\n\tStrictHostKeyChecking no\n" >> ~/.ssh/config

