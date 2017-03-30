# This dockerfile uses the ubuntu image
# VERSIOn 2 - EDITION 1
# AUthor: docker_user
# Command format: Instruction [arguments / command] ..

# 第一行必须指定基于的基础镜像
FROM ubuntu

# 维护者信息
MAINTAINER docker_user docker_user@email.com

# 镜像的操作指令
RUN echo "deb http:///archive.ubuntu.com/ubuntu raring main universe" >> /etc/apt/sources.list
RUN apt-get update && apt-get install -y nginx
RUN echo "\ndaemon off;" >> /etc/nginx/nginx.conf

# 容器启动时执行指令
CMD /usr/sbin/nginx
