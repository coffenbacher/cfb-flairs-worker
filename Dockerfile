############################################################
# Dockerfile to build Python WSGI Application Containers
# Based on Ubuntu
############################################################

# Set the base image to Ubuntu
FROM phusion/passenger-full:latest

# File Author / Maintainer
MAINTAINER Charles Offenbacher

# Copy the application folder inside the container


# Get pip to download and install requirements:
RUN apt-get update
RUN apt-get -y install python-pip

ADD worker /worker
RUN pip install -r /worker/requirements.txt

# Expose ports
# EXPOSE 80

# Set the default directory where CMD will execute
WORKDIR /worker

# Set environment variables last minute
ENV DELAY=0
ENV RETHINKDB_HOST=""
ENV RETHINKDB_PORT=""
# Set the default command to execute    
# when creating a new container
# i.e. using CherryPy to serve the application
CMD python worker.py