############################################################
# Dockerfile to build Python WSGI Application Containers
# Based on Ubuntu
############################################################

# Set the base image to Ubuntu
FROM ubuntu

# File Author / Maintainer
MAINTAINER Charles Offenbacher

# Add the application resources URL
RUN echo "deb http://archive.ubuntu.com/ubuntu/ $(lsb_release -sc) main universe" >> /etc/apt/sources.list

# Update the sources list
RUN apt-get update

# Install basic applications
RUN apt-get install -y tar git curl nano wget dialog net-tools build-essential

# Install Python and Basic Python Tools
RUN apt-get install -y python python-dev python-distribute python-pip zlib1g-dev libpq-dev
# python-lxml


# Copy the application folder inside the container
ADD worker /worker

# Get pip to download and install requirements:
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