# Data engineering case study and pipeline flow 

Docker image for **python3**.

Included some libraries for dealing with http requests, google cloud SDK, mainly google cloud storage, bigquery, dataflow:

* requests: 2.22.0


#  How build this application

In the root directory of this project you can run the following command:

**docker build -t imgname -f Dockerfile . --no-cache**

This will start a docker build.

#  How run this application

**docker run imgname:latest**