# Data engineering case study and pipeline flow for the data from the NYC Taxi Trips dataset

# Overview AWS Solution
How to leverage Amazon S3, Amazon Glue, Amazon Athena and Amazon QuickSight, automate the creation of a data catalog, 
transform data to a compressed columnar format that allows to speed up and reduce the cost of query/analysis

This is a serverless furthermore without worry about or having to manage clusters or having to spin up a single instance.

# Services in use:

- [AWS S3](https://aws.amazon.com/s3/) for data storage
- [AWS Glue](https://aws.amazon.com/pt/glue/) for data catalog automation and ETL from S3 with csv and json files provided
- [AWS Athena](https://aws.amazon.com/pt/athena/) Blazing fast mult propose and serverless query engine base on Apache/Facebook Presto
- [Amazon QuickSight](https://aws.amazon.com/quicksight/) for data visualization


## Data Used

[Taxi Trips data in New York City 2009](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2009-json_corrigido.json) 
[Taxi Trips data in New York City 2010](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2010-json_corrigido.json)
[Taxi Trips data in New York City 2011](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2011-json_corrigido.json)
[Taxi Trips data in New York City 2012](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2012-json_corrigido.json) 
[Vendor Loockup](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2012-json_corrigido.json) 
[Payment Loockup](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2012-json_corrigido.json) 


# Walkthrough for reproduce the analysis

### [1- Upload a dataset to Amazon S3](step-one.md)
### [2- Create a Data Catalog and squemas for each dataset in S3 by AWS Glue](step-two.md)
### [3- Transform data in tables using Apache Parquet, a compressed columnar format](step-three.md)

## [Analysis Review: For each question the related solution with explanations and evidences](analysis.html)

- [Architecture overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Deployment of the experiment](#deployment-of-the-experiment)
  - [Downloading necessary data](#downloading-necessary-data)
  - [Building the CDK application](#building-the-cdk-application)
  - [Deploying the stack and upload the data](#deploying-the-stack-and-upload-the-data)
  - [Deploying Amazon QuickSight](#deploying-amazon-quicksight)
    - [Preparing the Manifest file](#preparing-the-manifest-file)
    - [Signing\-up](#signing-up)
    - [Creating a dataset](#creating-a-dataset)
- [Exploring the demo](#exploring-the-demo)
  - [Launching the experiment](#launching-the-experiment)
    - [Launching the producer](#launching-the-producer)
    - [Launching the Kinesis Data Analytics Application](#launching-the-kinesis-data-analytics-application)
  - [Producer](#producer)
    - [Architecture overview of the producer layer](#architecture-overview-of-the-producer-layer)
    - [Lambda function](#lambda-function)
    - [AWS Fargate](#aws-fargate)
  - [Ingestion](#ingestion)
    - [Architecture overview of the ingestion layer](#architecture-overview-of-the-ingestion-layer)
    - [Kinesis Data Firehose](#kinesis-data-firehose)
    - [S3](#s3)
  - [Enhancement](#enhancement)
    - [Architecture overview of the enhancement layer](#architecture-overview-of-the-enhancement-layer)
    - [S3 for referential data](#s3-for-referential-data)
    - [Kinesis Data analytics SQL application](#kinesis-data-analytics-sql-application)
    - [AWS Lambda as a destination for a kinesis data analytics](#aws-lambda-as-a-destination-for-a-kinesis-data-analytics)
  - [Visualization](#visualization)
    - [Architecture overview of the visualization layer](#architecture-overview-of-the-visualization-layer)
    - [CloudWatch](#cloudwatch)
    - [QuickSight](#quicksight)
- [Cost](#cost)
- [Solutions alternatives](#solutions-alternatives)
- [Develop](#develop)
  - [Start watching for changes](#start-watching-for-changes)
  - [Useful commands](#useful-commands)
- [Clean up](#clean-up)
- [Inspiring source of information](#inspiring-source-of-information)

## Architecture overview

![Architecture](screenshots/arch.png)

1. App download and datasets form URL intercafes for S3 files
2. App upload files into Amazon S3
3. ETL and Data Catalog: Amazon Glue 
    - Crawlers read csv and json files, generate schemas and create data catalogs for each dataset
    - PySpark jobs in Glue tranform data into parquet format by extrnal table creation 
    - Pyspark jobs in Glue merge trips files/datasets in one table
4. Data discover and quering by Amazon Athena based on database and tables creted by Glue  
5. Visualization:
    - Amazon Quick Sights allows reporting on datasets directly with custom queries in Athena databases.


# Directory Structure of this solution

![Structure](screenshots/dir.png)


# References:
Installing the AWS CLI
https://docs.aws.amazon.com/cli/latest/userguide/installing.html

Configuring the AWS CLI
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html

AWS Glue Built-in classifiers
https://docs.aws.amazon.com/glue/latest/dg/add-classifier.html#classifier-built-in

Parquet data format
https://parquet.apache.org/documentation/latest/

Setting up QuickSight
https://docs.aws.amazon.com/quicksight/latest/user/signing-in.html
