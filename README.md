# Data engineering case study and pipeline flow for the data from the NYC Taxi Trips dataset

# Overview AWS Solution
How to leverage Amazon S3, Amazon Glue, Amazon Athena and Amazon QuickSight, automate the creation of a data catalog, 
transform data to a compressed columnar format that allows to speed up and reduce the cost of query/analysis

This is a serverless furthermore without worry about or having to manage clusters or having to spin up a single instance.

Also all dev environment, shell and text editor used are serverless too thanks Amazon Cloud9 solution.

# Services in use:

- [AWS S3](https://aws.amazon.com/s3/) for data storage
- [AWS Glue](https://aws.amazon.com/pt/glue/) for data catalog automation and ETL from S3 with csv and json files provided
- [AWS Athena](https://aws.amazon.com/pt/athena/) Blazing fast mult propose and serverless query engine base on Apache/Facebook Presto
- [Amazon QuickSight](https://aws.amazon.com/quicksight/) for data visualization


## Data Used

- [Taxi Trips data in New York City 2009](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2009-json_corrigido.json) 
- [Taxi Trips data in New York City 2010](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2010-json_corrigido.json)
- [Taxi Trips data in New York City 2011](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2011-json_corrigido.json)
- [Taxi Trips data in New York City 2012](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2012-json_corrigido.json) 
- [Vendor Loockup](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2012-json_corrigido.json)
- [Payment Loockup](https://s3.amazonaws.com/data-sprints-eng-test/data-sample_data-nyctaxi-trips-2012-json_corrigido.json) 

# [Architecture overview](#architecture-overview)
# [Prerequisites](#prerequisites)
# [Walkthrough to reproduce the analysis](#the-setup)

## Architecture overview

![Architecture](screenshots/arch.png)

## Prerequisites

For this experiment, you will need the following:

- The [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
- An AWS account. [here](https://portal.aws.amazon.com/billing/signup/iam).

## The Setup 

### Main steps

### 1 - Upload a dataset to Amazon S3
### 2 - Create a Data Catalog and squemas for each dataset in S3 by AWS Glue
### 3 - Transform data in tables using Apache Parquet, a compressed columnar format

### Actions to take to anwers the questions (detailed):

1. App download and datasets form URL intercafes for S3 files
2. App upload files into Amazon S3
3. ETL and Data Catalog: Amazon Glue 
    - Crawlers read csv and json files, generate schemas and create data catalogs for each dataset
    - PySpark jobs in Glue tranform data into parquet format by extrnal table creation 
    - Pyspark jobs in Glue merge trips files/datasets in one table
4. Data discover and quering by Amazon Athena based on database and tables creted by Glue  
5. Visualization:
    - Amazon Quick Sights allows reporting on datasets directly with custom queries in Athena databases.


## 1 - Upload a dataset to Amazon S3

```bash
python3 app.py
```

### 2 - Create a Data Catalog and squemas for each dataset in S3 by AWS Glue


### Create policy
```bash
aws iam create-policy \
--policy-name <NAME> \
--policy-document file://glue-policy.json

aws iam create-role \
--role-name <NAME> \
--assume-role-policy-document file://trust-policy.json

aws iam put-role-policy \
--role-name <NAME> \
--policy-name <NAME> \
--policy-document file://glue-policy.json
```

### Create database
```bash
aws glue create-database \
--database-input Name=r8g4
```

### Create crawler
```bash
aws glue create-crawler \
--name <NOME> \
--role arn:aws:iam::865118636886:role/<NOME> \
--database-name <NOME> \
--targets "{\"S3Targets\":[{\"Path\":\"<s3:NAME>\"}]}" \
--table-prefix <NOME>_
```

### Create job
```bash
aws glue create-job \
--name <NAME> \
--role arn:aws:iam::865118636886:role/<NAME> \
--command Name=glueetl,ScriptLocation=s3://<NAME>/job-script.py \
--default-arguments '{
    "--TempDir":"s3://<NAME>/jobs/temp",
    "--job-bookmark-option": "job-bookmark-enable",
    "--scriptLocation": "s3://<NAME>/job-script.py"
}'
```

### Start Crawlers
```bash
aws glue start-crawler \
--name <NOME>
```

### Start Jobs
```bash
aws glue start-job-run \
--job-name <NOME>
```

After creation of database, crawler and jobs the obhect bellow are created 

### Crawlers
![Crawlers](screenshots/glue-crawlers.png)

### Jobs
![Jobs](screenshots/glue-jobs.png)

### Tables 
![Tables](screenshots/glue-table-schema.png)


## [Analysis Review: For each question the related solution with explanations and evidences](analysis.html)

## References:
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
