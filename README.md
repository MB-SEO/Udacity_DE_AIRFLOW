## Udacity DE Nano Degree: Airflow Pipeline project
This project is mainly focus on the use of Apache Airflow core concepts. <>
Dataset is from a music streaming company:sparkify and they are requesting to automate the ETL process using Airflow. <>
Source of data resides in S3 and needs to be processed in AWS Redshift. <>

## Project learning topic
This project focus on learning below topics:
- general use of Airflow UI
- creating custom operators to staging data, filling datawarehouse, and dataquality checks.
- use of seperate ETL SQL file for better organization of codes. 

## Prerequites 
- IAM user in AWS
- Redshift Cluster (us-west-2)
- Airflow connection to AWS
- Airflow connection to cluster
- Airflow connection to S3 bucket

## Datasets
Log data = s3://udacity-dend/log-data
Song data = s3://udacity-dend/song-data

## Action Steps
1. Configure DAG (no dependencies on past runs / retries 3 times every 5 min / Catchup turned off / Do not email on retry)
2. Build Custom Operators (use built-in functionality - connections, hooks, make scalable & flexible code so that we can use in elsewhere with different datasets as well) 
3. Configure task dependencies
4. Test







