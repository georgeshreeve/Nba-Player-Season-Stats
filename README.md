# Nba-Player-Season-Stats

The 'Database' backend of this application are some simple flat files saved in S3. Here is the link to the 2 datasets that are used: https://www.kaggle.com/datasets/drgilermo/nba-players-stats?resource=download.  Athena configuration and schema recognition can be done easily using AWS Glue. 

You will also need to create relevant buckets for storing the athena query results and have an IAM role configured to execute Athena and read, write and delete permissions for S3.

I deployed my application using an Ubuntu 20.04 ECS instance and nginx
