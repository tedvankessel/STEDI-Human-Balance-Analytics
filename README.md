# STEDI-Human-Balance-Analytics
Data Engineering with AWS project 3
Author: Theodore G. van Kessel


## This repository has been constructed with the following directories:

	- All Glue Python files
	- All screen shots
	- All SQL files for tables

In preparing this work the following steps were executed:
  
## Step 1:

Uploaded data copied from the Udacity
https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter 
site to:

	- s3://tvkbucket/customer/landing
	- s3://tvkbucket/accelerometer/landing
	- s3://tvkbucket/step_trainer/landing

## Step 2: 

Verify glue roles etc. from previous work:

	- S3 Gateway Endpoint
	- Glue Service IAM Role
	- Glue Privileges on the S3 bucket and Glue Policy

## Step 3:

	Glue Studio was used to create the required data sets joins etc. for the landing, trusted and curated zones
	As instructed. In each case the python script was saved under the target data name.
	
	For each zone Athena was used to create SQL queries to create tables. 
	Each Glue job was saved as a SQL file under the target data name.
	
	In each case was executed to verify the data of the form:
		select * from table limit 10; 
	In each case a screen shot was taken of the data and stored under the corresponding table name.

 ## Sources

 Source material for this project came from the following:

 	Udacity course materials : nd027-Data-Engineering-Data-Lakes-AWS
  	Udacity Github : https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter
   	ChatGPT : https://chat.openai.com/
	Direct instruction from Udacity mentor: Jay Teguh Wijaya
