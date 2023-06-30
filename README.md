# STEDI-Human-Balance-Analytics
Data Engineering with AWS project 3
Author: Theodore G. van Kessel


## This repository has been organized with the following directories:

	- **All Glue Python files**
	- __All screen shots__
	- __All SQL files for tables__

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

The project instructions and Rubric were followed to create the various data lakehouse zones and database tables:

	Glue Studio was used to create the required data sets joins etc. for the landing, trusted and curated zones
	As instructed. In each case the python script was saved under the target data name.
	
	For each data lakehouse zone Athena was used to create SQL queries to create tables. 
	Each Glue job was saved as a SQL file under the target data name.
	
	For each table, a SQL query was executed to verify the data of the form:
		select * from table limit 10; 
	In each case a screen shot was taken of the data and stored under the corresponding table name.
 
 ## Authors

* **Theodore van Kessel** 

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments and sources

 Source material for this project came from the following:

 	Udacity course materials : nd027-Data-Engineering-Data-Lakes-AWS
  	Udacity Github : https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter
   	ChatGPT : https://chat.openai.com/
	Direct instruction from Udacity mentor: Jay Teguh Wijaya
 	README-Template - https://gist.github.com/PurpleBooth/109311bb0361f32d87a2
  	Amazon documentation - https://docs.aws.amazon.com/glue/latest/ug/what-is-glue-studio.html
