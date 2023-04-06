STATUS: WORKINNG

Simple python ETL application from s3 to Postgresql database.

Installation
Use the package manager pip to install dependency packages listed on requirements.txt file.

$ pip install requirements.txt

Usage
$ python etl.py

*Remember to set up all required dependencies like mysql and mysql workbench in order to get the application running.

$ pip install requirements.txt  Note: I strongly advise using virtualenv to configure your environment.

The aim of the project was to integrate 4 different datasets (2 csv, 1 json, and 1 xml) and prepare an analytical database/table for the digital marketing team to perform queries and answer business questions. Skills in ETL, IAC, data analysis, transformation, and processing were used, as well as the use of Docker Compose.

In the main code, the ETL process begins with the extraction of data from the files in the bucket to the local environment. The data is then read into dataframes and transformed to clean, rename, and delete unnecessary columns. Next, the tables are concatenated into a single table and loaded into the database.

Objective:

The objective was to integrate the datasets and create an analytical table to answer business questions for the digital marketing team.

Conclusions:

The code was able to extract, transform, and load the data from the datasets, integrate them into a single table, and load them into a database. An analytical table was created to answer business questions for the digital marketing team. The project demonstrated skills in ETL, IAC, data analysis, transformation, and processing, as well as the use of Docker Compose.
