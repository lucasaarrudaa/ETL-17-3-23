# ETL Application for Data Integration 
## Status: WORKING

This is a simple Python ETL (Extract, Transform, Load) application that transfers data from Amazon S3 to a PostgreSQL database. The project aims to integrate four different datasets, including two CSV files, one JSON file, and one XML file, and prepare an analytical database or table for the digital marketing team to perform queries and answer business questions.

## Installation
To install the required dependencies, please use the following command using pip, the Python package manager:

pip install -r requirements.txt
Note: It is strongly recommended to use a virtual environment to configure your development environment.


## Usage
To run the ETL process, execute the following command:
python main.py
Please make sure to set up all the required dependencies, such as PostgreSQL and MySQL Workbench, in order to ensure the proper functioning of the application.

## Objective
The main objective of this project is to integrate different datasets and create an analytical table that can be used to answer business questions for the digital marketing team.

## Conclusion

The code is capable of extracting, transforming, and loading data from the datasets, integrating them into a single table, and loading them into a PostgreSQL database. An analytical table is created to answer business questions for the digital marketing team. This project demonstrates skills in ETL, Infrastructure as Code (IAC), data analysis, transformation, and processing, as well as the use of Docker Compose for containerization.
