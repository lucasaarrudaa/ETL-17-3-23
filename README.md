Simple python ETL application from s3 to MySQL database.

Installation
Use the package manager pip to install dependency packages listed on requirements.txt file.

$ pip install requirements.txt
Obs: I strongly advise you to use virtualenv to set up your environment.

Usage
$ python etl.py
Remember to set up all required dependencies like mysql and mysql workbench in order to get the application running.

Inside the conf/ directory, there's a docker compose file putting a postgres docker image online that can help you to set it up.

Application's log will be written at /logs directory.

$ pip install requirements.txt
Obs: Aconselho fortemente o uso do virtualenv para configurar seu ambiente.

Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.