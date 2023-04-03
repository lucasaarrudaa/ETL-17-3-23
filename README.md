
Simple python ETL application from s3 to Postgresql database.

Installation
Use the package manager pip to install dependency packages listed on requirements.txt file.

$ pip install requirements.txt

Usage
$ python etl.py

*Remember to set up all required dependencies like mysql and mysql workbench in order to get the application running.

$ pip install requirements.txt  Note: I strongly advise using virtualenv to configure your environment.

em conf\sql\create_db você deve substituir as informações de user, password, host e port de acordo com as configurações do seu servidor MySQL. 
O comando CREATE DATABASE IF NOT EXISTS "leads" cria o banco de dados chamado leads, caso ele ainda não exista.
Substitua o leads pelo nome do banco você deseja. 

Para criar um container com essas imagens, você precisa criar um arquivo docker-compose.yml com o conteúdo que você postou. 

Depois, basta executar o comando docker-compose up no diretório onde se encontra o arquivo docker-compose.yml.

Certifique-se de que o Docker Compose esteja instalado na sua máquina antes de executar o comando acima.