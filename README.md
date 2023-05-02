This repo contains a basic AirFlow workflow for personal training. 

The workflow in question : 

- Treats some data and save it in a RDBSM system 
- Apply a basic Ml pipeLine
- Conducts some basic statistics on the data  

## STEPS to run the airflow containers through the bash script init: 

- fill the file variables.json in order to add the airflow variable sneeded and the db connection 
    - example : 
        {
            "user": "postgres",
            "password": "*****",
            "host": "localhost", // careful when running the database locally it may cause an issue use host.docker.internal instead
            "port": "5432",
            "database": "postgres",
            "execution_date": "''", // any value beside a valid date format will execute with today's date
            "force_append": "false" // best to be let always to false to avoid stacking same-day data
        }
- run the command bash init.sh once in the github repo/folder


## STEPS to run the airflow containers if the bash script fails : 

follow the steps here :  https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

- Install docker and docker-compose 

- If on linux/macos add the extra step detailed here for airflow  
    mkdir -p ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env

- Run the command 
     - docker compose up airflow-init 

- Once the command over we can finally run airflow with 
    - docker-compose up / docker compose up 

- After launching the UI, a postgresql connexion witht he id : postgres_conn_id must be added
- Finally the variables execution_date and force_append must be set also

