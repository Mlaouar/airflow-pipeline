#!/bin/bash

# Update package information
sudo apt-get update
sudo apt-get install jq

# Install packages to allow apt to use a repository over HTTPS
sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common

# Add Docker’s official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

# Set up the stable repository
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

# Update package information again
sudo apt-get update

# Install the latest version of Docker Engine and containerd
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# Apply executable permissions to the binary
sudo chmod +x /usr/local/bin/docker-compose

echo "Docker and Docker Compose have been installed successfully!"

# Start Docker
sudo service docker start


# Add the user to the docker group
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

sudo docker-compose up airflow-init && sudo docker-compose up -d


# Read variables from JSON file
file="variables.json"
declare -A data
keys=$(jq -r 'keys[]' $file)

for key in $keys; do
    value=$(jq -r --arg key "$key" '.[$key]' $file)
    data["$key"]="$value"
done

# Wait for Airflow service to start and then add variables and connection
while true; do
    if [ "$(sudo docker-compose ps -q airflow-worker)" ]; then
        echo "Airflow service is running"
        container_id="$(sudo docker-compose ps -q airflow-worker)"

        sudo docker exec $container_id airflow connections add 'postgres_conn_id' --conn-uri "postgresql://${data[user]}:${data[password]}@${data[password]}:${data[port]}/${data[database]}"
        sudo docker exec $container_id airflow variables set execution_date ${data[execution_date]}
        sudo docker exec $container_id airflow variables set force_append ${data[force_append]}
        break
    else
        echo "Airflow service is not running"
        sleep 5
    fi
done


sudo docker-compose up