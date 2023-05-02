#!/bin/bash

# Check if docker command exists
if [ -x "$(command -v docker)" ]; then
  echo "Docker is already installed."
else
  echo "Docker is not installed. Installing now..."
  # Check if OS is Linux or Windows
  if [ "$(uname)" == "Linux" ]; then
    # Use docker-install script from GitHub for Linux
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    rm get-docker.sh
  elif [ "$(uname)" == "MINGW64_NT-10.0" ]; then
    # Use Docker Desktop Installer.exe for Windows with PowerShell module
    Install-Module DockerMsftProvider -Force
    Install-Package Docker -ProviderName DockerMsftProvider -Force
  else
    echo "Unsupported OS. Please refer to https://docs.docker.com/engine/install/ for more information."
  fi  
fi

# Print docker version after installation or verification
docker --version
