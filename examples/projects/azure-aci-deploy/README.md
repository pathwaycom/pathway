# Azure ACI Deployment Example

This repository contains an example for the tutorial: [**"Running Pathway Program in Azure with Azure Container Instances"**](https://pathway.com/developers/user-guide/deployment/azure-aci-deploy).

## Repository Structure

The repository includes the following key files:
- **`launch.py`**: A Python script responsible for deploying a Docker image in Azure Container Instances.
- **`requirements.txt`**: A list of Python dependencies required by `launch.py`.
- **`Dockerfile`**: Defines the Docker image configuration, allowing the example to run in isolation.

## Usage Guide

Before running the example, update the constants in `launch.py` with the necessary values.

You can run the example using one of two methods: Docker or virtualenv.

### Method 1: Running with Docker

1. Build the Docker image:
   ```bash
   docker build . -t pathway-azure-container-instances-example
   ```
2. Run the Docker container:
   ```bash
   docker run -t pathway-azure-container-instances-example
   ```

### Method 2: Running with virtualenv

1. Create a virtual environment:
   ```bash
   virtualenv venv
   ```
2. Activate the virtual environment:
   ```bash
   . venv/bin/activate
   ```
3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the script:
   ```bash
   python launch.py
   ```
