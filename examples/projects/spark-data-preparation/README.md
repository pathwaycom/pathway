# Data Preparation for Spark Analytics

This repository contains the Dockerized example code for the "Data Preparation for Spark Analytics" showcase.

## Running the Example

To run this example, follow these steps:

1. Get your GitHub Personal Access Token (PAT) from the [Personal access tokens](https://github.com/settings/tokens) page.
2. Insert this token into the `personal_access_token` field in the `./github-config.yaml` file. There is a comment there for guidance.
3. Build the Docker image with the command: `docker build --no-cache -t spark-data-preparation .`
4. Run the Docker image. Note that the Delta Lake connector is available only in the Pathway Scale and Pathway Enterprise tiers. You need to provide the `PATHWAY_LICENSE_KEY` variable when launching the Docker container. Use the following command: `docker run -e PATHWAY_LICENSE_KEY=YOUR_LICENSE_KEY -t spark-data-preparation`

## Running the Example with S3

You can run this example similarly for the S3 case. To enable S3 output, pass the `AWS_S3_OUTPUT_PATH` environment variable to the container.

Additionally, you need to specify the following environment variables:
* `AWS_S3_ACCESS_KEY`: Your S3 access key
* `AWS_S3_SECRET_ACCESS_KEY`: Your S3 secret access key
* `AWS_BUCKET_NAME`: The name of your S3 bucket
* `AWS_REGION`: The region of your S3 bucket

The launch command will look like this:

```bash
docker run \
    -e PATHWAY_LICENSE_KEY=YOUR_LICENSE_KEY \
    -e AWS_S3_OUTPUT_PATH=YOUR_OUTPUT_PATH_IN_S3_BUCKET \
    -e AWS_S3_ACCESS_KEY=YOUR_S3_ACCESS_KEY \
    -e AWS_S3_SECRET_ACCESS_KEY=YOUR_S3_SECRET_ACCESS_KEY \
    -e AWS_BUCKET_NAME=YOUR_BUCKET_NAME \
    -e AWS_REGION=YOUR_AWS_REGION \
    -t spark-data-preparation
```
