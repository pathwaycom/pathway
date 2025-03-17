# Pathway + LLM + Slack notification: RAG App with real-time alerting when answers change in documents

Microservice for a context-aware alerting ChatGPT assistant.

This demo is very similar to the `alert` example, the only difference is the data source (Google Drive)
For the demo, alerts are sent to Slack (you need to provide `slack_alert_channel_id` and `slack_alert_token`),
you can either put these env variables in .env file,
or create env variables in the terminal (i.e. export in bash).

The program then starts a REST API endpoint serving queries about Google Docs stored in a
Google Drive folder.

We can create notifications by asking from Streamlit or sending query to API stating we want to be notified.
One example would be `Tell me and alert about the start date of the campaign for Magic Cola`

## How Does It Work?

First, Pathway connects to Google Drive, extracts all documents, splits them into chunks, turns them into
vectors using OpenAI embedding service, and store in a nearest neighbor index.

Each query text is first turned into a vector, then relevant document chunks are found
using the nearest neighbor index. A prompt is built from the relevant chunk
and sent to the OpenAI GPT3.5 chat service for processing and answering.

After an initial answer is provided, Pathway monitors changes to documents and selectively
re-triggers potentially affected queries. If the new answer is significantly different from
the previously presented one, a new notification is created.

## How to run the project

Before running the app, you will need to give the app access to the Google Drive folder, we follow the steps below.

In order to access files on your Google Drive from the Pathway app, you will need a Google Cloud project and a service user.

### Create a new project in the Google API console:

- Go to [https://console.cloud.google.com/projectcreate](https://console.cloud.google.com/projectcreate) and create new project
- Enable Google Drive API by going to [https://console.cloud.google.com/apis/library/drive.googleapis.com](https://console.cloud.google.com/apis/library/drive.googleapis.com), make sure the newly created project is selected in the top left corner
- Configure consent screen:
  - Go to [https://console.cloud.google.com/apis/credentials/consent](https://console.cloud.google.com/apis/credentials/consent)
  - If using a private Gmail, select "External", and go next.
  - Fill required parameters: application name, user support, and developer email (your email is fine)
  - On the next screen click "Add or remove scopes" search for "drive.readonly" and select this scope
  - Save and click through other steps
- Create service user:

  - Go to [https://console.cloud.google.com/apis/credentials](https://console.cloud.google.com/apis/credentials)
  - Click "+ Create credentials" and create a service account
  - Name the service user and click through the next steps
- Generate service user key:
  - Once more go to [https://console.cloud.google.com/apis/credentials](https://console.cloud.google.com/apis/credentials) and click on your newly created user (under Service Accounts)
  - Go to "Keys", click "Add key" -> "Create new key" -> "JSON"
  
  A JSON file will be saved to your computer.

Rename this JSON file to `secrets.json` and put it under `examples/pipelines/drive_alert` next to `app.py` so that it is easily reachable from the app.

You can now share desired Google Drive resources with the created user.
Note the email ending with `gserviceaccount.com` we will share the folder with this email.

Once you've done it, you will need an ID of some file or directory. You can obtain it manually by right-clicking on the file -> share -> copy link. It will be part of the URL.

[https://drive.google.com/file/d/[FILE_ID]/view?usp=drive_link](https://drive.google.com/file/d/%5BFILE_ID%5D/view?usp=drive_link)

For folders,
First, right-click on the folder and click share, link will be of the format: [https://drive.google.com/drive/folders/[folder_id]?usp=drive_link](https://drive.google.com/drive/folders/%7Bfolder_id%7D?usp=drive_link)
Copy the folder_id from the URL.
Second, click on share and share the folder with the email ending with `gserviceaccount.com`

### Setup Slack notifications:

For this demo, Slack notifications are optional and notifications will be printed if no Slack API keys are provided. See: [Slack Apps](https://api.slack.com/apps) and [Getting a token](https://api.slack.com/tutorials/tracks/getting-a-token).
Your Slack application  will need at least `chat:write.public` scope enabled.

### Setup environment:
Set your env variables in the .env file placed in this directory.

```bash
OPENAI_API_KEY=sk-...
SLACK_ALERT_CHANNEL_ID=  # If unset, alerts will be printed to the terminal
SLACK_ALERT_TOKEN=
FILE_OR_DIRECTORY_ID=  # file or folder ID that you want to track that we have retrieved earlier
GOOGLE_CREDS=./secrets.json  # Default location of Google Drive authorization secrets
PATHWAY_PERSISTENT_STORAGE= # Set this variable if you want to use caching
```

### Run with Docker

To run jointly the Alert pipeline and a simple UI execute:

```bash
docker compose up --build
```

Then, the UI will run at http://0.0.0.0:8501 by default. You can access it by following this URL in your web browser.

The `docker-compose.yml` file declares a [volume bind mount](https://docs.docker.com/reference/cli/docker/container/run/#volume) that makes changes to files under `data/` made on your host computer visible inside the docker container. The files in `data/live` are indexed by the pipeline - you can paste new files there and they will impact the computations.

### Run manually

Alternatively, you can run each service separately.

Make sure you have installed poetry dependencies with `--extras unstructured`. 
```bash
poetry install --with examples --extras unstructured
```

Then run:
```bash
poetry run python app.py
```

If all dependencies are managed manually rather than using poetry, you can alternatively use:
```bash
python app.py
```

To run the Streamlit UI, run:
```bash
streamlit run ui/server.py --server.port 8501 --server.address 0.0.0.0
```

### Querying the pipeline

To create alerts, you can call the REST API:

```bash
curl --data '{
  "user": "user",
  "query": "When does the magic cola campaign start? Alert me if the start date changes."
}' http://localhost:8080/ | jq
```

or access the Streamlit UI at `0.0.0.0:8501`.
