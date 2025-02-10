This readme is internal and is not synchronized with public repository. All general information are available in README.md, whereas this readme only contains information relevant to our infrastructure.

## Running locally

Run the following to get credentials.
```
prodaccess
vault kv get -field=gdrive_indexer.json deployments/showcase-indexer > ./gdrive_indexer.json
```

Create the `.env` file and set:
- OpenAI_API_KEY key.
- `MLFLOW_URI="https://mlflow.internal.pathway.com"`

Run with `source run_locally.sh`.

## MlFlow
All runs are logged in our internal MlFlow, accessible at: https://mlflow.internal.pathway.com/. The experiment name is `CI RAG Evals`.

### Setting description
Description of the run can be set with environment variable `MLFLOW_DESCRIPTION`. It will then show up as description on mlflow.

## TODO
- Add examples to RAGAS answer_correctness evaluator to relax the evaluation (should bump the scored performance a bit)
- Add gitignore
- Add new dataset(s). Finance or law domain?
