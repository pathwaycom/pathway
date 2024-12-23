## Dataset

Visit [this sheet](https://docs.google.com/spreadsheets/d/1ZlpEyc61dV4UeRYrejwcCISBoh_zRUlQ9BEYZcdwTyU/edit?pli=1#gid=1675078663) and switch to tab `Eval Data`.

File -> Download -> Tab Separated Values (.tsv)

Place it in this directory.

Swap the name of the dataset in the code ([`main.py`](main.py))

```python
df = pd.read_csv("extended_labeled.tsv", sep="\t")  # change this file name
```

## Running locally

Run the following to get credentials.
`vault kv get -field=gdrive_indexer.json deployments/showcase-indexer > ./gdrive_indexer.json`

Create the `.env` file and put API key.

## TODO
- Add examples to RAGAS answer_correctness evaluator to relax the evaluation (should bump the scored performance a bit)
- Add gitignore
- Add new dataset(s). Finance or law domain?
