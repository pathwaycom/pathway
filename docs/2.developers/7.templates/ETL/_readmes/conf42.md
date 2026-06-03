# Make your LLM app sane again: Forgetting incorrect data in real time 

This is the implementation of the LLM-app presented in the [Conf42 LLM talk](https://www.conf42.com/Large_Language_Models_LLMs_2024_Olivier_Ruas_llm_app_forgetting).

## Abstract

In this talk, you will learn how to create an LLM-powered chatbot in Python from scratch with an up-to-date RAG mechanism. The chatbot will answer questions about your documents, updating its knowledge in real-time alongside the changes in the documentation, enabling the chatbot to filter out fake news.

## How to run

To run the LLM-app, you need to:
1. install Pathway with `pip install pathway`,
2. enter your OpenAI key in the `.env` file,
3. run the script `python main.py`.

All the documents in `documents/` are indexed and you can query the chatbot with curl:
```
curl --data '{
   "user": "user",
   "query": "What is the revenue of Alphabet in 2022 in millions of dollars?"
}' http://localhost:8000/ | jq
```

It will query the most similar document and answer to your question.

You can then add/remove documents.
For example:
```
rm ./documents/20230203_alphabet_10K.pdf
```
All your next queries will not take into account this document.
