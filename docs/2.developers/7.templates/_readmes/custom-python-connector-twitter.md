# Custom python connector example

> ⚠️ Twitter has turned off its free tier streaming API.

The purpose of this example is to show how to implement custom python connector.

The sample script is a simple Pathway pipeline that turns a stream of tweets into a Pathway table and writes the stream of changes to an `output.csv` file. Twitter API was used as an example, but any data stream can be fed into Pathway using this method.

How to launch this example:
1. Install Pathway package using method described here `https://pathway.com/developers/documentation/introduction/installation-and-first-steps/` 
2. `pip install -r requirements.txt` to install additional dependencies.
3. `export TWITTER_API_TOKEN=<BEARER_TOKEN>` to provide bearer token obtained from Twitter developer portal.
4. `python twitter_connector_example.py` to run sample code.
5. Press `CTRL+C` to stop the script.
