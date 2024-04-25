# Realtime Twitter Analysis App with Pathway

> ⚠️ Twitter has turned off its free tier streaming API.

An example usage of Pathway, the programming framework for handling streaming data updates.
The application allows for streaming twitter data on a given topic, performs sentiment analysis and visualizes the results.

See [the corresponding page at pathway.com](https://pathway.com/developers/showcases/twitter) for a high-level description.

---

Under `services` directory, you will find six directories containing the demo application logic - ordered from left to right according to the flow of data:
1. `dataset-replayer` - a helper app which allows for the replay (simulation) of a dataset of tweets, sending the prepared dataset in portions to kafka or to the filesystem;
2. `tweets-streamer` - another helper app which fetches tweets for the given (customizable) topic. It can be used for real-time tweets analysis scenario;
3. `pathway-app` - the main app in Pathway, which takes data from the given channel, processes it, and then stores the data as a postgres snapshot;
4. `geocoder` - a helper app which does the geocoding on the received tweet's metadata;
5. `api` - backend for the visualization webpage. The backend takes data from the snapshot which is maintained by the app in Pathway;
6. `frontend` - the frontend for web page with results visualization.

You will also find ready to go setup for three possible deployments of the demo in `docker-compose` repository:
* `docker-compose-replay-all-at-once.yml` - docker-compose file for running a simple example, with the complete input dataset being replayed all at once;
* `docker-compose-replay-as-stream.yml` - the docker-compose file for running an example with dataset streaming, demonstrating how the stream is processed by the Pathway engine in the Pathway app;
* `docker-compose-stream-given-topic.yml` - docker-compose file for running an example, with streaming the tweets on a given topic in real-time.
## How to run this example?

In order to run this example, you need docker-compose.

1. Navigate `./docker-compose` directory. It contains docker-compose files for the examples;
2. Edit the `settings.env` file, updating the tokens, `API_PORT` and `FRONTEND_PORT`.
3. If you have previously ran the example, it may be a good idea to shut down the running example. It can be done with `docker-compose -f <name_of_example's_docker-compose-file> --env-file settings.env down -v`
4. Now you can start the example! It's as simple as `docker-compose -f <name_of_example's_docker-compose-file> --env-file settings.env up --build`
5. Now you can navigate to `http://localhost:${FRONTEND_PORT}` in your browser and see the progress.
## How to analyze tweets on a given topic in real-time?

In docker-compose directory, you can find the file `docker-compose-stream-given-topic.yml`. This is the file you need to use in order to stream the given topic. The streamer itself is placed within `services/tweets-streamer` folder. In its [Dockerfile](https://github.com/pathwaycom/pathway/blob/main/examples/showcases/twitter/services/tweets-streamer/Dockerfile#L6), you can provide a topic, on which the real-time data fetching should take place. Then it is parsed within the [streamer app code](https://github.com/pathwaycom/pathway/blob/main/examples/showcases/twitter/services/tweets-streamer/app/main.py#L127) and used in the requests to Tweepy. The realtime stream of tweets is then passed to Pathway engine through Kafka.
