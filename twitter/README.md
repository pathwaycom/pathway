# Realtime Twitter Analysis App with Pathway

An example usage of Pathway, the programming framework for handling streaming data updates.
The application allows for streaming twitter data on a given topic, performs sentiment analysis and visualizes the results.

See [the corresponding page at pathway.com](https://pathway.com/developers/showcases/twitter) for a high-level description.

---

At the top level of the repository, you will find three directories containing the demo application logic - ordered from left to right according to the flow of data:
1. `/dataset_replay` - a helper app which allows for the replay (simulation) of a dataset of tweets, sending the prepared dataset in portions to kafka or to the filesystem;
2. `/pathway_app` - the main app in Pathway, which takes data from the given channel, processes it, and then stores the data as a postgres snapshot; 
3. `/showcase_app` - the web page with results visualization (frontend and backend). The backend takes data from the snapshot which is maintained by the app in Pathway.

You will also find ready to go setup for two possible deployments of the demo
* `/docker_replay_all_at_once` - contains a docker-compose file for running a simple example, with the complete input dataset being replayed all at once;
* `/docker_replay_as_a_stream` - contains the docker-compose file for running an example with dataset streaming, demonstrating how the stream is processed by the Pathway engine in the Pathway app.
## How to run this example?

In order to run this example, you need docker-compose.

1. Navigate either to ./docker or to ./docker-replayer directory. The first one contains an example for batch mode: this is a simple version, where we upload the whole dataset to the engine at once and then we are quickly able to see the results. The second one streams the same dataset with delays proportional to the delays between the tweets appearance time;
2. If you have previously ran the example, it may be a good idea to shut down the running example. It can be done with `docker-compose --env-file settings.env down -v`
3. Now you can start the example! It's as simple as `docker-compose --env-file settings.env up --build`
4. In order to see the results and progress, you need to ensure that `API_PORT` and `FRONTEND_PORT` from the config `settings.env` are available.
5. Now you can navigate to `http://localhost:7704` in your browser and see the progress.