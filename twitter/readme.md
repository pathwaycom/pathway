1. Start postgres locally:

```
 initdb -D mylocal_db
```

2. Create required tables in postgres by running:

```
psql -h 0.0.0.0 -p 7703 -d postgres -U $USER -f /home/$USER/IoT-Pathway/public/pathway/python/pathway/examples/twitter/postgres_schema.sql`
```

3. For the demo purposes, please move to the main Python directory, so that the following command gives you:

```
pwd
...IoT-Pathway/public/pathway/python/pathway
```

4. Create folder, which will receive stream of tweets. In a separate window (or in `tmux`) launch the stream generator:

```
mkdir twitter_data
python examples/twitter/generate_stream_of_tweets.py
```

5. Run the engine:

```
python examples/twitter/main.py
```
