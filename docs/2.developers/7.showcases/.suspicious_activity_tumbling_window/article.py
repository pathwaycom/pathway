# ---
# title: "Real-Time Anomaly Detection: identifying brute-force logins using Tumbling Windows"
# description: Detecting suspicious login attempts
# notebook_export_path: notebooks/tutorials/suspicious_user_activity.ipynb
# author: 'przemek'
# aside: true
# article:
#   date: '2023-05-30'
#   thumbnail: '/assets/content/blog/th-shield.png'
#   tags: ['tutorial', 'machine-learning']
# keywords: ['window', 'tumbling', 'alert', 'notebook']
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Real-Time Anomaly Detection: identifying brute-force logins using Tumbling Windows
#
# In this tutorial you will learn how to perform a [tumbling window](/glossary/tumbling-window) operation to detect suspicious activity.
#
# Your task is to detect suspicious user login attempts during some period of time.
# You have a record of login data. Your goal is to detect suspicious users who have logged in more than 5 times in a single minute.
#
# To do this, you will be using the `windowby` syntax with a `pw.temporal.tumbling()` object. Let's jump in!
#
# Your input data table has the following columns:
# * `username`,
# * whether the login was `successful`,
# * `time` of a login attempt,
# * `ip_address` of a login.
#
#
# Let's start by ingesting the data:
#
#
# First ingest the data.
# %%
# Uncomment to download the required files.
# # %%capture --no-display
# # !wget https://public-pathway-releases.s3.eu-central-1.amazonaws.com/data/suspicious_users_tutorial_logins.csv -O logins.csv

# %%
import pathway as pw


class InputSchema(pw.Schema):
    username: str
    successful: str
    time: int
    ip_address: str


logins = pw.io.csv.read(
    "logins.csv",
    schema=InputSchema,
    mode="static",
)

# %% [markdown]
# The CSV data has the string values "True" and "False" in the `successful` column.
#
# Let's convert this to a Boolean column:

# %%
logins = logins.with_columns(successful=(pw.this.successful == "True"))

# %% [markdown]
# Then, let's filter attempts and keep only the unsuccessful ones.

# %%
failed = logins.filter(~pw.this.successful)

# %% [markdown]
# Now, perform a tumbling window operation with a duration of 60 (i.e. 1 minute).
#
# Use the `instance` keyword to separate rows by the `ip_address` value.

# %%
result = failed.windowby(
    failed.time, window=pw.temporal.tumbling(duration=60), instance=pw.this.ip_address
).reduce(
    ip_address=pw.this._pw_instance,
    count=pw.reducers.count(),
)

# %% [markdown]
# ...and finally, let's keep only the IP addresses where the number of failed logins exceeded the threshold (5):
# %%
suspicious_logins = result.filter(pw.this.count >= 5)
pw.debug.compute_and_print(suspicious_logins)

# %% [markdown]
# And that's it! You have used a tumbling window operation to identify suspicious user activity and can now act on this information to increase the security of your platform.
#
# Reach out to us on [Discord](https://discord.gg/pathway) if you'd like to discuss [real time anomaly detection](/glossary/real-time-anomaly-detection) use cases like this one in more detail!
