DEFAULT_VISION_MODEL = "gpt-4o-mini"

OPENAI_EMBEDDERS_MAX_TOKENS: dict = {
    "text-embedding-3-small": 8191,
    "text-embedding-3-large": 8191,
    "text-embedding-ada-002": 8191,
}

# Sizes of the vectors returned by the models, when the `dimensions` parameter
# is not used to shorten them.
OPENAI_EMBEDDERS_DIMENSIONS: dict = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536,
}
