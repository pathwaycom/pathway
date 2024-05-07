# Copyright © 2024 Pathway

# using constants with names is only workaround that will reduce
# amount of time next person will spend figuring out some special column names that are
# added (and later used) silently in code

# TODO: replace with some dedicated mechanism that allows extracting and renaming
# those colunms (at least those that are meant as output, maybe all of them)


_INDEX_REPLY = "_pw_index_reply"
_QUERY_ID = "_pw_query_id"
_NO_OF_MATCHES = "_pw_number_of_matches"
_PACKED_DATA = "_pw_packed_data"
_TOPK = "_pw_topk"

_MATCHED_ID = "_pw_index_reply_id"
"""Default name of the column that stores id of matched document
in DataIndex (and InnerIndex) reply."""

_SCORE = "_pw_index_reply_score"
"""Default name of the column that stores scores associated with matched document
in DataIndex (and InnerIndex) reply."""
