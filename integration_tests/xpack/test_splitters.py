import pandas as pd

import pathway as pw
from pathway.xpacks.llm.splitters import RecursiveSplitter


def test_recursive_from_hf_tokenizer():

    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")

    splitter = RecursiveSplitter(hf_tokenizer=tokenizer, chunk_size=25, chunk_overlap=0)
    txt = "Pójdź, kińże tę chmurność w głąb flaszy 🍾."  # 23 tokens in bert tokenizer
    big_txt = "\n\n".join([txt] * 5)
    input_table = pw.debug.table_from_pandas(pd.DataFrame([dict(ret=big_txt)]))

    result = input_table.select(ret=splitter(pw.this.ret)).flatten(pw.this.ret)
    result = pw.debug.table_to_pandas(result)

    assert len(result) == 5
    assert result.iloc[0].ret[0] == txt
    assert result.iloc[0].ret[1] == pw.Json({})
