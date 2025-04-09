cat article.md | \
perl -0777 -pe 's@```python\n# Due to the bug described in https://github.com/jupyter/notebook/issues/1622.*# *pw.run\(\)\n```@<a href="https://colab.research.google.com/github/pathwaycom/pathway/blob/main/examples/notebooks/showcases/live-data-jupyter.ipynb" target="_parent"><img src="https://pathway.com/assets/colab-badge.svg" alt="Run In Colab" class="inline"/></a>@sg' \
> _article.md
mv _article.md article.md
