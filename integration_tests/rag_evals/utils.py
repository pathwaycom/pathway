import matplotlib.pyplot as plt
import seaborn as sns


def create_file_filter(file: str) -> str:
    """Create globmatch filter for a single file."""
    return "globmatch(`" + file + "`, path)"


def save_pivot_table_as_confusion(table, path: str) -> None:
    colors = ["red", "green"]

    sns.set(font_scale=1.2)
    plt.figure(figsize=(12, 10))
    plt.subplots_adjust(bottom=0.15, left=0.15)
    heatmap = sns.heatmap(table, cmap=colors, annot=True, fmt="d", cbar=False)
    plt.yticks(rotation=0)
    plt.xticks(rotation=90)

    heatmap.figure.savefig(path, bbox_inches="tight")
