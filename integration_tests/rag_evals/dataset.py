import json


class DatasetUtils:
    @staticmethod
    def read_dataset(file_path):
        with open(file_path, "r") as file:
            parsed = json.load(file)

        return parsed

    @staticmethod
    def save_dataset(dataset: list[dict] | dict, file_path: str) -> None:
        with open(file_path, "w") as file:
            json.dump(dataset, file, indent=4)
