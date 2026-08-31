import yaml


def load_config(path="config.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)
