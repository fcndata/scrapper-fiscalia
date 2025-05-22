from pathlib import Path
import yaml

class Config:
    def __init__(self, config_path: Path = Path("config/config.yaml")):
        self.path = Path(config_path)
        if not self.path.exists():
            raise FileNotFoundError(f"Config file not found: {self.path}")
        with open(self.path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f) or {}

    def get(self, key: str, default=None):
        """
        Allows dot notation access: config.get("tools.chromedriver_path")
        """
        keys = key.split(".")
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value

    def __getitem__(self, key):
        return self.config[key]

    def __contains__(self, key):
        return key in self.config

    def __repr__(self):
        return f"<Config path={self.path}>"
