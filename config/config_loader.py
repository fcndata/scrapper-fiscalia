from pathlib import Path
import yaml

class Config:
    _instance = None
    _config = None
    
    def __new__(cls, config_path: Path = Path("config/config.yaml")):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config(config_path)
        return cls._instance
    
    def _load_config(self, config_path: Path):
        self.path = Path(config_path)
        if not self.path.exists():
            raise FileNotFoundError(f"Config file not found: {self.path}")
        with open(self.path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f) or {}

    def get(self, key: str, default=None):
        """
        Allows dot notation access: config.get("url.url_1")
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
        return self.get(key)

    def __contains__(self, key):
        return self.get(key) is not None

    def __repr__(self):
        return f"<Config path={self.path}>"
