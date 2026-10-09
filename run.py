
import uvicorn, yaml
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
with open(BASE_DIR / "config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

api = config.get("api", {})
uvicorn.run("app:app", host=api.get("host","0.0.0.0"), port=int(api.get("port",8080)), reload=False)
