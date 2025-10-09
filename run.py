import asyncio
from hypercorn.asyncio import serve
from hypercorn.config import Config
from flask_app import app

config = Config()
config.bind = ["0.0.0.0:3003"]
config.workers = 1
config.worker_class = "asyncio"

if __name__ == "__main__":
    asyncio.run(serve(app, config))

# or run this 
#hypercorn flask_app:app --bind 0.0.0.0:3003 --workers 1