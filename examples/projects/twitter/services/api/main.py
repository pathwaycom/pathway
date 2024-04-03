# Copyright © 2024 Pathway

import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.api:app", host="0.0.0.0", port=7711, reload=True)
