# Official Playwright image — includes Chromium + all system dependencies
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# /data is where the persistent disk mounts in production, so state
# (checkpoint.json, run history, the shared CSV) survives restarts/redeploys.
# webapp/ sits *inside* /data so app.py's `BASE_DIR.parent` — the folder it
# looks in for a shared default CSV — resolves to /data, same as it resolves
# to the project folder one level above webapp/ when run locally.
WORKDIR /data

COPY webapp/requirements.txt webapp/requirements.txt
RUN pip install --no-cache-dir -r webapp/requirements.txt

COPY webapp/ webapp/

WORKDIR /data/webapp
EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
