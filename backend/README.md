# FastAPI Backend

## Setup

1. The backend reads `backend/.env`.
2. `DATABASE_URL` is configured for Neon Postgres.
3. Install dependencies:

```powershell
pip install -r backend/requirements.txt
```

4. Run the API from the repo root:

```powershell
uvicorn app.main:app --app-dir backend --reload
```

## Render Deployment

The repo includes `render.yaml` for the backend web service.

1. In Render, create a new Blueprint or Web Service from this repository.
2. Set the service root to `backend` if you create it manually.
3. The backend is pinned to Python `3.12` via `backend/.python-version`, and `render.yaml` also sets `PYTHON_VERSION=3.12.9`.
4. Add the environment variables from `backend/.env.example`.
5. Use this start command if you configure the service manually:

```text
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Tables Created

- `competitions`
- `teams`
- `players`
- `repo_highlights`
- `source_assets`
- `matches`
- `match_events`
- `standings_snapshots`
- `player_stats`
