# Nova Dashboard — Backend API

Node.js/Express server that connects the Nova Loan Performance Dashboard to Metabase.

## Stack
- **Node.js** >= 18
- **Express** — HTTP server
- **Axios** — Metabase API calls
- **jsonwebtoken** — Metabase embed tokens
- **dotenv** — Environment config

## Local Setup

```bash
npm install
cp .env.example .env
# Fill in your values in .env
npm start
```

Server runs on `http://localhost:3000`

## Environment Variables

| Variable | Description |
|---|---|
| `METABASE_URL` | Internal Metabase URL (e.g. `https://192.168.x.x:443`) |
| `METABASE_SITE_URL` | Public Metabase URL for embed tokens |
| `METABASE_USERNAME` | Metabase admin email |
| `METABASE_PASSWORD` | Metabase admin password |
| `METABASE_SECRET` | Metabase embed secret key |
| `NOVA_DB_ID` | Metabase database ID (default: `2`) |
| `PORT` | Server port (default: `3000`) |
| `CACHE_TTL` | Session cache duration in seconds (default: `3600`) |

## DigitalOcean Deployment

Deploy via the App Platform using `.do/app.yaml`.  
Set all `SECRET` env vars in the DO dashboard — never commit `.env`.

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check for DO |
| `GET` | `/api/kpis` | Main KPI metrics |
| `GET` | `/api/branch-performance` | Branch performance data |
| `GET` | `/api/officer-performance` | Officer performance data |
| `POST` | `/api/embed-token` | Generate Metabase embed JWT |
