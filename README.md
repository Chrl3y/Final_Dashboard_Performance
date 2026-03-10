# Nova Dashboard — Frontend

Static HTML dashboard for Nova Microfinance Loan Performance.  
Deployed as a **DigitalOcean App Platform Static Site**.

## Files

| File | Description |
|---|---|
| `Nova-dashboard-live.html` | Main dashboard (single-file, self-contained) |
| `.do/app.yaml` | DigitalOcean App Platform spec |

## API Configuration

The dashboard connects to the backend API via `window.NOVA_API_BASE`.  
Set the `NOVA_API_BASE` environment variable in the DO app spec to point to your deployed backend URL:

```
https://nova-dashboard-backend-xxxxx.ondigitalocean.app
```

For local development, `API_BASE` falls back to `http://localhost:3000`.

## DigitalOcean Deployment

1. Go to [DigitalOcean App Platform](https://cloud.digitalocean.com/apps)
2. Create a new app → connect `Chrl3y/Final_Dashboard_Performance` repo
3. Select **branch: frontend**
4. Choose **Static Site** as the component type
5. Set `Index document` → `Nova-dashboard-live.html`
6. Add env var: `NOVA_API_BASE` = your backend URL
7. Deploy

## CORS

Ensure the backend has the frontend's DO domain in its CORS allowed origins.
