# Nifty 500 RSI analyzer

Streamlit app: pull Yahoo Finance OHLCV, compute RSI, find threshold excursions (first / last / max-high candle triplets).

**Repository:** [github.com/ashekhawatJC/nifty500-rsi-analyzer](https://github.com/ashekhawatJC/nifty500-rsi-analyzer)

## Host it free (freemium)

### Option 1 — Render (recommended: one click)

Free **Web Service** tier (sleeps when idle; wakes on the next visit).

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/ashekhawatJC/nifty500-rsi-analyzer)

1. Click the button above (or open the link, sign in to Render, connect GitHub if asked).
2. Confirm the blueprint from `render.yaml` and deploy.
3. After the build finishes, open the **`.onrender.com`** URL Render shows.

Optional: in the service **Environment** tab, add `ACCESS_PASSWORD` so only people with the secret can use the app.

### Option 2 — Streamlit Community Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.
2. **New app** → repository **`ashekhawatJC/nifty500-rsi-analyzer`**, branch **`main`**, main file **`app.py`** → Deploy.

Optional: **App settings → Secrets** → `ACCESS_PASSWORD = "your-secret"`.

### More options

See [DEPLOY.md](DEPLOY.md) (Hugging Face Spaces, Docker).

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Disclaimer

Educational / research use only; not investment advice.
