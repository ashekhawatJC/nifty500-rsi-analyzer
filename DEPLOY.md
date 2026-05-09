# Host the app (free UI with a public URL)

This folder should be the **root of a GitHub repository** (or set your host’s **root directory** to this folder if the repo is larger).

---

## Option A — Streamlit Community Cloud (simplest for Streamlit)

1. Push this project to a **GitHub** repo (public repos are free on Streamlit Cloud).
2. Sign in at [share.streamlit.io](https://share.streamlit.io) with GitHub.
3. **New app** → pick the repo, branch, and main file **`app.py`**.
4. Deploy. You get a URL like `https://<something>.streamlit.app`.

**Access:** Anyone with the link can use the app (typical for free tier). For private repos you need Streamlit paid plans.

**Secrets:** Not required. Wikipedia + Yahoo use public HTTPS.

---

## Option B — Render (free Web Service)

1. Push the same repo to GitHub.
2. Sign up at [render.com](https://render.com) → **New** → **Blueprint** (connect repo) **or** **Web Service** with:
   - **Build:** `pip install -r requirements.txt`
   - **Start:** `streamlit run app.py --server.port $PORT --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false`
   - **Root directory:** `nifty500-rsi-analyzer` if this folder is not the repo root.
3. If you use Blueprint, this repo includes **`render.yaml`** you can commit and connect.

**Access:** Public URL on `onrender.com`. Free instances **sleep** when idle (first load after sleep can take ~30–60s).

---

## Option C — Hugging Face Spaces

1. Create a new **Space** → SDK **Streamlit**.
2. Upload **`app.py`**, **`requirements.txt`**, and the **`src/`** tree (or push from Git).
3. HF runs `streamlit run app.py` for you.

**Access:** Public by default on `huggingface.co/spaces/...`.

---

## Option D — Docker (Fly.io, Railway, etc.)

Build and run locally:

```bash
docker build -t nifty500-rsi .
docker run -p 8501:8501 -e PORT=8501 nifty500-rsi
```

Platforms that accept a Dockerfile can use the same image; set **`PORT`** to the value they inject.

---

## Optional: password “access” (shared secret)

If you set **`ACCESS_PASSWORD`**, visitors must enter it once per browser session before the app loads.

| Host | How to set |
|------|------------|
| **Streamlit Cloud** | App → **Settings** → **Secrets** → add `ACCESS_PASSWORD = "your-secret"` |
| **Render** | Service → **Environment** → add `ACCESS_PASSWORD` = your value |
| **Local** | Create `.streamlit/secrets.toml` (see `secrets.toml.example`) — file is gitignored |

Leave it unset for a **fully public** app (anyone with the URL).

---

## Notes

- **Who can open it:** Free tiers give you a **public URL**. A shared password (above) limits who can use the UI but is not enterprise-grade security (do not store truly sensitive data in the app).
- **Cold starts:** Free Render/HF small CPU tiers may be slow on first request after idle.
