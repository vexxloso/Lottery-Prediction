# Deploy Lottery-Prediction on Ubuntu VPS

Guide to run the **Lottery-Prediction** app (FastAPI backend + React frontend + MongoDB + Selenium scraping) on a **new Ubuntu VPS**.

---

## 1. VPS requirements

- **OS**: Ubuntu 22.04 LTS (or 24.04)
- **RAM**: 2 GB minimum (Selenium + Chrome need ~1 GB)
- **Disk**: 10 GB+
- **Ports**: 22 (SSH), 80/443 (web), 8000 if you run API on it before putting behind nginx

---

## 2. Initial server setup

SSH into the VPS:

```bash
ssh root@YOUR_VPS_IP
# or: ssh ubuntu@YOUR_VPS_IP
```

Update and install basics:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl build-essential
```

Optional: create a dedicated user (recommended):

```bash
sudo adduser lottery
sudo usermod -aG sudo lottery
su - lottery
```

---

## 3. Install dependencies

### 3.1 Node.js (for frontend)

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
node -v   # v20.x
npm -v
```

### 3.2 Python 3.11+ and venv

```bash
sudo apt install -y python3 python3-pip python3-venv
python3 --version   # 3.11+
```

### 3.3 MongoDB

```bash
# Add MongoDB repo (Ubuntu 22.04)
wget -qO- https://www.mongodb.org/static/pgp/server-7.0.asc | sudo tee /etc/apt/trusted.gpg.d/mongodb-server-7.0.asc
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt update
sudo apt install -y mongodb-org
sudo systemctl start mongod
sudo systemctl enable mongod
```

Check: `mongosh --eval "db.runCommand({ping:1})"` → `{ ok: 1 }`

### 3.4 Chrome (for Selenium scraping)

The backend and scripts use headless Chrome.

```bash
sudo apt install -y wget gnupg
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt update
sudo apt install -y google-chrome-stable
```

Verify: `google-chrome-stable --version`

---

## 4. Clone and configure the project

```bash
cd ~
git clone https://github.com/YOUR_USER/Lottery-Prediction.git
cd Lottery-Prediction
```

Replace `YOUR_USER` with your Git username or use your real repo URL.

### 4.1 Backend environment

```bash
cd ~/Lottery-Prediction/backend
nano .env
```

Create `.env` with:

```env
MONGO_URI=mongodb://localhost:27017
MONGO_DB=lottery
```

If MongoDB is on another host or has auth, set `MONGO_URI` accordingly (e.g. `mongodb://user:pass@host:27017`).

### 4.2 Backend Python environment

```bash
cd ~/Lottery-Prediction/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Test run:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
# In another terminal: curl http://localhost:8000/api/health
# Then Ctrl+C
```

### 4.3 Frontend build (with API URL for production)

Set the API base URL to your backend (replace with your real domain or VPS IP):

```bash
cd ~/Lottery-Prediction/frontend
npm ci
```

Build with the production API URL (use your domain or VPS IP):

```bash
# If backend will be at https://api.yourdomain.com:
VITE_API_URL=https://api.yourdomain.com npm run build

# Or if same host, e.g. API at http://YOUR_VPS_IP:8000:
VITE_API_URL=http://YOUR_VPS_IP:8000 npm run build
```

Output will be in `frontend/dist`.

---

## 5. Production CORS (backend)

The backend currently allows only `localhost` origins. For the browser to call the API from your frontend domain, add your frontend origin.

Edit `backend/main.py` and add your frontend URL to `allow_origins`:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        # Add your production frontend URL(s):
        "https://yourdomain.com",
        "https://www.yourdomain.com",
        "http://YOUR_VPS_IP",   # if testing by IP
    ],
    ...
)
```

Restart the backend after changes.

---

## 6. Run with systemd (recommended)

### 6.1 Backend service

```bash
sudo nano /etc/systemd/system/lottery-backend.service
```

Paste (adjust paths and user if needed):

```ini
[Unit]
Description=Lottery Prediction API
After=network.target mongod.service

[Service]
Type=simple
User=lottery
WorkingDirectory=/home/lottery/Lottery-Prediction/backend
Environment="PATH=/home/lottery/Lottery-Prediction/backend/.venv/bin"
ExecStart=/home/lottery/Lottery-Prediction/backend/.venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

If you kept `root` or another user, replace `User=` and paths accordingly.

```bash
sudo systemctl daemon-reload
sudo systemctl enable lottery-backend
sudo systemctl start lottery-backend
sudo systemctl status lottery-backend
```

### 6.2 Serve frontend with Nginx

Install Nginx:

```bash
sudo apt install -y nginx
```

Create a site config (replace `yourdomain.com` and paths):

```bash
sudo nano /etc/nginx/sites-available/lottery
```

Example (API proxy + static frontend on same server):

```nginx
server {
    listen 80;
    server_name yourdomain.com www.yourdomain.com;
    root /home/lottery/Lottery-Prediction/frontend/dist;
    index index.html;
    location / {
        try_files $uri $uri/ /index.html;
    }
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

If you use only IP for testing:

```nginx
server {
    listen 80;
    server_name YOUR_VPS_IP;
    root /home/lottery/Lottery-Prediction/frontend/dist;
    index index.html;
    location / {
        try_files $uri $uri/ /index.html;
    }
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable and reload:

```bash
sudo ln -s /etc/nginx/sites-available/lottery /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

If you use Nginx as reverse proxy, build the frontend with the **same host** so the browser sends requests to the same origin:

```bash
# So frontend calls /api/ on same domain (no CORS needed for same-origin)
VITE_API_URL=  # empty or same as site URL
# Or: VITE_API_URL=https://yourdomain.com
npm run build
```

Then CORS can stay as-is for same-origin; only add origins if the frontend is on a different domain.

---

## 7. Daily scrape (optional)

Scraping runs at 00:02 and fills MongoDB. Two options.

### Option A: Cron calling the API

Backend must be running. Add to crontab:

```bash
crontab -e
```

Add line (adjust URL if needed):

```
2 0 * * * curl -X POST http://127.0.0.1:8000/api/scrape/daily
```

### Option B: Run the Python daily script (no API needed)

Uses MongoDB and Chrome directly. From project root:

```bash
cd ~/Lottery-Prediction/scripts
# Use backend venv so deps are available
../backend/.venv/bin/python run_daily_scrape.py
```

To run once per day at 00:02 with cron:

```bash
2 0 * * * cd /home/lottery/Lottery-Prediction/scripts && /home/lottery/Lottery-Prediction/backend/.venv/bin/python run_daily_scrape.py
```

Note: `run_daily_scrape.py` loops and sleeps until 00:02; for cron you may prefer a "run once" script or the API endpoint so cron only triggers one run per day.

---

## 8. Firewall

```bash
sudo ufw allow 22
sudo ufw allow 80
sudo ufw allow 443
sudo ufw enable
```

---

## 9. HTTPS (recommended)

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d yourdomain.com -d www.yourdomain.com
```

After that, use `https://yourdomain.com` in `VITE_API_URL` if the frontend is on a different subdomain, and add that origin to CORS.

---

## 10. Quick checklist

| Step | Command / action |
|------|-------------------|
| Node 20 | `node -v` |
| Python 3.11+ | `python3 --version` |
| MongoDB | `sudo systemctl status mongod` |
| Chrome | `google-chrome-stable --version` |
| Backend .env | `MONGO_URI`, `MONGO_DB` in `backend/.env` |
| Backend run | `uvicorn main:app --host 0.0.0.0 --port 8000` then systemd |
| Frontend build | `VITE_API_URL=... npm run build` in `frontend/` |
| CORS | Add production frontend origin in `backend/main.py` if different domain |
| Nginx | Serve `frontend/dist` and proxy `/api/` to `127.0.0.1:8000` |
| Daily scrape | Cron: `POST /api/scrape/daily` or run `scripts/run_daily_scrape.py` |

---

## 11. Restart backend after code changes

When you change the backend and push to Git, the VPS won’t see it until you pull and restart.

### Manual: run a deploy script (recommended)

On the VPS, from the repo root:

```bash
cd ~/Lottery-Prediction   # or /root/Lottery-Prediction if you use root
sudo chmod +x scripts/deploy-backend.sh
sudo ./scripts/deploy-backend.sh
```

The script runs `git pull`, installs backend deps, and restarts `lottery-backend`. Do this after each push that changes the backend.

### Optional: auto pull and restart with cron

To have the VPS pull and restart the backend every 5 minutes only when there are new commits:

```bash
crontab -e
```

Add (adjust path if your repo is not in `/root/Lottery-Prediction`):

```
*/5 * * * * cd /root/Lottery-Prediction && git fetch -q && [ -n "$(git rev-list HEAD..origin/main 2>/dev/null)" ] && git pull -q && /root/Lottery-Prediction/backend/.venv/bin/pip install -q -r /root/Lottery-Prediction/backend/requirements.txt && systemctl restart lottery-backend
```

Or use the script in the repo (run every 5 minutes):

```bash
chmod +x /root/Lottery-Prediction/scripts/auto-deploy-backend.sh
crontab -e
```

Add (adjust path if your repo is elsewhere):

```
*/5 * * * * /root/Lottery-Prediction/scripts/auto-deploy-backend.sh
```

After you push from your PC, the backend will update and restart within about 5 minutes.

---

## 12. Troubleshooting

- **Backend exits with code 3 (activating/auto-restart)**: The real error is in the logs. Run:
  ```bash
  journalctl -u lottery-backend -n 80 --no-pager
  ```
  Or run the app by hand to see the traceback:
  ```bash
  cd /root/Lottery-Prediction/backend   # or your backend path
  .venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
  ```
  Common causes: (1) **MongoDB not running** — start with `sudo systemctl start mongod` and ensure `MONGO_URI` in `.env` is correct (e.g. `mongodb://localhost:27017`). (2) **Missing .env** — create `backend/.env` with at least `MONGO_URI` and `MONGO_DB=lottery`. (3) **Import error** — ensure you installed deps with `pip install -r requirements.txt` inside the backend venv.
- **502 from /api/**: Backend not running or wrong port. Check `systemctl status lottery-backend` and `curl http://127.0.0.1:8000/api/health`.
- **CORS errors in browser**: Add the exact frontend origin (scheme + host) to `allow_origins` in `main.py`.
- **Scrape fails (Chrome/Selenium)**: Ensure Chrome is installed and the app runs with a user that can start Chrome; check logs for "Chrome" or "webdriver" errors.
- **MongoDB connection**: Ensure `MONGO_URI` is correct and MongoDB is listening (`ss -tlnp | grep 27017`).

Done. Your Lottery-Prediction app should be running on the Ubuntu VPS with API, frontend, and optional daily scrape.

---

## 13. Test from your Windows PC (no domain)

Use your **VPS public IP** (e.g. `203.0.113.50`). Replace it in the steps below.

### Open the app in a browser

1. On your Windows PC, open **Chrome** or **Edge**.
2. In the address bar type: **`http://YOUR_VPS_IP`** (e.g. `http://203.0.113.50`).
3. Press Enter. You should see the Lottery-Prediction frontend (dashboard, resultados, etc.).

### Quick API check (optional)

In **PowerShell** on Windows:

```powershell
# Replace YOUR_VPS_IP with your real VPS IP
Invoke-WebRequest -Uri "http://YOUR_VPS_IP/api/health" -UseBasicParsing | Select-Object -ExpandProperty Content
```

You should see: `{"status":"ok"}`.

### If it doesn’t load

| Problem | What to check |
|--------|----------------|
| Page not loading | VPS firewall: `sudo ufw allow 80` and your provider’s firewall/security group allows port 80. |
| “Can’t connect” | Ping from Windows: `ping YOUR_VPS_IP`. If it fails, the IP may be wrong or the VPS down. |
| Blank page or API errors | Open DevTools (F12) → Console/Network. If CORS errors, add `http://YOUR_VPS_IP` to `allow_origins` in `backend/main.py` and restart the backend. |
| 502 Bad Gateway | On VPS: `sudo systemctl status lottery-backend` and `curl http://127.0.0.1:8000/api/health`. |

### If you didn’t set up Nginx yet

- **API only**: Open port 8000 on the VPS (`sudo ufw allow 8000`) and in the browser go to **`http://YOUR_VPS_IP:8000/docs`** to use the Swagger UI.
- **Full app**: You need Nginx (or another way) to serve the frontend and proxy `/api/` to the backend, then use **`http://YOUR_VPS_IP`** as above.
