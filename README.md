# Lottery Prediction Platform

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB)](https://react.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white)](https://www.selenium.dev/)

Smart lottery analysis and ticket management for **Euromillones**, **La Primitiva**, and **El Gordo**.

This platform is built to help users make better betting decisions using data, historical analysis, and simulation-backed workflows.

---

## What Is This System? 🎯

This is a full workflow platform that combines:

- historical draw data processing
- model-based candidate generation
- ticket ranking and selection
- queue-based buying operations
- export and reporting tools

The core idea is **optimization of ticket ordering and strategy**, not random guessing.  
The system helps you test and compare how different ticket volumes perform over time, including jackpot and secondary prize outcomes.

---

## Key Capabilities 🚀

- Supports 3 lotteries: `Euromillones`, `La Primitiva`, `El Gordo`
- Builds candidate pools for selected draws
- Lets you create baskets manually, randomly, by count, or by range
- Sends baskets to buy queue for bot-assisted purchase flow
- Tracks queue statuses (`waiting`, `in_progress`, `bought`, `failed`)
- Exports queue data to `CSV`, `TXT`, and print-ready `PDF`
- Provides post-draw style analysis to evaluate betting efficiency

---

## Tech Stack 🧰

<p align="center">
  <img src="https://skillicons.dev/icons?i=python,fastapi,react,ts,selenium,docker,git,vscode" alt="Core stack icons" />
</p>

### Related Skills (Project Stack Only)

<p align="center">
  <img src="https://img.shields.io/badge/Python-Expert-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/FastAPI-Backend-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/React-Frontend-20232A?style=for-the-badge&logo=react&logoColor=61DAFB" alt="React" />
  <img src="https://img.shields.io/badge/TypeScript-Frontend-3178C6?style=for-the-badge&logo=typescript&logoColor=white" alt="TypeScript" />
  <img src="https://img.shields.io/badge/Selenium-Automation-43B02A?style=for-the-badge&logo=selenium&logoColor=white" alt="Selenium" />
  <img src="https://img.shields.io/badge/Docker-Deployment-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Git-Version%20Control-F05032?style=for-the-badge&logo=git&logoColor=white" alt="Git" />
</p>

- **Backend:** Python + FastAPI API services
- **Frontend:** React + TypeScript UI
- **Automation:** Selenium purchase bot
- **Ops/Tooling:** Docker and Git workflow

---

## How To Use 📘

### 1) Start the backend API

```bash
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 2) Start and open the frontend

Run the frontend app and open it in your browser.

### 3) Choose your lottery

Go to the betting/results area and pick one:

- Euromillones
- La Primitiva
- El Gordo

### 4) Generate candidate tickets

Use the prediction workflow to load or generate candidate tickets for your selected draw.

### 5) Build your basket

Add tickets using:

- click-to-add from candidates
- random selection
- buy-by-count
- buy-by-range

### 6) Queue your tickets

Send the basket to the buy queue so bot/automation can process it.

### 7) Monitor queue progress

Watch status updates and repair/remove queue items when needed.

### 8) Export your queue

Use **Export queue** to generate:

- `CSV`
- `TXT`
- `PDF` (print view)

### 9) Review outcomes and iterate

Check saved/bought tickets and repeat the cycle with improved strategy.

---

## Daily Automation (01:00) 🤖

There is an automation runner for prediction + compare orchestration:

- Script: `scripts/run_daily_prediction_automation.py`
- Reads latest row from each lottery `feature-model` endpoint (`id_sorteo`, `pre_id_sorteo`)
- Ensures:
  - `train/run-pipeline` completed
  - `train/full-wheel` completed
  - `compare/full-wheel/reorder` executed

Run once:

```bash
python scripts/run_daily_prediction_automation.py --api-url http://localhost:8000 --once
```

Run continuously (executes now, then every day at local `01:00`):

```bash
python scripts/run_daily_prediction_automation.py --api-url http://localhost:8000
```

---

## Project Structure (High Level) 🗂️

- `frontend/` - user interface and export views
- `backend/` - API endpoints and orchestration logic
- `bot/` - browser automation for queue purchase flows
- `scripts/` - data preparation, backfill, and model pipeline scripts
- `docs/` - product and technical documentation

---

## Contact 📬

- Telegram: [`@riora_1`](https://t.me/riora_1)
