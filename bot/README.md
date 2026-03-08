# Lottery buy-queue bots

Bots run on a separate device: they claim jobs from the API, open Chrome on loteriasyapuestas.es, fill and buy tickets, then report success/failure.

## One process for all (recommended)

From the `bot/` directory:

```bash
python run_bot.py
```

Polls **El Gordo**, **Euromillones** and **La Primitiva** queues and runs the first job that claims. One Chrome, one terminal.

Run only one lottery:

```bash
python run_bot.py --lottery el_gordo
python run_bot.py --lottery euromillones
python run_bot.py --lottery la_primitiva
```

## Single-lottery scripts

You can still run one lottery per process:

```bash
python el_gordo.py
python euromillones.py
python la_primitiva.py
```

## Env (`bot/.env`)

- `API_URL` – backend base URL (e.g. `http://localhost:8000`)
- `LOTTERY_LOGIN_USERNAME` / `LOTTERY_LOGIN_PASSWORD` – loteriasyapuestas.es login (form is filled; you click “Log in”)
- `LOTTERY_BOT_HEADLESS` – `false` to see Chrome (e.g. on Windows)
- `CHROMEDRIVER_PATH` – optional path to ChromeDriver

On stop (Ctrl+C) or crash, the current job is marked **failed** in the API.
