# Lottery buy-queue bots

Bots run on a separate device: they claim jobs from the API, open Chrome on loteriasyapuestas.es, fill and buy tickets, then report success/failure.

## Build bot.exe (Windows)

From the `bot/` folder:

```bat
build_exe.bat
```

Requires Python and `pip install pyinstaller` (the script installs it if missing). Output: `dist\bot.exe`.

Copy **bot.exe** and your **.env** into the same folder on the PC where you run the bot. The exe reads `.env` from its own directory. Chrome must be installed on that PC.

Run:

- `bot.exe` — poll all three lotteries
- `bot.exe --lottery el_gordo`
- `bot.exe --lottery euromillones`
- `bot.exe --lottery la_primitiva`

## One process for all (Python, recommended)

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
- **DB accounts (recommended):** set `BOT_CREDENTIALS_SECRET` to the same value as the backend env. The bot then uses the **active** account from the app’s “Cuentas bot” page.
- **Or .env login:** `LOTTERY_LOGIN_USERNAME` / `LOTTERY_LOGIN_PASSWORD` – used if `BOT_CREDENTIALS_SECRET` is not set or API fails.
- `LOTTERY_BOT_HEADLESS` – `false` to see Chrome (e.g. on Windows)
- `CHROMEDRIVER_PATH` – optional path to ChromeDriver

On stop (Ctrl+C) or crash, the current job is marked **failed** in the API.
