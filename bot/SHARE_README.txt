Lottery bot — one exe, no Python needed

1. Put bot.exe and .env in the same folder.
2. Edit .env with your API URL and lottery site login (see below).
3. Install Chrome on this PC.
4. Run bot.exe (double-click or from command line).

  bot.exe              → runs all three lotteries (El Gordo, Euromillones, La Primitiva)
  bot.exe --lottery el_gordo
  bot.exe --lottery euromillones
  bot.exe --lottery la_primitiva

.env (same folder as bot.exe):
  API_URL=http://your-server:8000
  BOT_CREDENTIALS_SECRET=your-secret   (same as backend; then bot uses active account from app "Cuentas bot" page)
  (or use LOTTERY_LOGIN_USERNAME and LOTTERY_LOGIN_PASSWORD if you don't use DB accounts)
  LOTTERY_BOT_HEADLESS=false
  CHROMEDRIVER_PATH=

When a job runs, Chrome opens; the bot fills the form and stops at the login page for you to click "Log in". Then it continues and clicks the final buy button.
