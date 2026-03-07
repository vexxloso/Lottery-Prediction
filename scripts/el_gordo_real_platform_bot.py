"""
El Gordo real platform (loteriasyapuestas.es) automation.
Runs when POST /api/el-gordo/betting/open-real-platform is called.

=== Headless Chrome workflow (exact order) ===

1. Open betting page.
2. Accept cookie (Cookiebot "Allow all").
3. Fill tickets (5 main numbers + 1 clave per ticket), with human-like delays between each action.
4. Click "CONFIRMA TU APUESTA".
5. Accept cookie again if it appears after the confirm step.
6. Input login info (username, password from .env) and sign in.
7. Click JUEGA (#submitFresguardoCompra).
8. Check for success page; if detected, return bought=True (backend adds to bought_tickets).

Delays are used throughout so headless behaviour looks human (e.g. after page load, between numbers,
before/after confirm, before login typing, before JUEGA).

=== Payment ===

This platform uses the account balance when you are signed in: after JUEGA there is no payment
screen – the bet is charged to your account. So once login succeeds, the flow is: JUEGA → 
confirmation page. If tickets still aren't detected as purchased, possible causes: login not
completing in headless, success page text/URL different from what we check, or slow redirect.
We verify login (wait for JUEGA button), widen success detection, and retry after JUEGA.

ChromeDriver: prefers env CHROMEDRIVER_PATH if set; else Selenium built-in manager.

=== VPS setup (fix "Could not start ChromeDriver") ===

1) Install Chrome or Chromium and dependencies (Ubuntu/Debian):

   # Chrome (recommended)
   sudo apt-get update
   sudo apt-get install -y wget gnupg
   wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg
   echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
   sudo apt-get update
   sudo apt-get install -y google-chrome-stable

   # Or Chromium instead:
   # sudo apt-get install -y chromium-browser

   sudo apt-get install -y libgbm1 libasound2 libnss3 libxss1 libxtst6 fonts-liberation

2) Install chromedriver matching your Chrome version:

   # Option A: Use the chromedriver package (version may lag)
   sudo apt-get install -y chromium-chromedriver
   # Then set in .env: CHROMEDRIVER_PATH=/usr/bin/chromedriver  (or wherever it is: which chromedriver)

   # Option B: Download from Chrome for Testing (match your chrome version: google-chrome --version)
   # https://googlechromelabs.github.io/chrome-for-testing/  → Linux64 chromedriver, extract and set CHROMEDRIVER_PATH

3) Optional for headed mode: Xvfb + DISPLAY=:99 (see project docs).

Env (in .env): LOTTERY_LOGIN_USERNAME, LOTTERY_LOGIN_PASSWORD.
  LOTTERY_BOT_HEADLESS=false  → visible Chrome on PC for testing; default true (headless) for VPS.
"""
import logging
import os
import random
import sys
import time
from typing import Callable, Optional, Tuple

# Load .env from project root or backend so LOTTERY_LOGIN_* are available
try:
    from dotenv import load_dotenv
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for _path in (os.path.join(_root, ".env"), os.path.join(_root, "backend", ".env")):
        if os.path.isfile(_path):
            load_dotenv(_path)
            break
    else:
        load_dotenv()
except Exception:
    pass

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)

EL_GORDO_APUESTA_URL = "https://juegos.loteriasyapuestas.es/jugar/gordo-primitiva/apuesta"
WAIT_TIMEOUT = 15
LOGIN_WAIT_TIMEOUT = 8   # max wait for login form to appear after confirm

# Human-like delays (seconds, min–max) so headless looks like a human
DELAY_AFTER_PAGE_LOAD = (1.8, 3.2)      # after opening betting page, "reading"
DELAY_AFTER_COOKIE = (0.6, 1.2)         # after accept cookie
DELAY_BETWEEN_NUMBERS = (0.35, 0.75)    # between each number click
DELAY_AFTER_CLAVE = (0.4, 0.9)          # after selecting clave
DELAY_BETWEEN_TICKETS = (1.0, 2.0)      # before next ticket
DELAY_BEFORE_CONFIRM = (1.0, 2.0)       # before clicking confirm (reviewing bet)
DELAY_AFTER_CONFIRM = (2.0, 3.5)        # after confirm, wait for next page
DELAY_AFTER_COOKIE_POST_CONFIRM = (0.5, 1.2)
DELAY_BEFORE_LOGIN_TYPING = (0.5, 1.0)
DELAY_BETWEEN_USER_PASS = (0.3, 0.7)
DELAY_BEFORE_SIGNIN_CLICK = (0.5, 1.0)
DELAY_AFTER_LOGIN = (2.0, 3.5)
DELAY_BEFORE_JUEGA = (0.8, 1.8)
DELAY_AFTER_JUEGA_BEFORE_CHECK = (3.0, 5.0)
JUEGA_SUCCESS_CHECK_RETRIES = 3
JUEGA_SUCCESS_CHECK_INTERVAL = (2.0, 3.0)


def _human_delay(min_sec: float, max_sec: float) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


def _do_login(driver: webdriver.Chrome) -> bool:
    """
    If login form is present, click "Allow all" first, then fill username/password from env and click LOG IN.
    Returns True if login was performed, False if form not found or credentials missing.
    """
    username = (os.environ.get("LOTTERY_LOGIN_USERNAME") or "").strip()
    password = (os.environ.get("LOTTERY_LOGIN_PASSWORD") or "").strip()
    if not username or not password:
        logger.warning("LOTTERY_LOGIN_USERNAME / LOTTERY_LOGIN_PASSWORD not set in .env; skipping login")
        return False
    wait = WebDriverWait(driver, LOGIN_WAIT_TIMEOUT)
    # Wait for login page: either cookie dialog or username field
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#username, #CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll, #CybotCookiebotDialogBodyButtonsWrapper")))
    except Exception:
        return False
    _human_delay(*DELAY_BEFORE_LOGIN_TYPING)
    # Must click "Allow all" before touching username/password – wait for button and click
    try:
        allow_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"))
        )
        driver.execute_script("arguments[0].click();", allow_btn)
        _human_delay(*DELAY_AFTER_COOKIE)
    except Exception:
        pass
    # Now wait for login form and fill
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#username")))
    except Exception:
        return False
    _human_delay(*DELAY_BEFORE_LOGIN_TYPING)
    try:
        user_el = driver.find_element(By.CSS_SELECTOR, "#username")
        pass_el = driver.find_element(By.CSS_SELECTOR, "#password")
        user_el.clear()
        user_el.send_keys(username)
        _human_delay(*DELAY_BETWEEN_USER_PASS)
        pass_el.clear()
        pass_el.send_keys(password)
        _human_delay(*DELAY_BEFORE_SIGNIN_CLICK)
        btn = driver.find_element(By.CSS_SELECTOR, "#btnLogin")
        # Page may keep button disabled until input events fire; trigger and/or force click
        driver.execute_script("arguments[0].removeAttribute('disabled');", btn)
        btn.click()
        _human_delay(*DELAY_AFTER_LOGIN)
        logger.info("el_gordo_real_platform_bot: submitted login form")
        return True
    except Exception as e:
        logger.warning("el_gordo_real_platform_bot: login form fill/click failed: %s", e)
        return False


def _click_cookiebot_allow_all(driver: webdriver.Chrome) -> bool:
    """Click Cookiebot 'Allow all' button if present. Returns True if clicked."""
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"))
        )
        driver.execute_script("arguments[0].click();", btn)
        _human_delay(0.4, 0.8)
        logger.info("el_gordo_real_platform_bot: clicked Cookiebot Allow all")
        return True
    except Exception:
        return False


def _click_juega_if_present(driver: webdriver.Chrome) -> bool:
    """If JUEGA button (#submitFresguardoCompra) is present, click it. Returns True if clicked."""
    try:
        juega = WebDriverWait(driver, LOGIN_WAIT_TIMEOUT).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#submitFresguardoCompra"))
        )
        _human_delay(0.5, 1.2)
        driver.execute_script("arguments[0].click();", juega)
        logger.info("el_gordo_real_platform_bot: clicked JUEGA")
        return True
    except Exception:
        return False


def create_chrome_driver() -> webdriver.Chrome:
    """Create Chrome. Headless by default (VPS); set LOTTERY_BOT_HEADLESS=false for visible browser on PC."""
    headless = os.environ.get("LOTTERY_BOT_HEADLESS", "true").strip().lower() not in ("0", "false", "no")
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,800")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    options.page_load_strategy = "eager"
    # Reduce headless/automation detection by the site
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    if headless and sys.platform.startswith("linux"):
        options.add_argument("--disable-setuid-sandbox")
        try:
            import shutil
            for path in ("/usr/bin/google-chrome", "/usr/bin/google-chrome-stable", "/usr/bin/chromium", "/usr/bin/chromium-browser"):
                if shutil.which(path):
                    options.binary_location = path
                    break
        except Exception:
            pass

    driver = None
    chromedriver_path = os.environ.get("CHROMEDRIVER_PATH", "").strip()
    if chromedriver_path and os.path.isfile(chromedriver_path):
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
    if driver is None:
        try:
            driver = webdriver.Chrome(options=options)
        except Exception:
            pass
    if driver is None:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            raise RuntimeError(
                "Could not start ChromeDriver. On VPS: install Chrome/Chromium and chromedriver (see script docstring), "
                "or set CHROMEDRIVER_PATH to the chromedriver binary. Original error: %s" % (e,)
            ) from e

    driver.set_page_load_timeout(60)
    driver.set_script_timeout(30)
    # Run script on every new page so site is less likely to detect automation
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": (
                "Object.defineProperty(navigator, 'webdriver', { get: function () { return undefined; } });"
                "window.chrome = window.chrome || { runtime: {} };"
            )
        })
    except Exception as e:
        logger.debug("el_gordo_real_platform_bot: CDP webdriver override failed (non-fatal): %s", e)
    return driver


def _detect_purchase_success(driver: webdriver.Chrome) -> Tuple[bool, str]:
    """
    After clicking JUEGA, check what page we're on. Account pays automatically when logged in
    (no payment screen). Returns (True, msg) if success/confirmation page.
    """
    _human_delay(DELAY_AFTER_JUEGA_BEFORE_CHECK[0], DELAY_AFTER_JUEGA_BEFORE_CHECK[1])
    url_success_keys = (
        "confirmacion", "resumen", "exito", "compra", "guardado", "recibo", "apuesta-realizada",
        "apuesta/confirmacion", "operacion", "validada",
    )
    body_success_phrases = (
        "apuesta realizada",
        "apuesta ha sido",
        "ha sido registrada",
        "resumen de tu apuesta",
        "confirmación",
        "tu apuesta se ha",
        "apuesta registrada",
        "operación realizada",
        "correctamente",
        "número de operación",
        "resumen de la apuesta",
        "tu apuesta ha sido",
    )
    try:
        url = (driver.current_url or "").lower()
        body_text = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
        if any(k in url for k in url_success_keys):
            return (True, "Compra confirmada.")
        if any(phrase in body_text for phrase in body_success_phrases):
            return (True, "Compra confirmada.")
    except Exception as e:
        logger.debug("el_gordo_real_platform_bot: success check failed: %s", e)

    # Retry after short waits (redirect may be slow)
    for _ in range(JUEGA_SUCCESS_CHECK_RETRIES - 1):
        _human_delay(JUEGA_SUCCESS_CHECK_INTERVAL[0], JUEGA_SUCCESS_CHECK_INTERVAL[1])
        try:
            url = (driver.current_url or "").lower()
            body_text = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
            if any(k in url for k in url_success_keys):
                return (True, "Compra confirmada.")
            if any(phrase in body_text for phrase in body_success_phrases):
                return (True, "Compra confirmada.")
        except Exception:
            pass

    # Log what we actually see so we can add the right phrase if needed
    try:
        url = driver.current_url or ""
        body_snippet = (driver.find_element(By.TAG_NAME, "body").text or "")[:400]
        logger.info(
            "el_gordo_real_platform_bot: no success page detected. url=%s body_snippet=%s",
            url, body_snippet.replace("\n", " ").strip(),
        )
    except Exception:
        pass
    return (False, "No se detectó página de confirmación. Si ya compraste, usa «Añadir a guardados».")


def run_el_gordo_real_platform_bot(
    tickets: list,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> dict:
    """
    Open Chrome, go to El Gordo bet page, fill each ticket (5 mains + 1 clave), click confirm, JUEGA.
    tickets: list of { "mains": [int, ...], "clave": int }. Max 6 tickets.
    progress_callback(step: str): optional, called with progress messages for UI.
    Returns {"bought": True} if purchase succeeded (confirmation detected), else {"bought": False}.
    """
    def _progress(msg: str) -> None:
        if progress_callback:
            try:
                progress_callback(msg)
            except Exception:
                pass

    if not tickets or len(tickets) > 6:
        logger.warning("el_gordo_real_platform_bot: need 1–6 tickets, got %s", len(tickets or []))
        return {"bought": False}
    driver = None
    try:
        # --- Exact workflow: open page -> accept cookie -> fill tickets -> confirm -> (accept cookie) -> login -> JUEGA ---
        _progress("Abriendo navegador")
        driver = create_chrome_driver()
        _progress("Cargando página de apuestas")
        driver.get(EL_GORDO_APUESTA_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        _human_delay(*DELAY_AFTER_PAGE_LOAD)

        # 1) Accept cookie (first thing on betting page)
        _progress("Aceptando cookies")
        try:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[id*='CybotCookiebot'], .CybotCookiebotScrollArea, #CybotCookiebotDialogBodyButtonsWrapper"))
            )
        except Exception:
            pass
        _human_delay(0.2, 0.5)
        _click_cookiebot_allow_all(driver)
        _human_delay(*DELAY_AFTER_COOKIE)

        _hide_cookiebot_script = """
            function hideCookiebot(doc) {
                var root = doc.getElementById('CybotCookiebotDialog');
                if (root) { root.remove(); return; }
                doc.querySelectorAll('[id*="CybotCookiebot"], [class*="CybotCookiebot"]').forEach(function(el) {
                    el.style.setProperty('display', 'none', 'important');
                    el.style.setProperty('pointer-events', 'none', 'important');
                });
            }
            hideCookiebot(document);
            document.querySelectorAll('iframe').forEach(function(f) { try { if (f.contentDocument) hideCookiebot(f.contentDocument); } catch (e) {} });
        """
        driver.execute_script(_hide_cookiebot_script)
        _human_delay(0.4, 0.8)

        # 2) Wait for number selectors and fill tickets
        _progress("Cargando selector de números")
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".botonera-combinaciones")))
        _human_delay(0.5, 1.0)
        _progress("Rellenando boletos")

        def safe_click(el):
            """Hide cookie dialog then click via JavaScript so overlays don't intercept."""
            driver.execute_script(_hide_cookiebot_script)
            driver.execute_script("arguments[0].click();", el)

        for idx, ticket in enumerate(tickets):
            mains = ticket.get("mains") or []
            clave = ticket.get("clave", 0)
            if len(mains) != 5:
                logger.warning("el_gordo_real_platform_bot: ticket %s invalid mains %s", idx, mains)
                continue

            # 5 main numbers (1–54): .botonera-combinaciones .boton-boleto[value="N"]
            combinaciones = driver.find_element(By.CSS_SELECTOR, ".botonera-combinaciones")
            for n in mains:
                btn = combinaciones.find_element(By.CSS_SELECTOR, f'button.boton-boleto[value="{int(n)}"]')
                safe_click(btn)
                _human_delay(*DELAY_BETWEEN_NUMBERS)

            # 1 clave (0–9): .botonera-num-clave .botones .boton-boleto[value="N"]
            clave_section = driver.find_element(By.CSS_SELECTOR, ".botonera-num-clave .botones")
            clave_btn = clave_section.find_element(By.CSS_SELECTOR, f'button.boton-boleto[value="{int(clave)}"]')
            safe_click(clave_btn)
            _human_delay(*DELAY_AFTER_CLAVE)

            if idx < len(tickets) - 1:
                _human_delay(*DELAY_BETWEEN_TICKETS)
            _progress(f"Boleto {idx + 1}/{len(tickets)} rellenado")

        # 3) Click CONFIRMA TU APUESTA
        _human_delay(*DELAY_BEFORE_CONFIRM)
        _progress("Confirmando apuesta")
        confirm = driver.find_element(By.CSS_SELECTOR, "button.boton-confirmar")
        safe_click(confirm)
        logger.info("el_gordo_real_platform_bot: filled %s tickets and clicked confirm", len(tickets))
        _human_delay(*DELAY_AFTER_CONFIRM)

        # 4) Accept cookie again if shown (e.g. after confirm page load)
        _progress("Aceptando cookies (si aparece)")
        _click_cookiebot_allow_all(driver)
        _human_delay(*DELAY_AFTER_COOKIE_POST_CONFIRM)

        # 5) Login: input username/password, sign in (account pays from balance – no payment screen)
        _progress("Iniciando sesión")
        login_done = _do_login(driver)
        _human_delay(*DELAY_AFTER_LOGIN)
        if login_done:
            _progress("Comprobando que la sesión está activa")
            try:
                WebDriverWait(driver, 12).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#submitFresguardoCompra"))
                )
                _human_delay(0.5, 1.0)
            except Exception:
                logger.warning("el_gordo_real_platform_bot: JUEGA button not found after login; page may still be loading or login failed")

        # 6) Click JUEGA
        _progress("Pulsando JUEGA")
        _human_delay(*DELAY_BEFORE_JUEGA)
        _click_juega_if_present(driver)
        bought, step_msg = _detect_purchase_success(driver)
        _progress("Finalizado. " + step_msg)
        if os.environ.get("LOTTERY_BOT_HEADLESS", "true").strip().lower() in ("0", "false", "no"):
            pass  # visible mode: leave browser open for payment
        else:
            try:
                driver.quit()
            except Exception as quit_err:
                logger.debug("driver.quit() ignored (browser may already be closed): %s", quit_err)
        return {"bought": bought}
    except Exception as e:
        logger.exception("el_gordo_real_platform_bot failed: %s", e)
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_tickets = [{"mains": [5, 16, 34, 40, 45], "clave": 9}]
    run_el_gordo_real_platform_bot(test_tickets)
    print("Done (Headless Chrome closed).")
