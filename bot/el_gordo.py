"""
El Gordo buy-queue bot for another device. Single file + .env only (no DB, no other scripts).

- POST /api/el-gordo/betting/bot/claim → get one waiting job.
- Run Selenium (inlined) to buy tickets on loteriasyapuestas.es.
- POST /api/el-gordo/betting/bot/complete → report success or failure.

On stop (Ctrl+C) or Chrome/Selenium crash → job is reported as failed.

Run: python bot.py   (from this folder; .env in same folder)
Env: API_URL, LOTTERY_LOGIN_USERNAME, LOTTERY_LOGIN_PASSWORD, LOTTERY_BOT_HEADLESS, CHROMEDRIVER_PATH
"""
import logging
import os
import random
import signal
import sys
import time
from typing import Callable, Optional, Tuple

# Load .env from this folder only
_this_dir = os.path.dirname(os.path.abspath(__file__))
_env = os.path.join(_this_dir, ".env")
if os.path.isfile(_env):
    try:
        from dotenv import load_dotenv
        load_dotenv(_env)
    except Exception:
        pass

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)

# --- API ---
API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
POLL_INTERVAL_SEC = 10
_current_queue_id: Optional[str] = None  # set while running a job; used to mark failed on stop


def _session():
    s = requests.Session()
    if "localhost" in API_URL or "127.0.0.1" in API_URL:
        s.trust_env = False
    return s


def claim_job():
    try:
        r = _session().post(f"{API_URL}/api/el-gordo/betting/bot/claim", timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("claim failed: %s", e)
        return None


def complete_job(queue_id: str, success: bool, error: Optional[str] = None):
    try:
        body = {"queue_id": queue_id, "success": success}
        if error:
            body["error"] = error
        _session().post(f"{API_URL}/api/el-gordo/betting/bot/complete", json=body, timeout=15)
    except Exception as e:
        logger.warning("complete failed: %s", e)


# --- Inlined Selenium (loteriasyapuestas.es El Gordo) ---
EL_GORDO_APUESTA_URL = "https://juegos.loteriasyapuestas.es/jugar/gordo-primitiva/apuesta"
WAIT_TIMEOUT = 15
WAIT_NUMBER_SELECTOR = 40  # number grid can load slowly after cookies
LOGIN_WAIT_TIMEOUT = 8
DELAY_AFTER_PAGE_LOAD = (1.8, 3.2)
DELAY_AFTER_COOKIE = (0.6, 1.2)
DELAY_BETWEEN_NUMBERS = (0.35, 0.75)
DELAY_AFTER_CLAVE = (0.4, 0.9)
DELAY_BETWEEN_TICKETS = (1.0, 2.0)
DELAY_BEFORE_CONFIRM = (1.0, 2.0)
DELAY_AFTER_CONFIRM = (2.0, 3.5)
DELAY_AFTER_COOKIE_POST_CONFIRM = (0.5, 1.2)
DELAY_BEFORE_LOGIN_TYPING = (0.15, 0.35)
DELAY_BETWEEN_USER_PASS = (0.1, 0.25)
DELAY_BEFORE_SIGNIN_CLICK = (0.15, 0.35)
DELAY_AFTER_LOGIN = (2.0, 3.5)
MANUAL_LOGIN_WAIT = (18.0, 25.0)
DELAY_BEFORE_JUEGA = (0.8, 1.8)
DELAY_AFTER_JUEGA_BEFORE_CHECK = (3.0, 5.0)
JUEGA_SUCCESS_CHECK_RETRIES = 3
JUEGA_SUCCESS_CHECK_INTERVAL = (2.0, 3.0)


def _human_delay(min_sec: float, max_sec: float) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


def _get_login_credentials() -> Tuple[str, str]:
    """Return (username, password): from API active-credentials (DB bot_credentials, is_active: true), else from env."""
    try:
        headers = {}
        secret = (os.environ.get("BOT_CREDENTIALS_SECRET") or "").strip()
        if secret:
            headers["X-Bot-Secret"] = secret
        r = _session().get(
            f"{API_URL}/api/bot/active-credentials",
            headers=headers or None,
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            u = (data.get("username") or "").strip()
            p = (data.get("password") or "").strip()
            if u and p:
                return (u, p)
        elif r.status_code in (401, 404):
            logger.info("Bot credentials API returned %s; using .env LOTTERY_LOGIN_*", r.status_code)
    except Exception as e:
        logger.warning("Failed to fetch active credentials from API: %s; using .env", e)
    u = (os.environ.get("LOTTERY_LOGIN_USERNAME") or "").strip()
    p = (os.environ.get("LOTTERY_LOGIN_PASSWORD") or "").strip()
    if not u or not p:
        logger.warning("No credentials: add active account in app (Cuentas bot) or set LOTTERY_LOGIN_USERNAME and LOTTERY_LOGIN_PASSWORD in .env")
    return (u, p)


def _set_input_value(driver: webdriver.Chrome, element, value: str) -> None:
    """Set input value so it sticks: JavaScript value + events, then send_keys."""
    driver.execute_script(
        "var el = arguments[0]; var v = arguments[1]; el.value = v; el.dispatchEvent(new Event('input', { bubbles: true })); el.dispatchEvent(new Event('change', { bubbles: true }));",
        element,
        value or "",
    )
    try:
        element.clear()
        element.send_keys(value or "")
    except Exception:
        pass


def _is_login_page_visible(driver: webdriver.Chrome, timeout: float = 3.0) -> bool:
    """Return True if the sign-in form (#username) is visible (user must log in). False if already logged in."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "#username"))
        )
        return True
    except Exception:
        return False


def _do_login(driver: webdriver.Chrome, username: str, password: str) -> bool:
    if not username or not password:
        logger.warning("No username/password passed to login; abort")
        return False
    wait = WebDriverWait(driver, LOGIN_WAIT_TIMEOUT)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#username, #CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")))
    except Exception:
        return False
    _human_delay(*DELAY_BEFORE_LOGIN_TYPING)
    try:
        allow_btn = driver.find_element(By.CSS_SELECTOR, "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")
        driver.execute_script("arguments[0].click();", allow_btn)
        _human_delay(0.2, 0.4)
    except Exception:
        pass
    try:
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#username")))
        user_el = driver.find_element(By.CSS_SELECTOR, "#username")
        pass_el = driver.find_element(By.CSS_SELECTOR, "#password")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", user_el)
        _human_delay(0.1, 0.2)
        print("Inputting username on login page:", username)
        print("Inputting password on login page:", password)
        _set_input_value(driver, user_el, username)
        _human_delay(*DELAY_BETWEEN_USER_PASS)
        _set_input_value(driver, pass_el, password)
        _human_delay(*DELAY_BEFORE_SIGNIN_CLICK)
        try:
            btn = driver.find_element(By.CSS_SELECTOR, "#btnLogin")
            driver.execute_script("arguments[0].removeAttribute('disabled');", btn)
        except Exception:
            pass
        logger.info("Filled username and password on login page; click LOG IN to continue")
        return True
    except Exception as e:
        logger.warning("Login form fill failed (username/password not input): %s", e)
        return False


def _click_cookiebot_allow_all(driver: webdriver.Chrome) -> bool:
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"))
        )
        driver.execute_script("arguments[0].click();", btn)
        _human_delay(0.4, 0.8)
        return True
    except Exception:
        return False


def _click_juega_if_present(driver: webdriver.Chrome) -> bool:
    try:
        juega = WebDriverWait(driver, LOGIN_WAIT_TIMEOUT).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#submitFresguardoCompra"))
        )
        _human_delay(0.5, 1.2)
        driver.execute_script("arguments[0].click();", juega)
        logger.info("clicked JUEGA")
        return True
    except Exception:
        return False


def _create_chrome_driver(force_visible: bool = False) -> webdriver.Chrome:
    # Default: visible Chrome on Windows (like queue bot), headless on Linux (VPS).
    # force_visible=True: always show browser (e.g. for manual login in run_bot).
    _default_headless = "false" if sys.platform.startswith("win") else "true"
    headless = False if force_visible else (
        os.environ.get("LOTTERY_BOT_HEADLESS", _default_headless).strip().lower() not in ("0", "false", "no")
    )
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--window-size=1280,800")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--no-first-run")
    options.add_argument("--mute-audio")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    options.page_load_strategy = "eager"
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    if headless and sys.platform.startswith("linux"):
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--remote-debugging-port=0")
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
    chromedriver_log = os.environ.get("CHROMEDRIVER_LOG_PATH", "").strip()
    try:
        if chromedriver_path and os.path.isfile(chromedriver_path):
            service_kw = {"executable_path": chromedriver_path}
            if chromedriver_log:
                service_kw["log_path"] = chromedriver_log
            driver = webdriver.Chrome(service=Service(**service_kw), options=options)
        if driver is None:
            try:
                driver = webdriver.Chrome(options=options)
            except Exception:
                pass
        if driver is None:
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    except Exception as e:
        err_lower = str(e).lower()
        if "session not created" in err_lower or "chrome instance exited" in err_lower:
            raise RuntimeError(
                "Chrome failed to start. Chrome and ChromeDriver must match. "
                "Set CHROMEDRIVER_PATH or install Chrome. Original: %s" % e
            ) from e
        raise

    driver.set_page_load_timeout(60)
    driver.set_script_timeout(30)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": (
                "Object.defineProperty(navigator, 'webdriver', { get: function () { return undefined; } });"
                "window.chrome = window.chrome || { runtime: {} };"
            )
        })
    except Exception:
        pass
    return driver


def _detect_purchase_success(driver: webdriver.Chrome) -> Tuple[bool, str]:
    _human_delay(DELAY_AFTER_JUEGA_BEFORE_CHECK[0], DELAY_AFTER_JUEGA_BEFORE_CHECK[1])
    url_success_keys = (
        "confirmacion", "resumen", "exito", "compra", "guardado", "recibo", "apuesta-realizada",
        "apuesta/confirmacion", "operacion", "validada",
    )
    body_success_phrases = (
        "apuesta realizada", "apuesta ha sido", "ha sido registrada", "resumen de tu apuesta",
        "confirmación", "tu apuesta se ha", "apuesta registrada", "operación realizada",
        "correctamente", "número de operación", "resumen de la apuesta", "tu apuesta ha sido",
    )
    try:
        url = (driver.current_url or "").lower()
        body_text = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
        if any(k in url for k in url_success_keys):
            return (True, "Compra confirmada.")
        if any(phrase in body_text for phrase in body_success_phrases):
            return (True, "Compra confirmada.")
    except Exception:
        pass
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
    return (False, "No se detectó página de confirmación.")


HIDE_COOKIEBOT_SCRIPT = """
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


def _run_selenium_buy(
    tickets: list,
    progress_callback: Optional[Callable[[str], None]] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    driver: Optional[webdriver.Chrome] = None,
) -> dict:
    def _progress(msg: str) -> None:
        if progress_callback:
            try:
                progress_callback(msg)
            except Exception:
                pass

    if not tickets or len(tickets) > 6:
        logger.warning("need 1–6 tickets, got %s", len(tickets or []))
        return {"bought": False}

    own_driver = driver is None
    if own_driver:
        if not username or not password:
            username, password = _get_login_credentials()
        if not username or not password:
            logger.warning("No credentials: stop. Set active account in app (Cuentas bot) or LOTTERY_LOGIN_USERNAME and LOTTERY_LOGIN_PASSWORD in .env")
            return {"bought": False, "error": "No credentials configured"}
        logger.info("Using credentials for username: %s", username)
    if own_driver:
        _progress("Abriendo navegador")
        driver = _create_chrome_driver()
    try:
        _progress("Cargando página de apuestas")
        driver.get(EL_GORDO_APUESTA_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        _human_delay(*DELAY_AFTER_PAGE_LOAD)

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
        driver.execute_script(HIDE_COOKIEBOT_SCRIPT)
        _human_delay(0.4, 0.8)

        _progress("Cargando selector de números")
        wait_numbers = WebDriverWait(driver, WAIT_NUMBER_SELECTOR)
        try:
            wait_numbers.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".botonera-combinaciones")))
        except Exception as e:
            try:
                driver.save_screenshot(os.path.join(_this_dir, "bot_timeout_screenshot.png"))
                logger.warning("Timeout waiting for number selector. Screenshot saved: bot_timeout_screenshot.png URL=%s", driver.current_url)
            except Exception:
                logger.warning("Timeout waiting for .botonera-combinaciones. Current URL: %s", driver.current_url)
            raise
        _human_delay(0.5, 1.0)
        _progress("Rellenando boletos")

        def safe_click(el):
            driver.execute_script(HIDE_COOKIEBOT_SCRIPT)
            driver.execute_script("arguments[0].click();", el)

        for idx, ticket in enumerate(tickets):
            mains = ticket.get("mains") or []
            clave = ticket.get("clave", 0)
            if len(mains) != 5:
                logger.warning("ticket %s invalid mains %s", idx, mains)
                continue
            combinaciones = driver.find_element(By.CSS_SELECTOR, ".botonera-combinaciones")
            for n in mains:
                btn = combinaciones.find_element(By.CSS_SELECTOR, f'button.boton-boleto[value="{int(n)}"]')
                safe_click(btn)
                _human_delay(*DELAY_BETWEEN_NUMBERS)
            clave_section = driver.find_element(By.CSS_SELECTOR, ".botonera-num-clave .botones")
            clave_btn = clave_section.find_element(By.CSS_SELECTOR, f'button.boton-boleto[value="{int(clave)}"]')
            safe_click(clave_btn)
            _human_delay(*DELAY_AFTER_CLAVE)
            if idx < len(tickets) - 1:
                _human_delay(*DELAY_BETWEEN_TICKETS)
            _progress(f"Boleto {idx + 1}/{len(tickets)} rellenado")

        _human_delay(*DELAY_BEFORE_CONFIRM)
        _progress("Confirmando apuesta")
        confirm = driver.find_element(By.CSS_SELECTOR, "button.boton-confirmar")
        safe_click(confirm)
        _human_delay(*DELAY_AFTER_CONFIRM)

        _progress("Aceptando cookies (si aparece)")
        _click_cookiebot_allow_all(driver)
        _human_delay(*DELAY_AFTER_COOKIE_POST_CONFIRM)

        if _is_login_page_visible(driver):
            if own_driver:
                _progress("Iniciando sesión")
                login_done = _do_login(driver, username, password)
                _human_delay(*MANUAL_LOGIN_WAIT)
                if not login_done:
                    logger.warning("Login failed: could not fill or submit username/password; stopping")
                    return {"bought": False, "error": "Login failed: could not fill username/password on page"}
            else:
                # Reused browser: user logs in manually once; do not auto-fill.
                logger.warning("Login page shown but browser is reused (manual login mode). Session may have expired.")
                return {"bought": False, "error": "Sesión expirada. Reinicia el bot (python run_bot.py) e inicia sesión en el navegador cuando se abra."}
        else:
            _progress("Sesión activa, omitiendo login")
        _progress("Comprobando sesión")
        try:
            WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#submitFresguardoCompra"))
            )
            _human_delay(0.5, 1.0)
        except Exception:
            logger.warning("JUEGA button not found after login")

        _progress("Pulsando JUEGA")
        _human_delay(*DELAY_BEFORE_JUEGA)
        _click_juega_if_present(driver)
        bought, step_msg = _detect_purchase_success(driver)
        _progress("Finalizado. " + step_msg)

        if own_driver:
            headless = os.environ.get("LOTTERY_BOT_HEADLESS", "true").strip().lower() not in ("0", "false", "no")
            if headless:
                try:
                    driver.quit()
                except Exception:
                    pass
        return {"bought": bought}
    except Exception as e:
        logger.exception("Selenium/Chrome failed: %s", e)
        if own_driver and driver:
            try:
                driver.quit()
            except Exception:
                pass
        raise


def run_buy(
    tickets: list,
    progress_callback: Optional[Callable[[str], None]] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    driver: Optional[webdriver.Chrome] = None,
) -> dict:
    """Public entry for combined runner. Runs Selenium buy flow; returns {bought: bool}. Optional driver to reuse one browser."""
    return _run_selenium_buy(tickets, progress_callback, username=username, password=password, driver=driver)


def _on_stop(*_args):
    """On Ctrl+C or SIGTERM: mark current job failed and exit."""
    global _current_queue_id
    if _current_queue_id:
        logger.info("Stopping: marking job %s as failed", _current_queue_id)
        complete_job(_current_queue_id, success=False, error="Bot stopped by user or signal")
        _current_queue_id = None
    sys.exit(0)


def main():
    global _current_queue_id
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger.info("El Gordo bot starting (single file, API only). API_URL=%s", API_URL)

    signal.signal(signal.SIGINT, _on_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _on_stop)

    while True:
        try:
            data = claim_job()
            if data and data.get("claimed") and data.get("queue_id"):
                queue_id = data["queue_id"]
                tickets = data.get("tickets") or []
                _current_queue_id = queue_id
                logger.info("Claimed job %s, tickets=%s", queue_id, len(tickets))
                try:
                    result = _run_selenium_buy(tickets, progress_callback=lambda s: logger.info("bot: %s", s))
                    success = result.get("bought") is True
                    complete_job(queue_id, success=success, error=None if success else "Bot did not report success")
                except Exception as e:
                    logger.exception("bot run failed: %s", e)
                    complete_job(queue_id, success=False, error=str(e))
                finally:
                    _current_queue_id = None
            else:
                time.sleep(POLL_INTERVAL_SEC)
        except KeyboardInterrupt:
            _on_stop()
        except Exception as e:
            logger.exception("loop: %s", e)
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
