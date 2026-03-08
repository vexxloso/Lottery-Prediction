"""
Euromillones buy-queue bot for another device. Single file + .env only (no DB).

- POST /api/euromillones/betting/bot/claim → get one waiting job.
- Run Selenium: juegos.loteriasyapuestas.es/jugar/euromillones/apuesta
  Cookie → click Tuesday (boton_diaria) → fill 5 mains + 2 stars per ticket →
  NEXT (#botonSiguiente) → NEXT (#siguiente_paso2) → cookie → login (user clicks) → BUY (#confirmarCompra).
- POST /api/euromillones/betting/bot/complete → report success or failure.

On stop (Ctrl+C) or Chrome crash → job reported as failed.

Run: python euromillones.py   (from bot folder; .env in same folder)
Env: API_URL, LOTTERY_LOGIN_USERNAME, LOTTERY_LOGIN_PASSWORD, LOTTERY_BOT_HEADLESS, CHROMEDRIVER_PATH
"""
import logging
import os
import random
import signal
import sys
import time
from typing import Callable, Optional, Tuple

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

API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
POLL_INTERVAL_SEC = 10
_current_queue_id: Optional[str] = None


def _session():
    s = requests.Session()
    if "localhost" in API_URL or "127.0.0.1" in API_URL:
        s.trust_env = False
    return s


def claim_job():
    try:
        r = _session().post(f"{API_URL}/api/euromillones/betting/bot/claim", timeout=15)
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
        _session().post(f"{API_URL}/api/euromillones/betting/bot/complete", json=body, timeout=15)
    except Exception as e:
        logger.warning("complete failed: %s", e)


EUROMILLONES_APUESTA_URL = "https://juegos.loteriasyapuestas.es/jugar/euromillones/apuesta"
WAIT_TIMEOUT = 15
WAIT_NUMBER_SELECTOR = 40
LOGIN_WAIT_TIMEOUT = 8
DELAY_AFTER_PAGE_LOAD = (1.8, 3.2)
DELAY_AFTER_COOKIE = (0.6, 1.2)
DELAY_BETWEEN_NUMBERS = (0.35, 0.75)
DELAY_BETWEEN_STARS = (0.35, 0.75)
DELAY_BETWEEN_TICKETS = (1.0, 2.0)
DELAY_AFTER_TAB_CLICK = (0.8, 1.5)
DELAY_BEFORE_NEXT = (1.0, 2.0)
DELAY_AFTER_NEXT = (2.0, 3.5)
DELAY_BEFORE_LOGIN_TYPING = (0.5, 1.0)
DELAY_BETWEEN_USER_PASS = (0.3, 0.7)
MANUAL_LOGIN_WAIT = (18.0, 25.0)
DELAY_BEFORE_BUY = (0.8, 1.8)
DELAY_AFTER_BUY_CHECK = (3.0, 5.0)
BUY_SUCCESS_CHECK_RETRIES = 3
BUY_SUCCESS_CHECK_INTERVAL = (2.0, 3.0)


def _human_delay(min_sec: float, max_sec: float) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


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


def _do_login(driver: webdriver.Chrome) -> bool:
    username = (os.environ.get("LOTTERY_LOGIN_USERNAME") or "").strip()
    password = (os.environ.get("LOTTERY_LOGIN_PASSWORD") or "").strip()
    if not username or not password:
        logger.warning("LOTTERY_LOGIN_USERNAME / LOTTERY_LOGIN_PASSWORD not set; skipping login")
        return False
    wait = WebDriverWait(driver, LOGIN_WAIT_TIMEOUT)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#username, #CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")))
    except Exception:
        return False
    _human_delay(*DELAY_BEFORE_LOGIN_TYPING)
    try:
        allow_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"))
        )
        driver.execute_script("arguments[0].click();", allow_btn)
        _human_delay(*DELAY_AFTER_COOKIE)
    except Exception:
        pass
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
        try:
            btn = driver.find_element(By.CSS_SELECTOR, "#btnLogin")
            driver.execute_script("arguments[0].removeAttribute('disabled');", btn)
        except Exception:
            pass
        logger.info("Filled username/password; click LOG IN manually")
        return True
    except Exception as e:
        logger.warning("Login form fill failed: %s", e)
        return False


def _create_chrome_driver() -> webdriver.Chrome:
    _default_headless = "false" if sys.platform.startswith("win") else "true"
    headless = os.environ.get("LOTTERY_BOT_HEADLESS", _default_headless).strip().lower() not in ("0", "false", "no")
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
                "Chrome failed to start. Chrome and ChromeDriver must match. Original: %s" % e
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
    _human_delay(DELAY_AFTER_BUY_CHECK[0], DELAY_AFTER_BUY_CHECK[1])
    url_success_keys = (
        "confirmacion", "resumen", "exito", "compra", "guardado", "recibo", "apuesta-realizada",
        "operacion", "validada",
    )
    body_success_phrases = (
        "apuesta realizada", "apuesta ha sido", "ha sido registrada", "resumen de tu apuesta",
        "confirmación", "apuesta registrada", "operación realizada", "correctamente",
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
    for _ in range(BUY_SUCCESS_CHECK_RETRIES - 1):
        _human_delay(BUY_SUCCESS_CHECK_INTERVAL[0], BUY_SUCCESS_CHECK_INTERVAL[1])
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


def _run_selenium_buy(tickets: list, progress_callback: Optional[Callable[[str], None]] = None) -> dict:
    def _progress(msg: str) -> None:
        if progress_callback:
            try:
                progress_callback(msg)
            except Exception:
                pass

    if not tickets or len(tickets) > 5:
        logger.warning("Euromillones: need 1–5 tickets, got %s", len(tickets or []))
        return {"bought": False}

    driver = None
    try:
        _progress("Abriendo navegador")
        driver = _create_chrome_driver()
        _progress("Cargando página Euromillones")
        driver.get(EUROMILLONES_APUESTA_URL)
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

        _progress("Seleccionando sorteo (Tuesday / uno)")
        try:
            tab_diaria = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#boton_diaria, button.tabs_inactive_left"))
            )
            driver.execute_script("arguments[0].click();", tab_diaria)
            _human_delay(*DELAY_AFTER_TAB_CLICK)
        except Exception as e:
            logger.warning("Could not click Tuesday tab (boton_diaria): %s", e)

        # Pattern from a.htm: main numbers button.botonBoleto[name="botonBoleto"] value 1-50;
        # stars button.botonBoleto[name="botonBoletoEstrellas"] value 1-12 inside div.estrellas;
        # after each stake click #confirmarColumna (CONTINUE).
        _progress("Cargando selector de números")
        wait_numbers = WebDriverWait(driver, WAIT_NUMBER_SELECTOR)
        try:
            wait_numbers.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#columna_boleto button.botonBoleto[name='botonBoleto'], #boton_num_1"))
            )
        except Exception:
            try:
                driver.save_screenshot(os.path.join(_this_dir, "euromillones_timeout_screenshot.png"))
                logger.warning("Timeout: no number panel. URL=%s", driver.current_url)
            except Exception:
                pass
            raise RuntimeError("No se encontró el selector de números (refer a.htm).")

        def safe_click(el):
            driver.execute_script(HIDE_COOKIEBOT_SCRIPT)
            driver.execute_script("arguments[0].click();", el)

        def click_main(n: int):
            try:
                btn = driver.find_element(By.CSS_SELECTOR, f"#boton_num_{n}")
                safe_click(btn)
                return
            except Exception:
                pass
            btn = driver.find_element(By.CSS_SELECTOR, f"#columna_boleto button.botonBoleto[name='botonBoleto'][value='{n}']")
            safe_click(btn)

        def click_star(s: int):
            try:
                btn = driver.find_element(By.CSS_SELECTOR, f"#boton_estrella_{s}")
                safe_click(btn)
                return
            except Exception:
                pass
            btn = driver.find_element(By.CSS_SELECTOR, "div.estrellas button.botonBoleto[name='botonBoletoEstrellas'][value='%s']" % s)
            safe_click(btn)

        _progress("Rellenando boletos")
        for idx, ticket in enumerate(tickets):
            mains = ticket.get("mains") or []
            stars = ticket.get("stars") or []
            if len(mains) != 5 or len(stars) != 2:
                logger.warning("Ticket %s invalid: mains=%s stars=%s", idx, mains, stars)
                continue
            for n in mains:
                click_main(int(n))
                _human_delay(*DELAY_BETWEEN_NUMBERS)
            for s in stars:
                click_star(int(s))
                _human_delay(*DELAY_BETWEEN_STARS)
            _progress(f"Boleto {idx + 1}/{len(tickets)} rellenado")
            # CONTINUE (confirmarColumna) after each stake: 5 mains + 2 stars → click CONTINUE (a.htm 501-506)
            _human_delay(0.4, 0.9)
            try:
                cont = driver.find_element(By.CSS_SELECTOR, "#confirmarColumna")
                safe_click(cont)
                _human_delay(*DELAY_BETWEEN_TICKETS)
            except Exception as e:
                logger.warning("Could not click CONTINUE (#confirmarColumna): %s", e)

        _human_delay(*DELAY_BEFORE_NEXT)
        _progress("Pulsando NEXT")
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "#botonSiguiente")
            safe_click(next_btn)
        except Exception:
            next_btn = driver.find_element(By.CSS_SELECTOR, "button#botonSiguiente")
            safe_click(next_btn)
        _human_delay(*DELAY_AFTER_NEXT)

        _progress("Pulsando NEXT (paso 2)")
        try:
            next2 = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#siguiente_paso2, button.siguiente_paso2"))
            )
            safe_click(next2)
        except Exception as e:
            logger.warning("siguiente_paso2 not found: %s", e)
        _human_delay(*DELAY_AFTER_NEXT)

        _progress("Aceptando cookies (si aparece)")
        _click_cookiebot_allow_all(driver)
        _human_delay(0.5, 1.2)

        _progress("Iniciando sesión")
        login_done = _do_login(driver)
        _human_delay(*MANUAL_LOGIN_WAIT)
        if login_done:
            _progress("Comprobando sesión")
            try:
                WebDriverWait(driver, 12).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#confirmarCompra"))
                )
                _human_delay(0.5, 1.0)
            except Exception:
                logger.warning("BUY button (#confirmarCompra) not found after login")

        _progress("Pulsando BUY")
        _human_delay(*DELAY_BEFORE_BUY)
        try:
            buy_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#confirmarCompra"))
            )
            driver.execute_script("arguments[0].click();", buy_btn)
            logger.info("Clicked BUY (#confirmarCompra)")
        except Exception as e:
            logger.warning("Could not click BUY: %s", e)

        bought, step_msg = _detect_purchase_success(driver)
        _progress("Finalizado. " + step_msg)

        headless = os.environ.get("LOTTERY_BOT_HEADLESS", "true").strip().lower() not in ("0", "false", "no")
        if headless:
            try:
                driver.quit()
            except Exception:
                pass
        return {"bought": bought}
    except Exception as e:
        logger.exception("Euromillones Selenium failed: %s", e)
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        raise


def _on_stop(*_args):
    global _current_queue_id
    if _current_queue_id:
        logger.info("Stopping: marking job %s as failed", _current_queue_id)
        complete_job(_current_queue_id, success=False, error="Bot stopped by user or signal")
        _current_queue_id = None
    sys.exit(0)


def main():
    global _current_queue_id
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger.info("Euromillones bot starting (API only). API_URL=%s", API_URL)

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
