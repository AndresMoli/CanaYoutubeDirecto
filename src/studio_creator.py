from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re


STUDIO_LIVESTREAM_URL = "https://studio.youtube.com/channel/UCZU9G9HPOLYK-QeaCJo6Fhg/livestreaming"


class StudioCreationError(RuntimeError):
    pass


def _log(message: str) -> None:
    print(message, flush=True)


@dataclass(frozen=True)
class StudioCreateResult:
    title: str
    scheduled_start: datetime
    source_keyword: str


class StudioBroadcastCreator(AbstractContextManager["StudioBroadcastCreator"]):
    def __init__(
        self,
        *,
        storage_state_path: str,
        headless: bool,
        timeout_ms: int,
        slow_mo_ms: int,
        log_screenshots: bool,
        log_screenshots_dir: str,
    ) -> None:
        self._storage_state_raw = storage_state_path
        self._storage_state_path = Path(storage_state_path)
        self._headless = headless
        self._timeout_ms = timeout_ms
        self._slow_mo_ms = slow_mo_ms
        self._log_screenshots = log_screenshots
        self._log_screenshots_dir = Path(log_screenshots_dir)
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._screenshot_index = 0

    def __enter__(self) -> "StudioBroadcastCreator":
        if not self._storage_state_raw.strip():
            raise StudioCreationError(
                "Falta YT_STUDIO_STORAGE_STATE_PATH. Debe apuntar a un archivo JSON "
                "generado por scripts/save_studio_storage_state.py."
            )

        if self._storage_state_path.is_dir():
            candidate = self._storage_state_path / "storage_state.json"
            if candidate.is_file():
                self._storage_state_path = candidate
                _log(
                    "STUDIO: YT_STUDIO_STORAGE_STATE_PATH es un directorio; "
                    f"usando {self._storage_state_path}."
                )
            else:
                json_files = sorted(self._storage_state_path.glob("*.json"))
                if len(json_files) == 1:
                    self._storage_state_path = json_files[0]
                    _log(
                        "STUDIO: YT_STUDIO_STORAGE_STATE_PATH es un directorio; "
                        f"usando el único JSON detectado: {self._storage_state_path}."
                    )
                elif len(json_files) > 1:
                    files_list = ", ".join(str(path.name) for path in json_files)
                    raise StudioCreationError(
                        "YT_STUDIO_STORAGE_STATE_PATH apunta a un directorio con varios JSON "
                        f"({files_list}). Debe apuntar explícitamente al archivo correcto."
                    )
                else:
                    raise StudioCreationError(
                        "YT_STUDIO_STORAGE_STATE_PATH apunta a un directorio. "
                        "Debe apuntar a un archivo JSON (por ejemplo: storage_state.json)."
                    )

        if not self._storage_state_path.exists():
            raise StudioCreationError(
                f"No existe YT_STUDIO_STORAGE_STATE_PATH: {self._storage_state_path}"
            )
        if not self._storage_state_path.is_file():
            raise StudioCreationError(
                "YT_STUDIO_STORAGE_STATE_PATH debe ser un archivo JSON válido. "
                f"Valor actual: {self._storage_state_path}"
            )
        try:
            with self._storage_state_path.open("r", encoding="utf-8") as storage_file:
                parsed_storage_state = json.load(storage_file)
        except json.JSONDecodeError as exc:
            raise StudioCreationError(
                "YT_STUDIO_STORAGE_STATE_PATH no contiene JSON válido. "
                f"Archivo: {self._storage_state_path}"
            ) from exc
        except OSError as exc:
            raise StudioCreationError(
                "No se pudo leer YT_STUDIO_STORAGE_STATE_PATH. "
                f"Archivo: {self._storage_state_path}"
            ) from exc

        if not isinstance(parsed_storage_state, dict):
            raise StudioCreationError(
                "YT_STUDIO_STORAGE_STATE_PATH debe contener un objeto JSON con el estado "
                f"de Playwright. Archivo: {self._storage_state_path}"
            )

        self._ensure_screenshot_directory()

        _log(f"STUDIO: usando storage state en {self._storage_state_path}.")
        try:
            from playwright.sync_api import Error as PlaywrightError
            from playwright.sync_api import sync_playwright
        except ModuleNotFoundError as exc:
            raise StudioCreationError(
                "Playwright no está instalado. Ejecuta `pip install -r requirements.txt`."
            ) from exc

        self._playwright = sync_playwright().start()
        _log("STUDIO: Playwright iniciado.")
        try:
            self._browser = self._playwright.chromium.launch(
                headless=self._headless,
                slow_mo=self._slow_mo_ms,
            )
        except PlaywrightError as exc:
            if "Executable doesn't exist" in str(exc):
                raise StudioCreationError(
                    "Faltan los navegadores de Playwright. Ejecuta "
                    "`python -m playwright install --with-deps chromium`."
                ) from exc
            raise
        _log(f"STUDIO: Chromium lanzado (headless={self._headless}, slow_mo_ms={self._slow_mo_ms}).")
        self._context = self._browser.new_context(
            storage_state=str(self._storage_state_path),
            locale="es-ES",
            timezone_id="Europe/Madrid",
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(self._timeout_ms)
        _log(f"STUDIO: contexto y página listos (timeout_ms={self._timeout_ms}).")
        self._capture_state("contexto-listo")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()

    @property
    def page(self):
        if not self._page:
            raise StudioCreationError("StudioBroadcastCreator no inicializado")
        return self._page

    def create_with_previous_settings(
        self,
        *,
        title: str,
        scheduled_start: datetime,
        template_keyword: str,
    ) -> StudioCreateResult:
        page = self.page
        _log(f"STUDIO: abriendo YouTube Studio en {STUDIO_LIVESTREAM_URL}")
        page.goto(STUDIO_LIVESTREAM_URL, wait_until="domcontentloaded")
        _log("STUDIO: YouTube Studio cargado (domcontentloaded).")
        self._capture_state("studio-cargado")

        _log("STUDIO STEP 1/8: click en 'Programar emisión'.")
        self._click_first([
            page.get_by_role("button", name="Programar emisión"),
            page.get_by_role("button", name="Schedule stream"),
        ])
        self._capture_state("step-1-programar-emision")

        _log("STUDIO STEP 2/8: abrir 'Configurar con ajustes anteriores'.")
        self._click_first([
            page.get_by_text("Configurar con ajustes anteriores", exact=False),
            page.get_by_text("Reuse settings", exact=False),
            page.get_by_text("ajustes anteriores", exact=False),
        ])
        self._capture_state("step-2-ajustes-anteriores")

        _log(f"STUDIO STEP 3/8: seleccionar plantilla con keyword '{template_keyword}'.")
        self._pick_latest_matching_template(template_keyword)
        self._capture_state("step-3-plantilla")

        _log("STUDIO STEP 4/8: click en 'Reutilizar configuración'.")
        self._click_first([
            page.get_by_role("button", name="Reutilizar configuración"),
            page.get_by_role("button", name="Reuse settings"),
        ])
        self._capture_state("step-4-reutilizar")

        _log(f"STUDIO STEP 5/8: rellenar título '{title}'.")
        title_box = self._first_locator([
            page.locator('textarea[aria-label*="Título"]'),
            page.locator('input[aria-label*="Título"]'),
            page.locator('textarea[aria-label*="Title"]'),
            page.locator('input[aria-label*="Title"]'),
        ])
        title_box.click()
        title_box.fill(title)
        self._capture_state("step-5-titulo")

        _log("STUDIO STEP 6/8: navegar a pestaña 'Visibilidad'.")
        self._go_to_visibility_tab()
        self._capture_state("step-6-visibilidad")

        _log(
            "STUDIO STEP 7/8: activar 'Programar' y establecer fecha/hora "
            f"{scheduled_start.strftime('%Y-%m-%d %H:%M %Z')}"
        )
        self._click_first([
            page.get_by_label("Programar", exact=False),
            page.get_by_text("Programar", exact=False),
            page.get_by_label("Schedule", exact=False),
            page.get_by_text("Schedule", exact=False),
        ])
        self._set_visibility_datetime(scheduled_start)
        self._capture_state("step-7-fecha-hora")

        _log("STUDIO STEP 8/8: confirmar con 'Hecho'.")
        self._click_first([
            page.get_by_role("button", name="Hecho"),
            page.get_by_role("button", name="Done"),
        ])
        self._capture_state("step-8-hecho")
        _log("STUDIO: emisión programada correctamente desde Studio UI.")

        return StudioCreateResult(
            title=title,
            scheduled_start=scheduled_start,
            source_keyword=template_keyword,
        )

    def _go_to_visibility_tab(self) -> None:
        page = self.page
        for _ in range(4):
            if page.get_by_role("tab", name="Visibilidad").count() > 0:
                page.get_by_role("tab", name="Visibilidad").first.click()
                return
            if page.get_by_role("tab", name="Visibility").count() > 0:
                page.get_by_role("tab", name="Visibility").first.click()
                return

            if not self._try_click([
                page.get_by_role("button", name="Siguiente"),
                page.get_by_role("button", name="Next"),
            ]):
                break

        raise StudioCreationError("No se pudo llegar a la pestaña de Visibilidad.")

    def _set_visibility_datetime(self, scheduled_start: datetime) -> None:
        page = self.page
        date_text = scheduled_start.strftime("%d/%m/%Y")
        time_text = scheduled_start.strftime("%H:%M")

        date_input = self._first_locator([
            page.locator('input[aria-label*="Fecha"]'),
            page.locator('input[aria-label*="Date"]'),
        ])
        date_input.click()
        date_input.fill(date_text)

        time_input = self._first_locator([
            page.locator('input[aria-label*="Hora"]'),
            page.locator('input[aria-label*="Time"]'),
        ])
        time_input.click()
        time_input.fill(time_text)

    def _pick_latest_matching_template(self, keyword: str) -> None:
        page = self.page
        list_item = self._first_locator([
            page.locator(f'ytcp-entity-card:has-text("{keyword}")'),
            page.get_by_text(keyword, exact=False),
        ])
        list_item.first.click()

    def _first_locator(self, locators):
        for locator in locators:
            if locator.count() > 0:
                return locator.first
        raise StudioCreationError("No se encontró el campo esperado en YouTube Studio.")

    def _try_click(self, locators) -> bool:
        timeout_error = Exception
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            timeout_error = PlaywrightTimeoutError
        except ModuleNotFoundError:
            pass

        for locator in locators:
            try:
                locator.first.click(timeout=1500)
                return True
            except timeout_error:
                continue
        return False

    def _click_first(self, locators) -> None:
        if self._try_click(locators):
            return
        raise StudioCreationError("No se encontró el botón esperado en YouTube Studio.")

    def _ensure_screenshot_directory(self) -> None:
        if not self._log_screenshots:
            return

        if not str(self._log_screenshots_dir).strip():
            self._log_screenshots = False
            _log("STUDIO: capturas desactivadas porque YT_STUDIO_LOG_SCREENSHOTS_DIR está vacío.")
            return

        self._log_screenshots_dir.mkdir(parents=True, exist_ok=True)
        _log(f"STUDIO: capturas de log activadas en {self._log_screenshots_dir}.")

    def _capture_state(self, label: str) -> None:
        if not self._log_screenshots or not self._page:
            return

        safe_label = re.sub(r"[^a-zA-Z0-9_-]+", "-", label).strip("-") or "estado"
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self._screenshot_index += 1
        filename = f"{self._screenshot_index:03d}-{timestamp}-{safe_label}.png"
        screenshot_path = self._log_screenshots_dir / filename

        try:
            self._page.screenshot(path=str(screenshot_path), full_page=True)
            _log(f"STUDIO SCREENSHOT: {screenshot_path}")
        except Exception as exc:  # noqa: BLE001
            _log(f"STUDIO WARN: no se pudo guardar captura '{screenshot_path}': {exc}")
