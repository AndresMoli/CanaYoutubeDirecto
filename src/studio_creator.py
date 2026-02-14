from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


STUDIO_LIVESTREAM_URL = "https://studio.youtube.com/channel/UCZU9G9HPOLYK-QeaCJo6Fhg/livestreaming"


class StudioCreationError(RuntimeError):
    pass


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
    ) -> None:
        self._storage_state_path = Path(storage_state_path)
        self._headless = headless
        self._timeout_ms = timeout_ms
        self._slow_mo_ms = slow_mo_ms
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    def __enter__(self) -> "StudioBroadcastCreator":
        if not self._storage_state_path.exists():
            raise StudioCreationError(
                f"No existe YT_STUDIO_STORAGE_STATE_PATH: {self._storage_state_path}"
            )
        try:
            from playwright.sync_api import sync_playwright
        except ModuleNotFoundError as exc:
            raise StudioCreationError(
                "Playwright no está instalado. Ejecuta `pip install -r requirements.txt`."
            ) from exc

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self._headless,
            slow_mo=self._slow_mo_ms,
        )
        self._context = self._browser.new_context(
            storage_state=str(self._storage_state_path),
            locale="es-ES",
            timezone_id="Europe/Madrid",
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(self._timeout_ms)
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
        page.goto(STUDIO_LIVESTREAM_URL, wait_until="domcontentloaded")

        # 1) Programar emisión.
        self._click_first([
            page.get_by_role("button", name="Programar emisión"),
            page.get_by_role("button", name="Schedule stream"),
        ])

        # 2) Popup: Configurar con ajustes anteriores.
        self._click_first([
            page.get_by_text("Configurar con ajustes anteriores", exact=False),
            page.get_by_text("Reuse settings", exact=False),
            page.get_by_text("ajustes anteriores", exact=False),
        ])

        # 3) Seleccionar la plantilla más reciente que contenga la keyword.
        self._pick_latest_matching_template(template_keyword)

        # 4) Reutilizar configuración.
        self._click_first([
            page.get_by_role("button", name="Reutilizar configuración"),
            page.get_by_role("button", name="Reuse settings"),
        ])

        # 5) Detalles: actualizar título.
        title_box = self._first_locator([
            page.locator('textarea[aria-label*="Título"]'),
            page.locator('input[aria-label*="Título"]'),
            page.locator('textarea[aria-label*="Title"]'),
            page.locator('input[aria-label*="Title"]'),
        ])
        title_box.click()
        title_box.fill(title)

        # 6) Siguiente -> Visibilidad.
        self._go_to_visibility_tab()

        # 7) Programar fecha y hora en Visibilidad.
        self._click_first([
            page.get_by_label("Programar", exact=False),
            page.get_by_text("Programar", exact=False),
            page.get_by_label("Schedule", exact=False),
            page.get_by_text("Schedule", exact=False),
        ])
        self._set_visibility_datetime(scheduled_start)

        # 8) Hecho.
        self._click_first([
            page.get_by_role("button", name="Hecho"),
            page.get_by_role("button", name="Done"),
        ])

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
