from playwright.sync_api import sync_playwright


# Uso:
#   python scripts/save_studio_storage_state.py storage_state.json
# Luego abre Chromium, inicia sesión manualmente en YouTube Studio y pulsa Enter.
def main() -> None:
    import sys

    output = sys.argv[1] if len(sys.argv) > 1 else "storage_state.json"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(locale="es-ES", timezone_id="Europe/Madrid")
        page = context.new_page()
        page.goto("https://studio.youtube.com")
        input("Inicia sesión en YouTube Studio y pulsa Enter para guardar sesión... ")
        context.storage_state(path=output)
        browser.close()
        print(f"Storage state guardado en: {output}")


if __name__ == "__main__":
    main()
