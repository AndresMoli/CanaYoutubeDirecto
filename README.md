# CanaYoutubeDirecto

Automatiza la creación masiva de emisiones programadas en YouTube Live. Puede crear por API de YouTube Data v3 o por interfaz de YouTube Studio (Playwright) reutilizando ajustes anteriores.

## Requisitos

- Python 3.11+
- Proyecto en Google Cloud con **YouTube Data API v3** habilitada.
- Credenciales OAuth2 (Client ID + Client Secret).

## Configuración en Google Cloud

1. Crea un proyecto en Google Cloud.
2. Habilita **YouTube Data API v3**.
3. Crea credenciales OAuth tipo **Desktop** (o **Web**, pero Desktop es más simple para generar el refresh token).
4. Guarda el `client_id` y `client_secret`.

## Generar el refresh token (local, una sola vez)

Ejecuta localmente:

```bash
export YT_CLIENT_ID="..."
export YT_CLIENT_SECRET="..."
python scripts/generate_refresh_token.py
```

Copia el refresh token resultante.

## Variables de entorno

### Obligatorios (Secrets)

Para soportar dos cuentas:

- ANDRES:
  - `YT_CLIENT_ID`
  - `YT_CLIENT_SECRET`
  - `YT_REFRESH_TOKEN`
- CANA:
  - `CANA_YT_CLIENT_ID`
  - `CANA_YT_CLIENT_SECRET`
  - `CANA_YT_REFRESH_TOKEN`

### Configurables (con defaults)

- `YT_TIMEZONE` (default: `Europe/Madrid`)
- `YT_DEFAULT_PRIVACY_STATUS` (default: `unlisted`)
- `YT_KEYWORD_MISA_10` (default: `Misa 10h`)
- `YT_KEYWORD_MISA_12` (default: `Misa 12h`)
- `YT_KEYWORD_MISA_20` (default: `Misa 20h`)
- `YT_KEYWORD_VELA_21` (default: `Vela 21h`)
- `YT_START_OFFSET_DAYS` (default: `1` → mañana)
- `YT_MAX_DAYS_AHEAD` (default: `3650` → días hacia adelante desde hoy)
- `YT_STOP_ON_CREATE_LIMIT` (default: `true`)
- `YT_CREATION_MODE` (default: `studio_ui`; valores: `studio_ui` o `api`)
- `YT_STUDIO_STORAGE_STATE_PATH` (default: `storage_state.json` si existe en el directorio actual; si no, vacío y obligatorio cuando `YT_CREATION_MODE=studio_ui`)
  - Debe apuntar a un archivo JSON de Playwright. Si se pasa un directorio, el sistema intentará usar `storage_state.json` o el único `*.json` disponible dentro.
- `YT_STUDIO_HEADLESS` (default: `true`)
- `YT_STUDIO_TIMEOUT_MS` (default: `30000`)
- `YT_STUDIO_SLOW_MO_MS` (default: `0`)

## GitHub Actions

El workflow está en `.github/workflows/schedule.yml` y se ejecuta:

- Manualmente (`workflow_dispatch`) eligiendo `target_account` (`ANDRES` o `CANA`).
- Automáticamente al modificar `ejecución_ahora.txt`
- Cron: **Lunes, Miércoles y Viernes** (UTC).

En GitHub Actions el workflow fuerza `YT_CREATION_MODE=api` por defecto (o el valor de la variable de repositorio `YT_CREATION_MODE` si la defines). Así no depende de `storage_state.json`, que no persiste entre ejecuciones de runners efímeros.

Para ejecuciones automáticas (`push` y `schedule`), puedes fijar la cuenta por defecto con la variable de repositorio `YT_TARGET_ACCOUNT` (valor `CANA` o `ANDRES`). Si no está definida, usa `CANA`.

> Nota: el cron es UTC. La lógica del script usa `Europe/Madrid` para calcular "mañana" y las horas 10/12/20/21.

## Ejecución local

```bash
pip install -r requirements.txt
python -m playwright install --with-deps chromium
python -m src.main
```

## Lógica principal

- Para cada día futuro (desde mañana, hasta `hoy + YT_MAX_DAYS_AHEAD`), crea emisiones:
  - `Misa 10h - {weekday_es} {dd} de {mes_es}` a las 10:00 Europe/Madrid
  - `Misa 12h - {weekday_es} {dd} de {mes_es}` a las 12:00 Europe/Madrid
  - `Misa 20h - {weekday_es} {dd} de {mes_es}` a las 20:00 Europe/Madrid
  - Si es jueves: `Vela 21h - {weekday_es} {dd} de {mes_es}` a las 21:00 Europe/Madrid
- Idempotencia: si el título ya existe, se omite.
- Si ya hay emisiones futuras, el proceso empieza después de la última existente.
- Plantillas: usa la última emisión cuyo título contenga la keyword para copiar ajustes y stream asociado.
- Cuando la API indique límite/cuota, el script termina con éxito y log: `STOP: límite alcanzado`.


## Modo YouTube Studio (Playwright)

La creación por defecto ahora es desde la interfaz de YouTube Studio (Playwright). Si quieres forzarlo explícitamente, usa:

```bash
export YT_CREATION_MODE="studio_ui"
export YT_STUDIO_STORAGE_STATE_PATH="storage_state.json"
python -m src.main
```

Para generar el `storage_state.json` una vez (iniciando sesión manual):

```bash
python -m playwright install --with-deps chromium
python scripts/save_studio_storage_state.py storage_state.json
```

En este modo, la creación entra exactamente en `https://studio.youtube.com/channel/UCZU9G9HPOLYK-QeaCJo6Fhg/livestreaming`, pulsa **Programar emisión**, luego **Configurar con ajustes anteriores**, selecciona la plantilla más reciente por keyword (`Misa 10h`, `Misa 12h`, `Misa 20h`, `Vela 21h`), pulsa **Reutilizar configuración**, cambia el título, avanza con **Siguiente** hasta **Visibilidad**, programa fecha/hora y finaliza con **Hecho**.


Para volver al modo API clásico:

```bash
export YT_CREATION_MODE="api"
python -m src.main
```
