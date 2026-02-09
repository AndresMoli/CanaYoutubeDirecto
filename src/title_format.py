from __future__ import annotations

from datetime import date

WEEKDAYS_ES = [
    "Lunes",
    "Martes",
    "Miércoles",
    "Jueves",
    "Viernes",
    "Sábado",
    "Domingo",
]

MONTHS_ES = [
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre",
]


def format_spanish_date(target_date: date) -> str:
    month_es = MONTHS_ES[target_date.month - 1]
    return f"{target_date.day} de {month_es}"


def build_title(prefix: str, target_date: date) -> str:
    return f"{prefix} - {format_spanish_date(target_date)}"
