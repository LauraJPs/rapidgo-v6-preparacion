"""Reglas de negocio de la Variante 6 - Preparación de pedidos.

Funciones puras (sin Spark ni Kafka) para que se puedan probar con pytest
en GitHub Actions. El job de Spark importa estos mismos umbrales, así las
pruebas y el pipeline usan exactamente los mismos valores.
"""
from datetime import datetime

# --- Umbrales iniciales (docs/diseno_variante6.md). Se ajustan en ADR-002. ---
FACTOR_EXCESO_PREPARACION = 1.5   # A1: real > 1.5 x estimado
TOPE_PREPARACION_MIN = 30         # A1: real > 30 min, sin importar lo prometido
UMBRAL_COLA_PROMEDIO = 8          # A2: avg(cola_actual) >= 8 en la ventana
UMBRAL_COLA_MAXIMA_ALTA = 12      # A2 sube a ALTA si max(cola_actual) >= 12

ALERTA_A1 = "A1_PREPARACION_EXCEDIDA"
ALERTA_A2 = "A2_ACUMULACION_PEDIDOS"


def _a_datetime(valor):
    if isinstance(valor, datetime):
        return valor
    if isinstance(valor, str):
        return datetime.fromisoformat(valor.replace("Z", "+00:00"))
    raise ValueError(f"Timestamp inválido: {valor!r}")


def tiempo_preparacion_min(inicio, fin) -> float:
    """Minutos entre inicio y fin de la preparación (acepta ISO-8601 o datetime)."""
    if inicio is None or fin is None:
        raise ValueError("Se requieren inicio y fin de preparación")
    minutos = (_a_datetime(fin) - _a_datetime(inicio)).total_seconds() / 60
    if minutos < 0:
        raise ValueError("El fin de preparación es anterior al inicio")
    return round(minutos, 2)


def es_demorado(tiempo_real_min: float, tiempo_estimado_min: int) -> bool:
    """True si el pedido tardó más de lo prometido por el comercio (KPI K3)."""
    if tiempo_estimado_min <= 0:
        raise ValueError("El tiempo estimado debe ser mayor que cero")
    if tiempo_real_min < 0:
        raise ValueError("El tiempo real no puede ser negativo")
    return tiempo_real_min > tiempo_estimado_min


def evaluar_preparacion(tiempo_real_min: float, tiempo_estimado_min: int):
    """Regla A1. Devuelve (codigo, severidad) o None si no hay alerta."""
    if tiempo_estimado_min <= 0:
        raise ValueError("El tiempo estimado debe ser mayor que cero")
    if tiempo_real_min < 0:
        raise ValueError("El tiempo real no puede ser negativo")
    excede_factor = tiempo_real_min > FACTOR_EXCESO_PREPARACION * tiempo_estimado_min
    excede_tope = tiempo_real_min > TOPE_PREPARACION_MIN
    if excede_factor or excede_tope:
        return (ALERTA_A1, "ALTA")
    return None


def evaluar_acumulacion(cola_promedio: float, cola_maxima: int):
    """Regla A2 sobre una ventana. Devuelve (codigo, severidad) o None."""
    if cola_promedio < 0 or cola_maxima < 0:
        raise ValueError("La cola no puede ser negativa")
    if cola_maxima < cola_promedio:
        raise ValueError("La cola máxima no puede ser menor que el promedio")
    if cola_promedio >= UMBRAL_COLA_PROMEDIO:
        severidad = "ALTA" if cola_maxima >= UMBRAL_COLA_MAXIMA_ALTA else "MEDIA"
        return (ALERTA_A2, severidad)
    return None
