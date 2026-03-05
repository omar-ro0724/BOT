"""
SUPABASE MANAGER - Gestión de base de datos
Maneja: logs de conversaciones, caché de datos MicroStrategy,
entrenamiento del bot e intenciones personalizadas
"""

import os
import json
import time
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("⚠️  supabase-py no instalado. Logs desactivados.")


class SupabaseManager:
    """Gestor de Supabase para el bot de MicroStrategy"""

    SQL_SETUP = """
-- ============================================================
-- EJECUTA ESTE SQL EN SUPABASE > SQL Editor > New Query
-- ============================================================

-- 1. Tabla de conversaciones (logs de chat)
CREATE TABLE IF NOT EXISTS conversaciones (
    id          BIGSERIAL PRIMARY KEY,
    session_id  TEXT,
    pregunta    TEXT NOT NULL,
    respuesta   TEXT NOT NULL,
    fuente_usada TEXT,
    tipo_fuente  TEXT,
    tipo_analisis TEXT,
    registros_analizados INT DEFAULT 0,
    tiempo_respuesta_ms  INT DEFAULT 0,
    es_error    BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Caché de datos MicroStrategy (evita re-ejecutar cubos)
CREATE TABLE IF NOT EXISTS cache_mstr (
    source_id   TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_name TEXT,
    data_json   JSONB,
    columns_json JSONB,
    row_count   INT DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    expires_at  TIMESTAMPTZ
);

-- 3. Intenciones para entrenamiento del bot
CREATE TABLE IF NOT EXISTS intenciones (
    id                BIGSERIAL PRIMARY KEY,
    texto_ejemplo     TEXT NOT NULL,
    intencion         TEXT NOT NULL,
    respuesta_hint    TEXT,
    activo            BOOLEAN DEFAULT TRUE,
    usos              INT DEFAULT 0,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Fuentes de datos disponibles (catálogo)
CREATE TABLE IF NOT EXISTS fuentes_datos (
    source_id   TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    descripcion TEXT,
    metricas    JSONB,
    dimensiones JSONB,
    activo      BOOLEAN DEFAULT TRUE,
    ultimo_acceso TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Índices para performance
CREATE INDEX IF NOT EXISTS idx_conversaciones_session ON conversaciones(session_id);
CREATE INDEX IF NOT EXISTS idx_conversaciones_created ON conversaciones(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_intenciones_activo ON intenciones(activo, intencion);

-- 6. Intenciones de entrenamiento base (ejemplos)
INSERT INTO intenciones (texto_ejemplo, intencion, respuesta_hint) VALUES
    ('muéstrame las ventas totales', 'total', 'Suma de todas las ventas disponibles'),
    ('cuánto vendimos en total', 'total', 'Total de ventas'),
    ('suma de ingresos', 'total', 'Suma de ingresos'),
    ('promedio de ventas por región', 'promedio', 'Promedio agrupado por región'),
    ('media de ingresos', 'promedio', 'Media de ingresos'),
    ('top 10 productos más vendidos', 'ranking_top', 'Top 10 por ventas'),
    ('mejores vendedores', 'ranking_top', 'Ranking de mejores vendedores'),
    ('peores regiones', 'ranking_bottom', 'Ranking ascendente por ventas'),
    ('ventas por región', 'desglose', 'Desglose de ventas por región'),
    ('desglose por categoría', 'desglose', 'Desglose por categoría'),
    ('predice las ventas del próximo mes', 'prediccion', 'Predicción ML usando promedio móvil'),
    ('forecast de ventas', 'prediccion', 'Pronóstico de ventas'),
    ('cuál será la tendencia', 'prediccion', 'Análisis de tendencia futura'),
    ('analiza las correlaciones entre variables', 'correlacion', 'Matriz de correlación'),
    ('relación entre precio y ventas', 'correlacion', 'Correlación precio-ventas'),
    ('genera un dashboard completo', 'dashboard', 'Dashboard ejecutivo con KPIs'),
    ('muéstrame los KPIs principales', 'dashboard', 'KPIs del período'),
    ('resumen ejecutivo', 'dashboard', 'Resumen ejecutivo completo'),
    ('comparar norte vs sur', 'comparacion', 'Comparación entre dimensiones'),
    ('diferencia entre enero y febrero', 'comparacion', 'Comparación temporal')
ON CONFLICT DO NOTHING;

-- ============================================================
-- FIN DEL SETUP
-- ============================================================
"""

    def __init__(self):
        self.client: Optional[Any] = None
        self.enabled = False
        self._connect()

    def _connect(self):
        """Conectar a Supabase"""
        if not SUPABASE_AVAILABLE:
            return

        url = os.environ.get('SUPABASE_URL', '')
        key = os.environ.get('SUPABASE_KEY', '')

        if not url or not key:
            print("⚠️  SUPABASE_URL y SUPABASE_KEY no configurados. Base de datos desactivada.")
            return

        try:
            self.client = create_client(url, key)
            self.enabled = True
            print("✅ Supabase conectado correctamente")
        except Exception as e:
            print(f"❌ Error conectando a Supabase: {e}")

    # ----------------------------------------------------------------
    # LOGS DE CONVERSACIONES
    # ----------------------------------------------------------------

    def log_conversacion(
        self,
        session_id: str,
        pregunta: str,
        respuesta: str,
        fuente_usada: str = None,
        tipo_fuente: str = None,
        tipo_analisis: str = None,
        registros: int = 0,
        tiempo_ms: int = 0,
        es_error: bool = False
    ) -> bool:
        """Guardar una conversación en Supabase"""
        if not self.enabled:
            return False

        try:
            self.client.table('conversaciones').insert({
                'session_id': session_id,
                'pregunta': pregunta[:2000],  # límite de texto
                'respuesta': respuesta[:5000],
                'fuente_usada': fuente_usada,
                'tipo_fuente': tipo_fuente,
                'tipo_analisis': tipo_analisis,
                'registros_analizados': registros,
                'tiempo_respuesta_ms': tiempo_ms,
                'es_error': es_error
            }).execute()
            return True
        except Exception as e:
            print(f"⚠️  Error guardando log: {e}")
            return False

    def obtener_historial(self, session_id: str, limite: int = 20) -> List[Dict]:
        """Obtener historial de conversación de una sesión"""
        if not self.enabled:
            return []

        try:
            result = (
                self.client.table('conversaciones')
                .select('pregunta, respuesta, tipo_analisis, created_at')
                .eq('session_id', session_id)
                .order('created_at', desc=True)
                .limit(limite)
                .execute()
            )
            return result.data or []
        except Exception as e:
            print(f"⚠️  Error obteniendo historial: {e}")
            return []

    def obtener_estadisticas(self) -> Dict:
        """Estadísticas generales de uso del bot"""
        if not self.enabled:
            return {}

        try:
            total = self.client.table('conversaciones').select('id', count='exact').execute()
            errores = self.client.table('conversaciones').select('id', count='exact').eq('es_error', True).execute()
            hoy = self.client.table('conversaciones').select('id', count='exact').gte(
                'created_at', datetime.now(timezone.utc).strftime('%Y-%m-%dT00:00:00Z')
            ).execute()

            return {
                'total_conversaciones': total.count or 0,
                'errores': errores.count or 0,
                'consultas_hoy': hoy.count or 0
            }
        except Exception as e:
            print(f"⚠️  Error obteniendo estadísticas: {e}")
            return {}

    # ----------------------------------------------------------------
    # CACHÉ DE DATOS MICROSTRATEGY
    # ----------------------------------------------------------------

    def guardar_cache(
        self,
        source_id: str,
        source_type: str,
        source_name: str,
        df_data: List[Dict],
        columns: List[str],
        ttl_minutos: int = 30
    ) -> bool:
        """Guardar datos de MicroStrategy en caché"""
        if not self.enabled:
            return False

        try:
            from datetime import timedelta
            expires = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutos)

            self.client.table('cache_mstr').upsert({
                'source_id': source_id,
                'source_type': source_type,
                'source_name': source_name,
                'data_json': df_data[:500],  # máximo 500 filas en caché
                'columns_json': columns,
                'row_count': len(df_data),
                'expires_at': expires.isoformat()
            }).execute()
            return True
        except Exception as e:
            print(f"⚠️  Error guardando caché: {e}")
            return False

    def obtener_cache(self, source_id: str) -> Optional[Dict]:
        """Obtener datos cacheados si no han expirado"""
        if not self.enabled:
            return None

        try:
            now = datetime.now(timezone.utc).isoformat()
            result = (
                self.client.table('cache_mstr')
                .select('*')
                .eq('source_id', source_id)
                .gt('expires_at', now)
                .limit(1)
                .execute()
            )

            if result.data:
                return result.data[0]
            return None
        except Exception as e:
            print(f"⚠️  Error obteniendo caché: {e}")
            return None

    # ----------------------------------------------------------------
    # ENTRENAMIENTO DEL BOT
    # ----------------------------------------------------------------

    def obtener_intenciones(self) -> List[Dict]:
        """Obtener todas las intenciones de entrenamiento activas"""
        if not self.enabled:
            return []

        try:
            result = (
                self.client.table('intenciones')
                .select('texto_ejemplo, intencion, respuesta_hint')
                .eq('activo', True)
                .execute()
            )
            return result.data or []
        except Exception as e:
            print(f"⚠️  Error obteniendo intenciones: {e}")
            return []

    def agregar_intencion(
        self,
        texto_ejemplo: str,
        intencion: str,
        respuesta_hint: str = None
    ) -> bool:
        """Agregar nueva intención de entrenamiento"""
        if not self.enabled:
            return False

        try:
            self.client.table('intenciones').insert({
                'texto_ejemplo': texto_ejemplo,
                'intencion': intencion,
                'respuesta_hint': respuesta_hint
            }).execute()
            return True
        except Exception as e:
            print(f"⚠️  Error agregando intención: {e}")
            return False

    def registrar_uso_intencion(self, intencion: str):
        """Incrementar contador de uso de una intención"""
        if not self.enabled:
            return

        try:
            # Obtener las intenciones con ese nombre y actualizar usos
            result = self.client.table('intenciones').select('id, usos').eq('intencion', intencion).execute()
            if result.data:
                for row in result.data:
                    self.client.table('intenciones').update({
                        'usos': (row.get('usos') or 0) + 1
                    }).eq('id', row['id']).execute()
        except Exception as e:
            pass  # No crítico

    # ----------------------------------------------------------------
    # CATÁLOGO DE FUENTES
    # ----------------------------------------------------------------

    def registrar_fuente(
        self,
        source_id: str,
        source_type: str,
        source_name: str,
        metricas: List[str] = None,
        dimensiones: List[str] = None
    ) -> bool:
        """Registrar una fuente de datos descubierta"""
        if not self.enabled:
            return False

        try:
            self.client.table('fuentes_datos').upsert({
                'source_id': source_id,
                'source_type': source_type,
                'source_name': source_name,
                'metricas': metricas or [],
                'dimensiones': dimensiones or [],
                'ultimo_acceso': datetime.now(timezone.utc).isoformat()
            }).execute()
            return True
        except Exception as e:
            print(f"⚠️  Error registrando fuente: {e}")
            return False

    def obtener_fuentes_registradas(self) -> List[Dict]:
        """Obtener catálogo de fuentes conocidas"""
        if not self.enabled:
            return []

        try:
            result = (
                self.client.table('fuentes_datos')
                .select('*')
                .eq('activo', True)
                .order('ultimo_acceso', desc=True)
                .execute()
            )
            return result.data or []
        except Exception as e:
            return []

    def get_sql_setup(self) -> str:
        """Retorna el SQL para configurar Supabase"""
        return self.SQL_SETUP
