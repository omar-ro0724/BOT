"""
BOT MEJORADO v2.0 - MicroStrategy Intelligence Bot
─────────────────────────────────────────────────
✅ Integración Supabase (logs, caché, entrenamiento)
✅ NLP mejorado con intenciones dinámicas desde BD
✅ Caché inteligente de datos MicroStrategy
✅ Contexto de conversación multi-turno
✅ Análisis estadístico avanzado
✅ Resumen automático de gráficos
"""

import os
import re
import time
import json
import requests
import pandas as pd
import numpy as np
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

warnings.filterwarnings('ignore')

from supabase_manager import SupabaseManager


class NLPEngine:
    """Motor de Procesamiento de Lenguaje Natural con entrenamiento desde Supabase"""

    # Patrones base (siempre activos)
    PATRONES_BASE = {
        'total':        r'(total|suma|cuánto|cuanto|cantidad total|sumatoria)',
        'promedio':     r'(promedio|media|average|promedio de)',
        'ranking_top':  r'(top|mejor(?:es)?|mayor(?:es)?|m[aá]s alto|principales|primeros?)\s*(\d+)?',
        'ranking_bottom': r'(peor(?:es)?|menor(?:es)?|m[aá]s bajo|últimos?|bottom)\s*(\d+)?',
        'desglose':     r'(por\s+\w+|desglose|dividido por|segmentado|cada|grupo)',
        'comparacion':  r'(comparar|versus|vs\.?|vs\s|diferencia entre|comparativa)',
        'tendencia':    r'(tendencia|evolución|histórico|a lo largo|en el tiempo|mes a mes)',
        'prediccion':   r'(predic|predeci|futuro|próximo|siguiente|forecast|proyec|qué pasará)',
        'correlacion':  r'(correlaci[oó]n|relaci[oó]n entre|asociaci[oó]n|correlaciona)',
        'dashboard':    r'(dashboard|tablero|resumen|kpis?|ejecutivo|panorama general)',
        'exportar':     r'(exportar|descargar|guardar|descarga|excel|xlsx)',
        'listar':       r'(lista|muéstrame|dame|cuáles|cuales son|mostrar)',
        'filtrar':      r'(donde|cuando|que tengan|con más de|mayor a|menor a|filtrar)',
        'maximo':       r'(máximo|máxima|mayor|el más alto|pico|peak)',
        'minimo':       r'(mínimo|mínima|menor|el más bajo|el peor)',
    }

    # Palabras clave de dominio (negocio)
    KEYWORDS_NEGOCIO = {
        'ventas':       ['venta', 'ventas', 'ingresos', 'revenue', 'facturación'],
        'clientes':     ['cliente', 'clientes', 'customer', 'compradores'],
        'productos':    ['producto', 'productos', 'item', 'sku', 'artículo'],
        'regiones':     ['región', 'regiones', 'zona', 'tienda', 'local', 'ciudad'],
        'tiempo':       ['mes', 'año', 'semana', 'trimestre', 'período', 'fecha'],
        'cantidad':     ['cantidad', 'unidades', 'volumen', 'piezas'],
        'precio':       ['precio', 'valor', 'costo', 'importe'],
    }

    def __init__(self, supabase: SupabaseManager):
        self.supabase = supabase
        self.intenciones_bd = []
        self._cargar_intenciones_bd()

    def _cargar_intenciones_bd(self):
        """Cargar intenciones desde Supabase para enriquecer el NLP"""
        self.intenciones_bd = self.supabase.obtener_intenciones()
        if self.intenciones_bd:
            print(f"🧠 {len(self.intenciones_bd)} intenciones cargadas desde BD")

    def detectar_intencion(self, texto: str) -> Dict:
        """Detectar intención principal y secundarias en el texto"""
        texto_lower = texto.lower().strip()

        intenciones_detectadas = {}
        numero_detectado = None

        # 1. Detectar número si existe (para top N)
        num_match = re.search(r'\b(\d+)\b', texto_lower)
        if num_match:
            numero_detectado = int(num_match.group(1))

        # 2. Patrones base (regex)
        for intencion, patron in self.PATRONES_BASE.items():
            match = re.search(patron, texto_lower)
            if match:
                intenciones_detectadas[intencion] = True

        # 3. Intenciones desde Supabase (coincidencia de palabras)
        for item in self.intenciones_bd:
            ejemplo = item.get('texto_ejemplo', '').lower()
            intencion = item.get('intencion', '')
            # Tokenizar y buscar al menos 2 tokens en común
            tokens_ejemplo = set(re.findall(r'\b\w{3,}\b', ejemplo))
            tokens_texto = set(re.findall(r'\b\w{3,}\b', texto_lower))
            comunes = tokens_ejemplo & tokens_texto
            if len(comunes) >= 2:
                intenciones_detectadas[intencion] = True

        # 4. Detectar dominio (qué datos se mencionan)
        dominio = []
        for categoria, palabras in self.KEYWORDS_NEGOCIO.items():
            for p in palabras:
                if p in texto_lower:
                    dominio.append(categoria)
                    break

        # 5. Determinar intención principal
        prioridad = [
            'dashboard', 'prediccion', 'correlacion', 'exportar',
            'comparacion', 'ranking_top', 'ranking_bottom', 'desglose',
            'filtrar', 'tendencia', 'promedio', 'total', 'maximo', 'minimo',
            'listar', 'general'
        ]

        intencion_principal = 'general'
        for p in prioridad:
            if p in intenciones_detectadas:
                intencion_principal = p
                break

        return {
            'principal': intencion_principal,
            'todas': list(intenciones_detectadas.keys()),
            'numero': numero_detectado,
            'dominio': dominio,
            'texto_original': texto
        }

    def detectar_columna_objetivo(self, texto: str, columnas: List[str]) -> Optional[str]:
        """Detectar qué columna quiere analizar el usuario"""
        texto_lower = texto.lower()
        for col in columnas:
            if col.lower() in texto_lower:
                return col
        # Buscar por sinónimos del dominio
        for categoria, palabras in self.KEYWORDS_NEGOCIO.items():
            for p in palabras:
                if p in texto_lower:
                    # Buscar columna que contenga ese término
                    for col in columnas:
                        if p in col.lower():
                            return col
        return None


class AnalizadorDatos:
    """Análisis estadístico avanzado + interpretación automática de gráficos"""

    @staticmethod
    def analizar_dataframe(df: pd.DataFrame) -> Dict:
        """Análisis estadístico completo de un DataFrame"""
        if df is None or df.empty:
            return {}

        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        cat_cols = df.select_dtypes(include=['object']).columns.tolist()

        resumen = {
            'filas': len(df),
            'columnas': len(df.columns),
            'metricas': {},
            'dimensiones': {},
            'correlaciones': {},
            'interpretacion': []
        }

        # Análisis de métricas numéricas
        for col in numeric_cols:
            serie = df[col].dropna()
            if len(serie) == 0:
                continue

            stats = {
                'total': float(serie.sum()),
                'promedio': float(serie.mean()),
                'mediana': float(serie.median()),
                'min': float(serie.min()),
                'max': float(serie.max()),
                'std': float(serie.std()),
                'coef_variacion': float(serie.std() / serie.mean() * 100) if serie.mean() != 0 else 0
            }
            resumen['metricas'][col] = stats

        # Análisis de dimensiones categóricas
        for col in cat_cols:
            resumen['dimensiones'][col] = {
                'valores_unicos': int(df[col].nunique()),
                'top_5': df[col].value_counts().head(5).to_dict()
            }

        # Correlaciones entre métricas
        if len(numeric_cols) >= 2:
            corr_matrix = df[numeric_cols].corr()
            correlaciones_sig = []
            for i in range(len(numeric_cols)):
                for j in range(i + 1, len(numeric_cols)):
                    val = corr_matrix.iloc[i, j]
                    if abs(val) > 0.3:
                        correlaciones_sig.append({
                            'var1': numeric_cols[i],
                            'var2': numeric_cols[j],
                            'valor': round(float(val), 3),
                            'fuerza': 'fuerte' if abs(val) > 0.7 else 'moderada',
                            'tipo': 'positiva' if val > 0 else 'negativa'
                        })
            resumen['correlaciones'] = sorted(correlaciones_sig, key=lambda x: abs(x['valor']), reverse=True)

        # Interpretación automática
        resumen['interpretacion'] = AnalizadorDatos._generar_interpretacion(df, resumen)

        return resumen

    @staticmethod
    def _generar_interpretacion(df: pd.DataFrame, stats: Dict) -> List[str]:
        """Generar interpretación automática en lenguaje natural"""
        interpretaciones = []
        metricas = stats.get('metricas', {})
        dimensiones = stats.get('dimensiones', {})
        correlaciones = stats.get('correlaciones', [])

        for col, s in metricas.items():
            cv = s.get('coef_variacion', 0)
            if cv > 50:
                interpretaciones.append(
                    f"📊 **{col}** tiene alta variabilidad (CV={cv:.1f}%) — los datos son muy dispersos."
                )
            elif cv < 10:
                interpretaciones.append(
                    f"📊 **{col}** es muy uniforme (CV={cv:.1f}%) — los valores son consistentes."
                )

        if correlaciones:
            top_corr = correlaciones[0]
            interpretaciones.append(
                f"🔗 **{top_corr['var1']}** y **{top_corr['var2']}** tienen una correlación "
                f"{top_corr['fuerza']} {top_corr['tipo']} ({top_corr['valor']:.2f})."
            )

        for col, info in dimensiones.items():
            n = info.get('valores_unicos', 0)
            if n == 1:
                interpretaciones.append(f"⚠️  **{col}** solo tiene 1 valor único — considera si es útil como dimensión.")
            elif n > 50:
                interpretaciones.append(f"ℹ️  **{col}** tiene {n} valores distintos — alta cardinalidad.")

        return interpretaciones[:5]  # máximo 5 interpretaciones

    @staticmethod
    def prediccion_ml(df: pd.DataFrame, columna: str = None) -> Dict:
        """Predicción ML con promedio móvil ponderado y tendencia lineal"""
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()

        if not numeric_cols:
            return {'error': 'No hay datos numéricos para predicciones'}

        col = columna if (columna and columna in numeric_cols) else numeric_cols[0]
        valores = df[col].dropna().values

        if len(valores) < 3:
            return {'error': 'Se necesitan mínimo 3 registros para predecir'}

        # Promedio móvil ponderado (más peso a valores recientes)
        n = min(5, len(valores))
        pesos = np.arange(1, n + 1, dtype=float)
        pesos = pesos / pesos.sum()
        pred_promedio_movil = float(np.average(valores[-n:], weights=pesos))

        # Regresión lineal simple
        x = np.arange(len(valores))
        coef = np.polyfit(x, valores, 1)
        pred_lineal = float(coef[0] * len(valores) + coef[1])

        # Promedio de ambas predicciones
        prediccion_final = (pred_promedio_movil + pred_lineal) / 2

        # Tendencia
        pendiente = float(coef[0])
        if pendiente > 0.01 * abs(np.mean(valores)):
            tendencia = 'creciente ↗'
        elif pendiente < -0.01 * abs(np.mean(valores)):
            tendencia = 'decreciente ↘'
        else:
            tendencia = 'estable →'

        # Intervalo de confianza simple (±1 std)
        std_reciente = float(np.std(valores[-n:]))
        ci_lower = prediccion_final - std_reciente
        ci_upper = prediccion_final + std_reciente

        return {
            'columna': col,
            'prediccion': prediccion_final,
            'ci_lower': ci_lower,
            'ci_upper': ci_upper,
            'tendencia': tendencia,
            'pendiente_diaria': pendiente,
            'ultimos_valores': valores[-5:].tolist(),
            'promedio_historico': float(np.mean(valores)),
            'metodo': 'Promedio Móvil Ponderado + Regresión Lineal'
        }


class BotMejorado:
    """Bot principal - MicroStrategy Intelligence v2.0"""

    def __init__(self, base_url: str, username: str, password: str, project_id: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.project_id = project_id

        self.session = requests.Session()
        self.auth_token = None
        self.is_authenticated = False

        self.available_sources = []
        self.last_query_result = None
        self._auth_timestamp = None

        # Inicializar módulos
        self.db = SupabaseManager()
        self.nlp = NLPEngine(self.db)
        self.analizador = AnalizadorDatos()

        print("🤖 Bot MicroStrategy v2.0 inicializado")
        if self.db.enabled:
            print("✅ Base de datos Supabase activa")

    # ----------------------------------------------------------------
    # AUTENTICACIÓN
    # ----------------------------------------------------------------

    def authenticate(self) -> bool:
        """Autenticar o re-autenticar con MicroStrategy"""
        # Re-autenticar si el token tiene más de 55 minutos
        if self.is_authenticated and self._auth_timestamp:
            elapsed = (datetime.now() - self._auth_timestamp).seconds / 60
            if elapsed < 55:
                return True

        try:
            print(f"🔐 Autenticando: {self.username}")
            response = self.session.post(
                f"{self.base_url}/auth/login",
                json={"username": self.username, "password": self.password, "loginMode": 1},
                headers={"Content-Type": "application/json"},
                timeout=20
            )

            if response.status_code == 204:
                self.auth_token = response.headers.get('X-MSTR-AuthToken')
                self.session.headers.update({
                    'X-MSTR-AuthToken': self.auth_token,
                    'Content-Type': 'application/json',
                    'X-MSTR-ProjectID': self.project_id
                })
                self.is_authenticated = True
                self._auth_timestamp = datetime.now()
                print("✅ Autenticación exitosa")
                self._descubrir_fuentes()
                return True
            else:
                print(f"❌ Auth falló: HTTP {response.status_code}")
                return False

        except Exception as e:
            print(f"❌ Error de autenticación: {e}")
            return False

    def _descubrir_fuentes(self):
        """Descubrir todas las fuentes de datos disponibles"""
        print("\n🔍 Descubriendo fuentes de datos...")
        self.available_sources = []

        endpoints = [
            ('cubo',    f"{self.base_url}/cubes",    {'limit': 200}),
            ('reporte', f"{self.base_url}/reports",  {'limit': 200}),
            ('dossier', f"{self.base_url}/dossiers", {'limit': 50}),
        ]

        for tipo, url, params in endpoints:
            try:
                r = self.session.get(url, params=params, timeout=15)
                if r.status_code == 200:
                    items = r.json()
                    if isinstance(items, list):
                        for item in items:
                            source = {
                                'tipo': tipo,
                                'id': item.get('id'),
                                'nombre': item.get('name', 'Sin nombre'),
                                'objeto': item
                            }
                            self.available_sources.append(source)
                            # Registrar en Supabase
                            self.db.registrar_fuente(
                                source['id'], tipo, source['nombre']
                            )
                    print(f"  ✅ {tipo}s: {len([s for s in self.available_sources if s['tipo']==tipo])} encontrados")
            except Exception as e:
                print(f"  ⚠️  {tipo}s: {e}")

        # Búsqueda genérica si no hay nada
        if not self.available_sources:
            try:
                r = self.session.get(
                    f"{self.base_url}/searches/results",
                    params={'type': 3, 'limit': 100},
                    timeout=15
                )
                if r.status_code == 200:
                    data = r.json()
                    for obj in data.get('result', []):
                        self.available_sources.append({
                            'tipo': 'objeto',
                            'id': obj.get('id'),
                            'nombre': obj.get('name', 'Sin nombre'),
                            'objeto': obj
                        })
            except Exception:
                pass

        print(f"\n📚 Total fuentes disponibles: {len(self.available_sources)}")

    # ----------------------------------------------------------------
    # OBTENCIÓN DE DATOS
    # ----------------------------------------------------------------

    def _obtener_datos_fuente(self, source_id: str, source_type: str, source_name: str) -> Optional[pd.DataFrame]:
        """Obtener datos con caché inteligente de Supabase"""

        # 1. Verificar caché en Supabase
        cache = self.db.obtener_cache(source_id)
        if cache:
            print(f"  ⚡ Datos desde caché ({cache.get('row_count', 0)} filas)")
            try:
                data = cache.get('data_json', [])
                cols = cache.get('columns_json', [])
                if data and cols:
                    return pd.DataFrame(data, columns=cols)
            except Exception:
                pass

        # 2. Obtener de MicroStrategy
        df = None
        if source_type == 'cubo':
            df = self._ejecutar_cubo(source_id)
        elif source_type == 'reporte':
            df = self._ejecutar_reporte(source_id)
        else:
            df = self._ejecutar_reporte(source_id) or self._ejecutar_cubo(source_id)

        # 3. Guardar en caché si hay datos
        if df is not None and not df.empty:
            self.db.guardar_cache(
                source_id, source_type, source_name,
                df.to_dict('records'), list(df.columns),
                ttl_minutos=int(os.environ.get('CACHE_TTL_MIN', 30))
            )
            # Actualizar catálogo con métricas/dimensiones
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            cat_cols = df.select_dtypes(include=['object']).columns.tolist()
            self.db.registrar_fuente(
                source_id, source_type, source_name,
                metricas=numeric_cols, dimensiones=cat_cols
            )

        return df

    def _ejecutar_cubo(self, cubo_id: str) -> Optional[pd.DataFrame]:
        """Ejecutar cubo MicroStrategy y obtener DataFrame"""
        try:
            r = self.session.post(
                f"{self.base_url}/cubes/{cubo_id}/instances",
                json={"limit": 10000},
                timeout=45
            )
            if r.status_code not in [200, 201]:
                return None

            instance_id = r.json().get('instanceId')
            r2 = self.session.get(
                f"{self.base_url}/cubes/{cubo_id}/instances/{instance_id}",
                timeout=45
            )
            if r2.status_code == 200:
                return self._json_a_dataframe(r2.json())
        except Exception as e:
            print(f"  ❌ Error en cubo {cubo_id}: {e}")
        return None

    def _ejecutar_reporte(self, reporte_id: str) -> Optional[pd.DataFrame]:
        """Ejecutar reporte MicroStrategy y obtener DataFrame"""
        try:
            r = self.session.post(
                f"{self.base_url}/reports/{reporte_id}/instances",
                json={"limit": 10000},
                timeout=45
            )
            if r.status_code not in [200, 201]:
                return None

            instance_id = r.json().get('instanceId')
            r2 = self.session.get(
                f"{self.base_url}/reports/{reporte_id}/instances/{instance_id}",
                timeout=45
            )
            if r2.status_code == 200:
                return self._json_a_dataframe(r2.json())
        except Exception as e:
            print(f"  ❌ Error en reporte {reporte_id}: {e}")
        return None

    def _json_a_dataframe(self, data: Dict) -> Optional[pd.DataFrame]:
        """Convertir respuesta JSON de MicroStrategy a DataFrame"""
        try:
            definition = data.get('definition', {})
            attributes = definition.get('attributes', [])
            metrics = definition.get('metrics', [])

            columns = [a.get('name') for a in attributes] + [m.get('name') for m in metrics]

            rows_data = []
            root = data.get('data', {}).get('root', {})

            def extraer_filas(nodo, valores_padres=None):
                valores_padres = valores_padres or []
                element = nodo.get('element', {})
                val_attr = element.get('formValues', [element.get('name', '')])

                current_vals = valores_padres + (val_attr if val_attr else [str(element.get('name', ''))])

                if 'children' in nodo:
                    for hijo in nodo['children']:
                        extraer_filas(hijo, current_vals)
                else:
                    metricas_vals = []
                    for v in nodo.get('metrics', {}).values():
                        metricas_vals.append(v.get('fv'))
                    if current_vals or metricas_vals:
                        rows_data.append(current_vals + metricas_vals)

            if 'children' in root:
                for child in root['children']:
                    extraer_filas(child)

            if rows_data and columns:
                # Ajustar longitudes si hay diferencias
                max_len = len(columns)
                rows_norm = [
                    (r + [None] * max_len)[:max_len] for r in rows_data
                ]
                df = pd.DataFrame(rows_norm, columns=columns)

                for col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col])
                    except Exception:
                        pass

                print(f"  ✅ DataFrame: {len(df)} filas × {len(df.columns)} columnas")
                return df

        except Exception as e:
            print(f"  ❌ Error convirtiendo JSON a DataFrame: {e}")
        return None

    # ----------------------------------------------------------------
    # PROCESAMIENTO DE PREGUNTAS
    # ----------------------------------------------------------------

    def procesar_pregunta(
        self,
        pregunta: str,
        session_id: str = 'default',
        source_idx: int = 0
    ) -> Dict[str, Any]:
        """Punto de entrada principal para procesar preguntas del usuario"""

        t_inicio = time.time()

        if not self.is_authenticated:
            if not self.authenticate():
                return {'error': 'No se pudo autenticar con MicroStrategy'}

        if not self.available_sources:
            return {
                'error': '⚠️ No se encontraron fuentes de datos en MicroStrategy.',
                'sugerencia': 'Crea al menos un cubo o reporte con datos en MicroStrategy Workstation y asegúrate de que tu usuario tenga permisos de ejecución.'
            }

        # Seleccionar fuente (con búsqueda inteligente por nombre)
        source = self._seleccionar_fuente(pregunta, source_idx)

        print(f"\n💬 Pregunta: {pregunta}")
        print(f"   📂 Fuente: {source['nombre']} ({source['tipo']})")

        # Obtener datos
        df = self._obtener_datos_fuente(source['id'], source['tipo'], source['nombre'])

        # Si la fuente no funciona, intentar con las siguientes
        if df is None or df.empty:
            for i, alt_source in enumerate(self.available_sources):
                if alt_source['id'] == source['id']:
                    continue
                df = self._obtener_datos_fuente(alt_source['id'], alt_source['tipo'], alt_source['nombre'])
                if df is not None and not df.empty:
                    source = alt_source
                    break

        if df is None or df.empty:
            return {
                'error': '❌ No se pudieron obtener datos de ninguna fuente disponible.',
                'sugerencia': 'Verifica que los cubos/reportes tengan datos y que tu usuario pueda ejecutarlos.'
            }

        self.last_query_result = df
        self.nlp._cargar_intenciones_bd()  # Refrescar intenciones

        # Analizar intención
        intencion = self.nlp.detectar_intencion(pregunta)

        # Generar respuesta
        respuesta = self._generar_respuesta(pregunta, df, intencion)

        # Análisis automático del conjunto de datos
        stats = self.analizador.analizar_dataframe(df)

        # Log en Supabase
        t_ms = int((time.time() - t_inicio) * 1000)
        self.db.log_conversacion(
            session_id=session_id,
            pregunta=pregunta,
            respuesta=respuesta,
            fuente_usada=source['nombre'],
            tipo_fuente=source['tipo'],
            tipo_analisis=intencion['principal'],
            registros=len(df),
            tiempo_ms=t_ms
        )
        self.db.registrar_uso_intencion(intencion['principal'])

        return {
            'success': True,
            'respuesta': respuesta,
            'fuente_usada': source['nombre'],
            'tipo_fuente': source['tipo'],
            'registros': len(df),
            'columnas': list(df.columns),
            'metricas': list(df.select_dtypes(include='number').columns),
            'dimensiones': list(df.select_dtypes(include='object').columns),
            'intencion': intencion['principal'],
            'interpretacion': stats.get('interpretacion', []),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tiempo_ms': t_ms,
            'puede_exportar': True
        }

    def _seleccionar_fuente(self, pregunta: str, idx: int = 0) -> Dict:
        """Seleccionar la fuente más relevante según la pregunta"""
        pregunta_lower = pregunta.lower()

        # Buscar fuente que coincida con palabras de la pregunta
        for source in self.available_sources:
            nombre = source['nombre'].lower()
            # Si el nombre del reporte está mencionado en la pregunta
            palabras_nombre = re.findall(r'\b\w{4,}\b', nombre)
            for palabra in palabras_nombre:
                if palabra in pregunta_lower:
                    return source

        # Fallback: usar índice
        if 0 <= idx < len(self.available_sources):
            return self.available_sources[idx]
        return self.available_sources[0]

    # ----------------------------------------------------------------
    # GENERADORES DE RESPUESTA
    # ----------------------------------------------------------------

    def _generar_respuesta(self, pregunta: str, df: pd.DataFrame, intencion: Dict) -> str:
        """Despachar al generador correcto según la intención"""
        tipo = intencion['principal']
        numero = intencion.get('numero')

        # Columna objetivo detectada por NLP
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        cat_cols = df.select_dtypes(include=['object']).columns.tolist()
        col_objetivo = self.nlp.detectar_columna_objetivo(pregunta, list(df.columns))

        dispatch = {
            'dashboard':      lambda: self._resp_dashboard(df, numeric_cols, cat_cols),
            'prediccion':     lambda: self._resp_prediccion(df, col_objetivo or (numeric_cols[0] if numeric_cols else None)),
            'correlacion':    lambda: self._resp_correlacion(df, numeric_cols),
            'ranking_top':    lambda: self._resp_ranking(df, numeric_cols, cat_cols, numero or 10, 'desc'),
            'ranking_bottom': lambda: self._resp_ranking(df, numeric_cols, cat_cols, numero or 10, 'asc'),
            'total':          lambda: self._resp_totales(df, numeric_cols, col_objetivo),
            'promedio':       lambda: self._resp_promedios(df, numeric_cols, col_objetivo),
            'desglose':       lambda: self._resp_desglose(df, numeric_cols, cat_cols, col_objetivo),
            'comparacion':    lambda: self._resp_comparacion(df, pregunta, numeric_cols, cat_cols),
            'maximo':         lambda: self._resp_extremo(df, numeric_cols, cat_cols, 'max'),
            'minimo':         lambda: self._resp_extremo(df, numeric_cols, cat_cols, 'min'),
            'exportar':       lambda: "✅ Usa el botón **Exportar Excel** para descargar todos los datos en formato .xlsx",
        }

        func = dispatch.get(tipo, lambda: self._resp_general(df, numeric_cols, cat_cols))
        try:
            return func()
        except Exception as e:
            return self._resp_general(df, numeric_cols, cat_cols)

    def _resp_totales(self, df, numeric_cols, col_objetivo=None) -> str:
        cols = [col_objetivo] if col_objetivo and col_objetivo in numeric_cols else numeric_cols[:6]
        r = "📊 **Totales:**\n\n"
        for col in cols:
            val = df[col].sum()
            r += f"• **{col}**: {val:,.2f}\n"
        r += f"\n_Calculado sobre {len(df):,} registros_"
        return r

    def _resp_promedios(self, df, numeric_cols, col_objetivo=None) -> str:
        cols = [col_objetivo] if col_objetivo and col_objetivo in numeric_cols else numeric_cols[:6]
        r = "📊 **Promedios:**\n\n"
        for col in cols:
            r += f"• **{col}**: {df[col].mean():,.2f} (min: {df[col].min():,.2f} | max: {df[col].max():,.2f})\n"
        r += f"\n_Basado en {len(df):,} registros_"
        return r

    def _resp_ranking(self, df, numeric_cols, cat_cols, limite=10, orden='desc') -> str:
        if not numeric_cols or not cat_cols:
            return "⚠️ Se necesitan métricas y dimensiones para generar rankings."

        metrica = numeric_cols[0]
        dimension = cat_cols[0]

        agrupado = df.groupby(dimension)[metrica].sum().sort_values(
            ascending=(orden == 'asc')
        ).head(limite)

        emoji = "🏆" if orden == 'desc' else "📉"
        titulo = f"Top {limite}" if orden == 'desc' else f"Bottom {limite}"
        r = f"{emoji} **{titulo} — {dimension} por {metrica}:**\n\n"

        total = agrupado.sum()
        for i, (nombre, valor) in enumerate(agrupado.items(), 1):
            pct = (valor / total * 100) if total != 0 else 0
            medalla = ["🥇", "🥈", "🥉"][i - 1] if (i <= 3 and orden == 'desc') else f"{i}."
            r += f"{medalla} **{nombre}**: {valor:,.2f} ({pct:.1f}%)\n"

        return r

    def _resp_desglose(self, df, numeric_cols, cat_cols, col_objetivo=None) -> str:
        if not numeric_cols or not cat_cols:
            return "⚠️ No hay suficientes datos para generar un desglose."

        metrica = col_objetivo if (col_objetivo and col_objetivo in numeric_cols) else numeric_cols[0]
        dimension = cat_cols[0]

        agrupado = df.groupby(dimension)[metrica].sum().sort_values(ascending=False)
        total = agrupado.sum()

        r = f"📂 **Desglose de {metrica} por {dimension}:**\n\n"
        for nombre, valor in agrupado.items():
            pct = (valor / total * 100) if total != 0 else 0
            bar = "█" * int(pct / 5)
            r += f"• **{nombre}**: {valor:,.2f} ({pct:.1f}%) {bar}\n"

        r += f"\n**Total**: {total:,.2f}"
        return r

    def _resp_comparacion(self, df, pregunta, numeric_cols, cat_cols) -> str:
        if not numeric_cols:
            return "⚠️ No hay métricas para comparar."

        metrica = numeric_cols[0]
        r = f"⚖️ **Comparativa — {metrica}:**\n\n"

        if cat_cols:
            dimension = cat_cols[0]
            grupos = df.groupby(dimension)[metrica].agg(['sum', 'mean', 'count'])
            for nombre, row in grupos.iterrows():
                r += f"• **{nombre}**\n"
                r += f"  Total: {row['sum']:,.2f} | Promedio: {row['mean']:,.2f} | Registros: {int(row['count'])}\n"
        else:
            # Comparar primeros vs últimos 50%
            mitad = len(df) // 2
            primera = df[metrica].iloc[:mitad]
            segunda = df[metrica].iloc[mitad:]
            r += f"• **Primera mitad**: Total={primera.sum():,.2f} | Promedio={primera.mean():,.2f}\n"
            r += f"• **Segunda mitad**: Total={segunda.sum():,.2f} | Promedio={segunda.mean():,.2f}\n"
            diff = segunda.mean() - primera.mean()
            r += f"\n📈 Diferencia en promedio: **{diff:+,.2f}**"

        return r

    def _resp_extremo(self, df, numeric_cols, cat_cols, tipo='max') -> str:
        if not numeric_cols:
            return "⚠️ No hay datos numéricos."

        metrica = numeric_cols[0]
        r = f"{'🔝' if tipo=='max' else '🔻'} **{'Máximo' if tipo=='max' else 'Mínimo'} — {metrica}:**\n\n"

        if tipo == 'max':
            idx = df[metrica].idxmax()
            val = df[metrica].max()
        else:
            idx = df[metrica].idxmin()
            val = df[metrica].min()

        fila = df.loc[idx]
        r += f"• **Valor**: {val:,.2f}\n"
        for col in cat_cols[:3]:
            r += f"• **{col}**: {fila[col]}\n"

        return r

    def _resp_prediccion(self, df, columna=None) -> str:
        pred = self.analizador.prediccion_ml(df, columna)

        if 'error' in pred:
            return f"⚠️ {pred['error']}"

        r = f"🔮 **Predicción ML — {pred['columna']}:**\n\n"
        r += f"📈 Tendencia: **{pred['tendencia']}**\n"
        r += f"🎯 Predicción próximo período: **{pred['prediccion']:,.2f}**\n"
        r += f"📊 Intervalo de confianza: {pred['ci_lower']:,.2f} — {pred['ci_upper']:,.2f}\n"
        r += f"📉 Promedio histórico: {pred['promedio_historico']:,.2f}\n\n"
        r += f"📋 Últimos {len(pred['ultimos_valores'])} valores:\n"
        for i, v in enumerate(pred['ultimos_valores'], 1):
            r += f"   {i}. {v:,.2f}\n"
        r += f"\n_Método: {pred['metodo']}_"
        return r

    def _resp_correlacion(self, df, numeric_cols) -> str:
        if len(numeric_cols) < 2:
            return "⚠️ Se necesitan al menos 2 métricas numéricas para correlaciones."

        corr = df[numeric_cols].corr()
        pares = []
        for i in range(len(numeric_cols)):
            for j in range(i + 1, len(numeric_cols)):
                val = float(corr.iloc[i, j])
                if abs(val) > 0.2:
                    pares.append((numeric_cols[i], numeric_cols[j], val))

        pares.sort(key=lambda x: abs(x[2]), reverse=True)

        r = "🔗 **Análisis de Correlaciones:**\n\n"
        if not pares:
            r += "No se encontraron correlaciones significativas (r > 0.2).\n"
        else:
            for v1, v2, val in pares[:8]:
                fuerza = "fuerte" if abs(val) > 0.7 else ("moderada" if abs(val) > 0.4 else "débil")
                signo = "positiva ↗" if val > 0 else "negativa ↘"
                emoji = "🟢" if abs(val) > 0.7 else ("🟡" if abs(val) > 0.4 else "🔵")
                r += f"{emoji} **{v1}** ↔ **{v2}**: r={val:.3f} ({fuerza} {signo})\n"

        r += "\n_r > 0.7 = fuerte | 0.4-0.7 = moderada | 0.2-0.4 = débil_"
        return r

    def _resp_dashboard(self, df, numeric_cols, cat_cols) -> str:
        r = "📊 **DASHBOARD — Resumen Ejecutivo:**\n"
        r += "─" * 40 + "\n\n"

        # KPIs
        r += "🎯 **KPIs Principales:**\n"
        for col in numeric_cols[:5]:
            total = df[col].sum()
            prom = df[col].mean()
            r += f"• **{col}**: Total={total:,.0f} | Avg={prom:,.0f}\n"

        # Top 3 por categoría principal
        if cat_cols and numeric_cols:
            dim = cat_cols[0]
            met = numeric_cols[0]
            top3 = df.groupby(dim)[met].sum().nlargest(3)
            r += f"\n🏆 **Top 3 {dim} por {met}:**\n"
            for i, (nombre, val) in enumerate(top3.items(), 1):
                medallas = ["🥇", "🥈", "🥉"]
                r += f"{medallas[i-1]} {nombre}: {val:,.0f}\n"

        # Tendencia
        if numeric_cols and len(df) >= 2:
            col = numeric_cols[0]
            mitad = len(df) // 2
            prim_avg = df[col].iloc[:mitad].mean()
            seg_avg = df[col].iloc[mitad:].mean()
            if seg_avg > prim_avg * 1.02:
                r += "\n📈 **Tendencia global:** Creciente ↗"
            elif seg_avg < prim_avg * 0.98:
                r += "\n📉 **Tendencia global:** Decreciente ↘"
            else:
                r += "\n➡️ **Tendencia global:** Estable →"

        r += f"\n\n_Análisis de {len(df):,} registros | {len(df.columns)} variables_"
        return r

    def _resp_general(self, df, numeric_cols, cat_cols) -> str:
        r = f"📋 **Resumen de datos** ({len(df):,} registros):\n\n"

        if numeric_cols:
            r += "**📐 Métricas:**\n"
            for col in numeric_cols[:5]:
                r += f"• **{col}**: Total={df[col].sum():,.0f} | Avg={df[col].mean():,.0f} | Max={df[col].max():,.0f}\n"

        if cat_cols:
            r += "\n**📁 Dimensiones:**\n"
            for col in cat_cols[:4]:
                r += f"• **{col}**: {df[col].nunique()} valores únicos\n"

        r += "\n💡 _Puedes preguntar: totales, promedios, top 10, desglose por X, predicción, correlaciones o dashboard._"
        return r

    # ----------------------------------------------------------------
    # EXPORT
    # ----------------------------------------------------------------

    def exportar_excel(self, filename: str = None) -> Optional[str]:
        """Exportar último resultado a Excel"""
        if self.last_query_result is None:
            return None

        try:
            import io
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                self.last_query_result.to_excel(writer, index=False, sheet_name='Datos')

                # Estadísticas en segunda hoja
                stats = self.analizador.analizar_dataframe(self.last_query_result)
                numeric_cols = self.last_query_result.select_dtypes(include='number').columns.tolist()
                if numeric_cols:
                    desc = self.last_query_result[numeric_cols].describe()
                    desc.to_excel(writer, sheet_name='Estadísticas')

            output.seek(0)
            return output
        except Exception as e:
            print(f"❌ Error exportando Excel: {e}")
            return None

    def obtener_fuentes(self) -> List[Dict]:
        """Listar fuentes disponibles"""
        return [{'id': s['id'], 'nombre': s['nombre'], 'tipo': s['tipo']}
                for s in self.available_sources]

    def obtener_estadisticas_bd(self) -> Dict:
        """Estadísticas de uso desde Supabase"""
        return self.db.obtener_estadisticas()
