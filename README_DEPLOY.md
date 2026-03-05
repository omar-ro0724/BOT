# 🚀 MSTR Intelligence Bot v2.0 — Guía Completa

## Arquitectura

```
┌─────────────────────────────────────────────────────────┐
│  Navegador / Dispositivo (cualquier red)                │
│           ↕  HTTPS                                      │
├─────────────────────────────────────────────────────────┤
│  Railway (Cloud) — app_mejorada.py + bot_mejorado.py   │
│           ↕ REST API                                    │
├─────────────────────────────────────────────────────────┤
│  MicroStrategy Cloud (Cencosud)                         │
│           ↕ PostgreSQL                                  │
├─────────────────────────────────────────────────────────┤
│  Supabase — Logs · Caché · Entrenamiento                │
└─────────────────────────────────────────────────────────┘
```

---

## PASO 1 — Configurar Supabase (base de datos)

1. Crea cuenta gratuita en https://supabase.com
2. **New Project** → nombre: `mstr-bot` → elige región cercana (us-east-1)
3. Guarda la contraseña de la BD
4. Ve a **SQL Editor → New Query**
5. Copia y pega el SQL desde:
   ```
   http://tu-app-url/api/supabase-sql
   ```
   O usa el contenido del archivo `supabase_manager.py` (propiedad `SQL_SETUP`)
6. Ejecuta con **Run**
7. Ve a **Settings → API** y copia:
   - `Project URL` → `SUPABASE_URL`
   - `anon / public key` → `SUPABASE_KEY`

---

## PASO 2 — Deploy en Railway (gratis, acceso desde cualquier red)

### 2a. Preparar repositorio

```bash
# En tu máquina local
git init
git add .
git commit -m "MSTR Intelligence Bot v2.0"

# Crear repo en GitHub y subir
git remote add origin https://github.com/TU_USUARIO/mstr-bot.git
git push -u origin main
```

### 2b. Deploy en Railway

1. Ve a https://railway.app → **New Project → Deploy from GitHub**
2. Conecta tu repositorio `mstr-bot`
3. Railway detecta automáticamente el `Procfile` y `requirements.txt`

### 2c. Configurar Variables de Entorno en Railway

En tu proyecto Railway → **Variables** → agrega:

| Variable | Valor |
|----------|-------|
| `MSTR_BASE_URL` | `https://cencosud.cloud.microstrategy.com/MicroStrategyLibrary/api` |
| `MSTR_USERNAME` | tu usuario |
| `MSTR_PASSWORD` | tu contraseña |
| `MSTR_PROJECT_ID` | `7AAAF1F6464A68C0EB8CFAB5B9E02F49` |
| `SUPABASE_URL` | URL de tu proyecto Supabase |
| `SUPABASE_KEY` | anon key de Supabase |
| `SECRET_KEY` | string aleatorio largo (genera con `python -c "import secrets; print(secrets.token_hex(32))"`) |
| `CACHE_TTL_MIN` | `30` |

4. Railway genera automáticamente una URL como `https://mstr-bot-xxxx.railway.app`
5. **Esa URL funciona desde cualquier dispositivo y red** ✅

---

## PASO 3 — Ejecución local (desarrollo)

```bash
# 1. Clonar / descargar el proyecto
cd mstr-bot

# 2. Crear entorno virtual
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar credenciales
cp .env.example .env
# Edita .env con tus credenciales

# 5. Ejecutar
python app_mejorada.py

# 6. Abrir
# http://localhost:5000
```

---

## PASO 4 — Entrenar el Bot

### Desde la interfaz web
1. Abre el bot en el navegador
2. Sidebar izquierdo → **"+ Entrenar bot"**
3. Escribe una frase de ejemplo y selecciona la intención
4. El bot la aprende inmediatamente (sin reiniciar)

### Desde Supabase directamente
```sql
INSERT INTO intenciones (texto_ejemplo, intencion, respuesta_hint)
VALUES 
  ('muéstrame el consolidado de ingresos por tienda', 'desglose', 'Ingresos por tienda'),
  ('cuántos clientes nuevos tuvimos este mes', 'total', 'Total clientes nuevos'),
  ('comparar ventas de online vs presencial', 'comparacion', 'Ventas por canal');
```

### Intenciones disponibles para entrenamiento

| Intención | Cuándo usarla |
|-----------|---------------|
| `total` | Sumas, totales, cuánto en total |
| `promedio` | Medias, promedios, promedio de |
| `ranking_top` | Top N, mejores, mayores |
| `ranking_bottom` | Peores, menores, bottom N |
| `desglose` | Por categoría, desglose de, dividido por |
| `comparacion` | vs, versus, comparar, diferencia entre |
| `prediccion` | Predecir, forecast, próximo período |
| `correlacion` | Correlación, relación entre variables |
| `dashboard` | Dashboard, KPIs, resumen ejecutivo |
| `tendencia` | Evolución, histórico, a lo largo del tiempo |

---

## Estructura de archivos

```
mstr-bot/
├── app_mejorada.py       # 🌐 Flask server + UI
├── bot_mejorado.py       # 🤖 Lógica del bot + ML
├── supabase_manager.py   # 🗄️ Base de datos Supabase
├── requirements.txt      # 📦 Dependencias
├── Procfile              # 🚀 Config Railway/Render
├── railway.toml          # ⚙️ Config Railway
├── .env.example          # 🔑 Template de variables
└── README_DEPLOY.md      # 📖 Esta guía
```

---

## Tablas en Supabase

| Tabla | Descripción |
|-------|-------------|
| `conversaciones` | Log completo de cada chat (pregunta, respuesta, fuente, tiempo) |
| `cache_mstr` | Caché de datos de MicroStrategy (evita re-ejecutar cubos) |
| `intenciones` | Ejemplos de entrenamiento del bot (editable en tiempo real) |
| `fuentes_datos` | Catálogo de cubos/reportes disponibles con sus métricas |

---

## Funcionalidades

- ✅ Chat en lenguaje natural con MicroStrategy
- ✅ NLP con intenciones desde base de datos (entrenable)
- ✅ Caché inteligente de datos (TTL configurable)
- ✅ Logs de conversaciones en Supabase
- ✅ Predicciones ML (promedio móvil + regresión)
- ✅ Análisis de correlaciones
- ✅ Dashboard ejecutivo automático
- ✅ Desglose, rankings, totales, promedios
- ✅ Interpretación automática en lenguaje natural
- ✅ Exportación a Excel (2 hojas: datos + estadísticas)
- ✅ Selector de fuente de datos por sesión
- ✅ Accesible desde cualquier dispositivo/red vía Railway

---

## Solución de problemas

### "No se encontraron fuentes de datos"
1. Verifica credenciales en variables de entorno
2. Asegúrate de que el usuario tenga permisos en MicroStrategy
3. Crea al menos 1 cubo o reporte con datos en el proyecto

### Supabase no conecta
1. Verifica `SUPABASE_URL` y `SUPABASE_KEY` en variables de entorno
2. Ejecuta el SQL de setup en Supabase SQL Editor
3. El bot funciona sin Supabase, solo pierde logs y caché

### Error de autenticación MicroStrategy
1. Prueba las credenciales directamente en la URL del API
2. Verifica que `loginMode=1` sea correcto para tu instancia
3. Verifica que el `project_id` sea correcto
