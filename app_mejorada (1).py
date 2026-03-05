"""
APP MEJORADA v2.0 — MicroStrategy Intelligence Bot
Flask server con UI enterprise premium + Supabase + Deploy-ready
"""

import os
import io
import json
import secrets
import uuid
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify, send_file, session
from bot_mejorado import BotMejorado
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

# ── CONFIGURACIÓN DESDE VARIABLES DE ENTORNO ─────────────────────────────────
MSTR_CONFIG = {
    'base_url':   os.environ.get('MSTR_BASE_URL',   'https://cencosud.cloud.microstrategy.com/MicroStrategyLibrary/api'),
    'username':   os.environ.get('MSTR_USERNAME',   ''),
    'password':   os.environ.get('MSTR_PASSWORD',   ''),
    'project_id': os.environ.get('MSTR_PROJECT_ID', ''),
}

bot = None


def inicializar_bot():
    global bot
    print("\n" + "═" * 60)
    print("  🤖 MSTR INTELLIGENCE BOT v2.0 — INICIANDO")
    print("═" * 60)
    print(f"  URL:      {MSTR_CONFIG['base_url']}")
    print(f"  Usuario:  {MSTR_CONFIG['username']}")
    print(f"  Proyecto: {MSTR_CONFIG['project_id']}")
    print("═" * 60 + "\n")

    bot = BotMejorado(
        MSTR_CONFIG['base_url'],
        MSTR_CONFIG['username'],
        MSTR_CONFIG['password'],
        MSTR_CONFIG['project_id']
    )

    success = bot.authenticate()
    if success:
        print(f"\n✅ Bot listo | Fuentes: {len(bot.available_sources)}")
    else:
        print("\n⚠️  Bot inició sin autenticación — revisa credenciales")

    return success


# ── HTML TEMPLATE ─────────────────────────────────────────────────────────────
HTML = r'''<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MSTR Intelligence Bot</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg:       #06090f;
      --surface:  #0c1220;
      --panel:    #101827;
      --border:   #1a2740;
      --border2:  #243553;
      --accent:   #3db8f5;
      --accent2:  #00e5b4;
      --accent3:  #f5a623;
      --danger:   #f5534a;
      --text:     #dce8f5;
      --muted:    #5a7a9e;
      --mono:     'JetBrains Mono', monospace;
      --sans:     'Plus Jakarta Sans', sans-serif;
    }

    * { margin:0; padding:0; box-sizing:border-box; }

    body {
      font-family: var(--sans);
      background: var(--bg);
      color: var(--text);
      height: 100vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    /* ── TOP BAR ── */
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 24px;
      height: 56px;
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }

    .logo {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .logo-icon {
      width: 32px;
      height: 32px;
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      border-radius: 8px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 16px;
    }

    .logo-text {
      font-weight: 700;
      font-size: 15px;
      letter-spacing: 0.02em;
    }

    .logo-sub {
      font-family: var(--mono);
      font-size: 10px;
      color: var(--muted);
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .status-bar {
      display: flex;
      align-items: center;
      gap: 20px;
    }

    .status-pill {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 4px 12px;
      border-radius: 20px;
      border: 1px solid var(--border2);
      font-size: 11px;
      font-family: var(--mono);
      color: var(--muted);
    }

    .status-pill.online .dot { background: var(--accent2); box-shadow: 0 0 6px var(--accent2); }
    .status-pill.offline .dot { background: var(--danger); }
    .dot { width: 6px; height: 6px; border-radius: 50%; }

    .stat-badge {
      font-family: var(--mono);
      font-size: 11px;
      color: var(--muted);
    }
    .stat-badge span { color: var(--accent); font-weight: 600; }

    /* ── QUICK ACTIONS ── */
    .quickbar {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 24px;
      background: var(--panel);
      border-bottom: 1px solid var(--border);
      overflow-x: auto;
      flex-shrink: 0;
    }

    .quickbar::-webkit-scrollbar { height: 0; }

    .quick-label {
      font-size: 10px;
      color: var(--muted);
      font-family: var(--mono);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      white-space: nowrap;
      margin-right: 4px;
    }

    .qbtn {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 6px 14px;
      background: transparent;
      border: 1px solid var(--border2);
      border-radius: 6px;
      color: var(--text);
      font-family: var(--sans);
      font-size: 12px;
      font-weight: 500;
      cursor: pointer;
      white-space: nowrap;
      transition: all 0.15s ease;
    }

    .qbtn:hover {
      border-color: var(--accent);
      color: var(--accent);
      background: rgba(61, 184, 245, 0.06);
    }

    .qbtn .icon { font-size: 13px; }

    /* ── MAIN LAYOUT ── */
    .main {
      display: flex;
      flex: 1;
      overflow: hidden;
    }

    /* ── SIDEBAR ── */
    .sidebar {
      width: 260px;
      background: var(--surface);
      border-right: 1px solid var(--border);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
      overflow: hidden;
    }

    .sidebar-section {
      padding: 16px;
      border-bottom: 1px solid var(--border);
    }

    .sidebar-title {
      font-size: 9px;
      font-family: var(--mono);
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
      margin-bottom: 10px;
    }

    .source-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 10px;
      border-radius: 6px;
      cursor: pointer;
      transition: background 0.12s;
      margin-bottom: 2px;
    }

    .source-item:hover, .source-item.active {
      background: rgba(61, 184, 245, 0.08);
    }

    .source-item.active .source-name {
      color: var(--accent);
    }

    .source-type {
      font-size: 9px;
      padding: 2px 6px;
      border-radius: 3px;
      font-family: var(--mono);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      flex-shrink: 0;
    }

    .type-cubo    { background: rgba(61,184,245,0.15); color: var(--accent); }
    .type-reporte { background: rgba(0,229,180,0.15); color: var(--accent2); }
    .type-dossier { background: rgba(245,166,35,0.15); color: var(--accent3); }
    .type-objeto  { background: rgba(100,100,150,0.15); color: var(--muted); }

    .source-name {
      font-size: 12px;
      color: var(--text);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .sidebar-stats {
      padding: 16px;
      flex: 1;
      overflow-y: auto;
    }

    .stats-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }

    .stat-card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px;
    }

    .stat-val {
      font-family: var(--mono);
      font-size: 20px;
      font-weight: 600;
      color: var(--accent);
    }

    .stat-lbl {
      font-size: 10px;
      color: var(--muted);
      margin-top: 2px;
      font-family: var(--mono);
    }

    .train-btn {
      width: 100%;
      padding: 10px;
      background: transparent;
      border: 1px dashed var(--border2);
      border-radius: 6px;
      color: var(--muted);
      font-size: 12px;
      cursor: pointer;
      transition: all 0.15s;
      margin-top: 12px;
      font-family: var(--sans);
    }

    .train-btn:hover {
      border-color: var(--accent2);
      color: var(--accent2);
    }

    /* ── CHAT ── */
    .chat-wrapper {
      flex: 1;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .chat-messages {
      flex: 1;
      overflow-y: auto;
      padding: 24px 28px;
    }

    .chat-messages::-webkit-scrollbar { width: 4px; }
    .chat-messages::-webkit-scrollbar-track { background: transparent; }
    .chat-messages::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }

    .msg {
      display: flex;
      margin-bottom: 20px;
      animation: fadeUp 0.25s ease;
    }

    @keyframes fadeUp {
      from { opacity: 0; transform: translateY(8px); }
      to   { opacity: 1; transform: translateY(0); }
    }

    .msg.user { justify-content: flex-end; }

    .msg-bubble {
      max-width: 72%;
      padding: 14px 18px;
      border-radius: 14px;
      line-height: 1.65;
      font-size: 14px;
    }

    .msg.bot .msg-bubble {
      background: var(--panel);
      border: 1px solid var(--border2);
    }

    .msg.user .msg-bubble {
      background: linear-gradient(135deg, #1a3a5c, #0f2540);
      border: 1px solid rgba(61, 184, 245, 0.3);
      color: #b8d9f5;
    }

    .msg-meta {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 8px;
      font-family: var(--mono);
      font-size: 10px;
      color: var(--muted);
    }

    .msg-source {
      padding: 1px 6px;
      border-radius: 3px;
      background: rgba(61,184,245,0.1);
      color: var(--accent);
    }

    .msg-time { color: var(--muted); }

    .msg-text pre,
    .msg-text code {
      font-family: var(--mono);
      font-size: 12px;
      white-space: pre-wrap;
    }

    .msg-text strong { color: #a0d4f5; }

    .msg-interp {
      margin-top: 12px;
      padding: 10px 12px;
      background: rgba(0,229,180,0.05);
      border-left: 2px solid var(--accent2);
      border-radius: 0 6px 6px 0;
      font-size: 12px;
      color: #8ac8b8;
    }

    .msg-interp .interp-title {
      font-family: var(--mono);
      font-size: 10px;
      color: var(--accent2);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 4px;
    }

    .msg-actions {
      display: flex;
      gap: 6px;
      margin-top: 10px;
    }

    .act-btn {
      padding: 5px 12px;
      border-radius: 5px;
      border: 1px solid var(--border2);
      background: transparent;
      color: var(--muted);
      font-size: 11px;
      cursor: pointer;
      font-family: var(--sans);
      transition: all 0.12s;
    }

    .act-btn:hover {
      border-color: var(--accent2);
      color: var(--accent2);
    }

    /* ── TYPING INDICATOR ── */
    .typing {
      display: none;
      align-items: center;
      gap: 8px;
      padding: 12px 18px;
      margin: 0 28px 16px;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 14px;
      width: fit-content;
      font-size: 12px;
      color: var(--muted);
      font-family: var(--mono);
    }

    .typing.show { display: flex; animation: fadeUp 0.2s ease; }

    .typing-dots { display: flex; gap: 4px; }
    .typing-dots span {
      width: 5px; height: 5px;
      background: var(--accent);
      border-radius: 50%;
      animation: pulse 1.2s infinite;
    }
    .typing-dots span:nth-child(2) { animation-delay: 0.2s; }
    .typing-dots span:nth-child(3) { animation-delay: 0.4s; }

    @keyframes pulse {
      0%, 60%, 100% { opacity: 0.2; transform: scale(1); }
      30% { opacity: 1; transform: scale(1.2); }
    }

    /* ── INPUT ── */
    .input-area {
      padding: 16px 24px 20px;
      background: var(--surface);
      border-top: 1px solid var(--border);
      flex-shrink: 0;
    }

    .input-row {
      display: flex;
      gap: 10px;
      align-items: flex-end;
    }

    .input-wrap {
      flex: 1;
      position: relative;
    }

    #msgInput {
      width: 100%;
      padding: 13px 18px;
      padding-right: 48px;
      background: var(--panel);
      border: 1px solid var(--border2);
      border-radius: 10px;
      color: var(--text);
      font-size: 14px;
      font-family: var(--sans);
      outline: none;
      resize: none;
      line-height: 1.5;
      transition: border-color 0.15s;
      max-height: 120px;
    }

    #msgInput:focus { border-color: var(--accent); }
    #msgInput::placeholder { color: var(--muted); }

    .send-btn {
      padding: 13px 22px;
      background: linear-gradient(135deg, var(--accent), #1a8fbf);
      border: none;
      border-radius: 10px;
      color: white;
      font-weight: 600;
      font-size: 13px;
      cursor: pointer;
      transition: all 0.15s;
      font-family: var(--sans);
      white-space: nowrap;
    }

    .send-btn:hover { filter: brightness(1.1); transform: translateY(-1px); }
    .send-btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }

    .input-hint {
      font-size: 11px;
      color: var(--muted);
      margin-top: 6px;
      font-family: var(--mono);
    }

    /* ── MODAL TRAINING ── */
    .modal-overlay {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.7);
      z-index: 100;
      align-items: center;
      justify-content: center;
    }

    .modal-overlay.show { display: flex; }

    .modal {
      background: var(--panel);
      border: 1px solid var(--border2);
      border-radius: 14px;
      padding: 28px;
      width: 480px;
      max-width: 90vw;
    }

    .modal h3 {
      font-size: 16px;
      margin-bottom: 6px;
      color: var(--accent2);
    }

    .modal p {
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 20px;
    }

    .form-group {
      margin-bottom: 14px;
    }

    .form-label {
      font-size: 11px;
      font-family: var(--mono);
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 5px;
      display: block;
    }

    .form-input, .form-select {
      width: 100%;
      padding: 10px 14px;
      background: var(--surface);
      border: 1px solid var(--border2);
      border-radius: 7px;
      color: var(--text);
      font-size: 13px;
      font-family: var(--sans);
      outline: none;
    }

    .form-input:focus, .form-select:focus { border-color: var(--accent); }
    .form-select option { background: var(--surface); }

    .modal-actions {
      display: flex;
      gap: 10px;
      margin-top: 20px;
      justify-content: flex-end;
    }

    .btn-cancel {
      padding: 9px 20px;
      background: transparent;
      border: 1px solid var(--border2);
      border-radius: 7px;
      color: var(--muted);
      cursor: pointer;
      font-family: var(--sans);
    }

    .btn-save {
      padding: 9px 20px;
      background: var(--accent2);
      border: none;
      border-radius: 7px;
      color: #06090f;
      font-weight: 600;
      cursor: pointer;
      font-family: var(--sans);
    }

    /* ── EMPTY STATE ── */
    .empty-state {
      text-align: center;
      padding: 60px 20px;
      color: var(--muted);
    }

    .empty-icon { font-size: 48px; margin-bottom: 16px; opacity: 0.4; }
    .empty-title { font-size: 18px; font-weight: 600; margin-bottom: 8px; color: var(--text); }
    .empty-sub { font-size: 13px; line-height: 1.6; }

    .suggestion-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: center;
      margin-top: 20px;
    }

    .chip {
      padding: 7px 14px;
      border: 1px solid var(--border2);
      border-radius: 20px;
      font-size: 12px;
      cursor: pointer;
      color: var(--muted);
      transition: all 0.15s;
    }

    .chip:hover {
      border-color: var(--accent);
      color: var(--accent);
      background: rgba(61,184,245,0.05);
    }

    /* ── RESPONSIVE ── */
    @media (max-width: 900px) {
      .sidebar { display: none; }
    }

    @media (max-width: 768px) {
      body { overflow: auto; }

      .topbar {
        padding: 0 14px;
        height: auto;
        flex-wrap: wrap;
        gap: 8px;
        padding: 10px 14px;
      }

      .status-bar {
        gap: 10px;
        flex-wrap: wrap;
      }

      .stat-badge { font-size: 10px; }

      .quickbar {
        padding: 8px 12px;
        gap: 6px;
      }

      .qbtn {
        padding: 5px 10px;
        font-size: 11px;
      }

      .main {
        flex-direction: column;
        height: calc(100vh - 110px);
      }

      .chat-wrapper {
        height: 100%;
      }

      .chat-messages {
        padding: 14px 14px;
      }

      .msg-bubble {
        max-width: 92%;
        padding: 11px 14px;
        font-size: 13px;
      }

      .input-area {
        padding: 10px 12px 14px;
      }

      .input-row { gap: 8px; }

      #msgInput {
        font-size: 14px;
        padding: 11px 14px;
      }

      .send-btn {
        padding: 11px 16px;
        font-size: 13px;
      }

      .input-hint { font-size: 10px; }

      .empty-state { padding: 30px 14px; }
      .empty-icon { font-size: 36px; }
      .empty-title { font-size: 15px; }
      .empty-sub { font-size: 12px; }

      .suggestion-chips { gap: 6px; }
      .chip { font-size: 11px; padding: 6px 11px; }

      .msg-meta { flex-wrap: wrap; gap: 4px; }

      .modal { width: 95vw; padding: 20px; }

      .logo-text { font-size: 13px; }
      .logo-sub { font-size: 9px; }
      .logo-icon { width: 26px; height: 26px; font-size: 13px; }
    }

    @media (max-width: 480px) {
      .topbar { padding: 8px 10px; }

      .status-bar .stat-badge { display: none; }

      .quickbar { gap: 5px; }
      .quick-label { display: none; }

      .qbtn {
        padding: 5px 9px;
        font-size: 10px;
      }

      .send-btn {
        padding: 11px 13px;
        font-size: 12px;
      }

      .msg-bubble { max-width: 97%; }

      .msg-actions { flex-wrap: wrap; }
      .act-btn { font-size: 10px; padding: 4px 9px; }
    }
  </style>
</head>
<body>

<!-- TOP BAR -->
<div class="topbar">
  <div class="logo">
    <div class="logo-icon">📊</div>
    <div>
      <div class="logo-text">MSTR Intelligence</div>
      <div class="logo-sub">Analytics Bot v2.0</div>
    </div>
  </div>
  <div class="status-bar">
    <div class="status-pill offline" id="statusPill">
      <div class="dot"></div>
      <span id="statusText">Conectando...</span>
    </div>
    <div class="stat-badge">Fuentes: <span id="srcCount">—</span></div>
    <div class="stat-badge">Consultas hoy: <span id="queryCount">—</span></div>
  </div>
</div>

<!-- QUICK ACTIONS -->
<div class="quickbar">
  <span class="quick-label">Accesos rápidos:</span>
  <button class="qbtn" onclick="ask('Genera un dashboard completo con KPIs principales')">
    <span class="icon">📊</span> Dashboard
  </button>
  <button class="qbtn" onclick="ask('¿Cuáles son las ventas totales?')">
    <span class="icon">💰</span> Ventas totales
  </button>
  <button class="qbtn" onclick="ask('Top 10 por la métrica principal')">
    <span class="icon">🏆</span> Top 10
  </button>
  <button class="qbtn" onclick="ask('Predice los valores del próximo período')">
    <span class="icon">🔮</span> Predicción ML
  </button>
  <button class="qbtn" onclick="ask('Analiza las correlaciones entre todas las variables')">
    <span class="icon">🔗</span> Correlaciones
  </button>
  <button class="qbtn" onclick="ask('Desglose de ventas por cada categoría disponible')">
    <span class="icon">📂</span> Desglose
  </button>
  <button class="qbtn" onclick="exportarExcel()">
    <span class="icon">📥</span> Excel
  </button>
</div>

<!-- MAIN -->
<div class="main">

  <!-- SIDEBAR -->
  <div class="sidebar">
    <div class="sidebar-section">
      <div class="sidebar-title">Fuentes de datos</div>
      <div id="sourceList">
        <div style="font-size:12px; color:var(--muted); padding:8px 0;">Cargando fuentes...</div>
      </div>
    </div>

    <div class="sidebar-stats">
      <div class="sidebar-title">Estadísticas</div>
      <div class="stats-grid">
        <div class="stat-card">
          <div class="stat-val" id="statTotal">—</div>
          <div class="stat-lbl">Total consultas</div>
        </div>
        <div class="stat-card">
          <div class="stat-val" id="statHoy">—</div>
          <div class="stat-lbl">Hoy</div>
        </div>
      </div>

      <button class="train-btn" onclick="openTrainModal()">
        ＋ Entrenar bot — agregar intención
      </button>
    </div>
  </div>

  <!-- CHAT -->
  <div class="chat-wrapper">
    <div class="chat-messages" id="chatMessages">
      <div class="empty-state">
        <div class="empty-icon">🤖</div>
        <div class="empty-title">MSTR Intelligence Bot</div>
        <div class="empty-sub">
          Haz una pregunta sobre tus datos de MicroStrategy.<br>
          Soporta análisis en lenguaje natural, predicciones ML y exportación.
        </div>
        <div class="suggestion-chips">
          <div class="chip" onclick="ask('¿Cuáles son las ventas totales del período?')">Ventas totales</div>
          <div class="chip" onclick="ask('Genera un dashboard ejecutivo')">Dashboard ejecutivo</div>
          <div class="chip" onclick="ask('Top 10 regiones por ingreso')">Top 10 regiones</div>
          <div class="chip" onclick="ask('Predice las ventas del próximo mes')">Predicción ventas</div>
          <div class="chip" onclick="ask('Analiza correlaciones entre variables')">Correlaciones</div>
          <div class="chip" onclick="ask('Peores 5 productos por venta')">Bottom 5 productos</div>
        </div>
      </div>
    </div>

    <div class="typing" id="typingIndicator">
      <div class="typing-dots">
        <span></span><span></span><span></span>
      </div>
      <span>Analizando datos...</span>
    </div>

    <div class="input-area">
      <div class="input-row">
        <div class="input-wrap">
          <textarea
            id="msgInput"
            rows="1"
            placeholder="Escribe tu consulta en lenguaje natural... ej: muéstrame el desglose de ventas por región"
          ></textarea>
        </div>
        <button class="send-btn" id="sendBtn" onclick="sendMessage()">
          Enviar ↵
        </button>
      </div>
      <div class="input-hint">Enter para enviar · Shift+Enter para nueva línea</div>
    </div>
  </div>
</div>

<!-- MODAL ENTRENAMIENTO -->
<div class="modal-overlay" id="trainModal">
  <div class="modal">
    <h3>🧠 Entrenar Bot — Nueva Intención</h3>
    <p>Agrega ejemplos de frases y su intención para que el bot las reconozca mejor.</p>
    <div class="form-group">
      <label class="form-label">Frase de ejemplo</label>
      <input class="form-input" id="trainTexto" placeholder="ej: muéstrame el resumen de ventas mensuales">
    </div>
    <div class="form-group">
      <label class="form-label">Intención</label>
      <select class="form-select" id="trainIntencion">
        <option value="total">total — sumas/totales</option>
        <option value="promedio">promedio — medias</option>
        <option value="ranking_top">ranking_top — top N</option>
        <option value="ranking_bottom">ranking_bottom — bottom N</option>
        <option value="desglose">desglose — por categoría</option>
        <option value="comparacion">comparacion — versus</option>
        <option value="prediccion">prediccion — forecast ML</option>
        <option value="correlacion">correlacion — relaciones</option>
        <option value="dashboard">dashboard — resumen ejecutivo</option>
        <option value="tendencia">tendencia — evolución temporal</option>
      </select>
    </div>
    <div class="form-group">
      <label class="form-label">Pista de respuesta (opcional)</label>
      <input class="form-input" id="trainHint" placeholder="ej: Mostrar suma de ventas agrupada por mes">
    </div>
    <div class="modal-actions">
      <button class="btn-cancel" onclick="closeTrainModal()">Cancelar</button>
      <button class="btn-save" onclick="guardarIntencion()">Guardar intención</button>
    </div>
  </div>
</div>

<script>
let msgCount = 0;

// ── INIT ──────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  checkStatus();
  loadSources();

  const input = document.getElementById('msgInput');
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  // Auto-resize textarea
  input.addEventListener('input', () => {
    input.style.height = 'auto';
    input.style.height = Math.min(input.scrollHeight, 120) + 'px';
  });
});

// ── STATUS ────────────────────────────────────────────────────
async function checkStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    const pill = document.getElementById('statusPill');
    const txt  = document.getElementById('statusText');

    if (d.connected) {
      pill.className = 'status-pill online';
      txt.textContent = 'Conectado';
      document.getElementById('srcCount').textContent = d.fuentes_disponibles || 0;
    } else {
      pill.className = 'status-pill offline';
      txt.textContent = 'Desconectado';
    }

    if (d.stats) {
      document.getElementById('statTotal').textContent = d.stats.total_conversaciones || 0;
      document.getElementById('statHoy').textContent   = d.stats.consultas_hoy || 0;
      document.getElementById('queryCount').textContent = d.stats.consultas_hoy || 0;
    }
  } catch(e) {}
}

// ── FUENTES ───────────────────────────────────────────────────
async function loadSources() {
  try {
    const r = await fetch('/api/sources');
    const d = await r.json();
    const list = document.getElementById('sourceList');

    if (!d.sources || d.sources.length === 0) {
      list.innerHTML = '<div style="font-size:12px;color:var(--danger);padding:8px 0;">Sin fuentes. Crea cubos en MSTR.</div>';
      return;
    }

    list.innerHTML = d.sources.map((s, i) => `
      <div class="source-item ${i===0?'active':''}" onclick="selectSource(${i}, this)">
        <span class="source-type type-${s.tipo}">${s.tipo}</span>
        <span class="source-name" title="${s.nombre}">${s.nombre}</span>
      </div>
    `).join('');
  } catch(e) {}
}

let selectedSourceIdx = 0;
function selectSource(idx, el) {
  selectedSourceIdx = idx;
  document.querySelectorAll('.source-item').forEach(x => x.classList.remove('active'));
  el.classList.add('active');
}

// ── SEND MESSAGE ──────────────────────────────────────────────
async function sendMessage() {
  const input = document.getElementById('msgInput');
  const text  = input.value.trim();
  if (!text) return;

  input.value = '';
  input.style.height = 'auto';

  // Remover empty state si existe
  const empty = document.querySelector('.empty-state');
  if (empty) empty.remove();

  addMsg('user', text);
  setInputEnabled(false);
  showTyping(true);

  try {
    const res = await fetch('/api/ask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: text, source_idx: selectedSourceIdx })
    });

    const data = await res.json();

    if (data.error) {
      addBotError(data.error, data.sugerencia);
    } else {
      addBotMsg(data);
      msgCount++;
      checkStatus();
    }
  } catch(e) {
    addBotError('Error de red: ' + e.message);
  } finally {
    showTyping(false);
    setInputEnabled(true);
    document.getElementById('msgInput').focus();
  }
}

function ask(text) {
  document.getElementById('msgInput').value = text;
  sendMessage();
}

// ── ADD MESSAGE ───────────────────────────────────────────────
function addMsg(type, text) {
  const area = document.getElementById('chatMessages');
  const div  = document.createElement('div');
  div.className = `msg ${type}`;
  div.innerHTML = `<div class="msg-bubble"><div class="msg-text">${escHtml(text)}</div></div>`;
  area.appendChild(div);
  area.scrollTop = area.scrollHeight;
}

function addBotMsg(data) {
  const area = document.getElementById('chatMessages');
  const div  = document.createElement('div');
  div.className = 'msg bot';

  const interpHtml = (data.interpretacion && data.interpretacion.length > 0)
    ? `<div class="msg-interp">
        <div class="interp-title">💡 Interpretación automática</div>
        ${data.interpretacion.map(i => `<div>${parseMarkdown(i)}</div>`).join('')}
       </div>`
    : '';

  const meta = `<div class="msg-meta">
    <span class="msg-source">${data.fuente_usada || 'MSTR'}</span>
    <span>${data.registros?.toLocaleString() || 0} registros</span>
    <span>${data.tiempo_ms || 0}ms</span>
    <span class="msg-time">${data.timestamp || ''}</span>
  </div>`;

  div.innerHTML = `
    <div class="msg-bubble">
      ${meta}
      <div class="msg-text">${parseMarkdown(data.respuesta)}</div>
      ${interpHtml}
      <div class="msg-actions">
        <button class="act-btn" onclick="exportarExcel()">📥 Exportar Excel</button>
        <button class="act-btn" onclick="ask('${escAttr(data.respuesta.substring(0,50))}... más detalle')">🔍 Profundizar</button>
      </div>
    </div>`;

  area.appendChild(div);
  area.scrollTop = area.scrollHeight;
}

function addBotError(err, sug) {
  const area = document.getElementById('chatMessages');
  const div  = document.createElement('div');
  div.className = 'msg bot';
  div.innerHTML = `
    <div class="msg-bubble">
      <div class="msg-text" style="color:var(--danger)">⚠️ ${escHtml(err)}</div>
      ${sug ? `<div style="font-size:12px;color:var(--muted);margin-top:8px;">💡 ${escHtml(sug)}</div>` : ''}
    </div>`;
  area.appendChild(div);
  area.scrollTop = area.scrollHeight;
}

// ── MARKDOWN SIMPLE ───────────────────────────────────────────
function parseMarkdown(text) {
  if (!text) return '';
  return text
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/`(.+?)`/g, '<code>$1</code>')
    .replace(/\n/g, '<br>');
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function escAttr(s) {
  return String(s).replace(/'/g,"\\'").replace(/"/g,'&quot;').replace(/\n/g,' ');
}

// ── EXPORT ────────────────────────────────────────────────────
async function exportarExcel() {
  try {
    const r = await fetch('/api/export/excel');
    if (r.ok) {
      const blob = await r.blob();
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href     = url;
      a.download = `mstr_export_${Date.now()}.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } else {
      alert('No hay datos para exportar. Haz una consulta primero.');
    }
  } catch(e) { alert('Error exportando: ' + e.message); }
}

// ── TRAINING MODAL ────────────────────────────────────────────
function openTrainModal()  { document.getElementById('trainModal').classList.add('show'); }
function closeTrainModal() { document.getElementById('trainModal').classList.remove('show'); }

async function guardarIntencion() {
  const texto    = document.getElementById('trainTexto').value.trim();
  const intencion = document.getElementById('trainIntencion').value;
  const hint     = document.getElementById('trainHint').value.trim();

  if (!texto) { alert('Ingresa una frase de ejemplo.'); return; }

  try {
    const r = await fetch('/api/train', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ texto, intencion, hint })
    });
    const d = await r.json();
    if (d.success) {
      alert('✅ Intención guardada. El bot la aprenderá en la próxima consulta.');
      closeTrainModal();
      document.getElementById('trainTexto').value = '';
      document.getElementById('trainHint').value  = '';
    } else {
      alert('Error guardando intención.');
    }
  } catch(e) { alert('Error: ' + e.message); }
}

// ── UI HELPERS ────────────────────────────────────────────────
function showTyping(show) {
  document.getElementById('typingIndicator').className = show ? 'typing show' : 'typing';
}

function setInputEnabled(enabled) {
  document.getElementById('msgInput').disabled = !enabled;
  document.getElementById('sendBtn').disabled  = !enabled;
}
</script>
</body>
</html>'''


# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    return render_template_string(HTML)


@app.route('/api/status')
def api_status():
    global bot
    try:
        if bot is None:
            return jsonify({'connected': False, 'error': 'Bot no inicializado'})

        stats = bot.obtener_estadisticas_bd()
        return jsonify({
            'connected': bot.is_authenticated,
            'fuentes_disponibles': len(bot.available_sources),
            'tipos': list(set(s['tipo'] for s in bot.available_sources)),
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'connected': False, 'error': str(e)})


@app.route('/api/sources')
def api_sources():
    global bot
    if bot is None:
        return jsonify({'sources': []})
    return jsonify({'sources': bot.obtener_fuentes()})


@app.route('/api/ask', methods=['POST'])
def api_ask():
    global bot
    try:
        data       = request.get_json()
        question   = data.get('question', '').strip()
        source_idx = data.get('source_idx', 0)

        if not question:
            return jsonify({'error': 'La pregunta no puede estar vacía'}), 400

        if bot is None:
            return jsonify({'error': 'Bot no inicializado'}), 500

        session_id = session.get('session_id', 'anon')
        result = bot.procesar_pregunta(question, session_id=session_id, source_idx=source_idx)

        if result.get('error'):
            return jsonify({
                'error':     result['error'],
                'sugerencia': result.get('sugerencia', '')
            }), 200

        return jsonify(result)

    except Exception as e:
        return jsonify({'error': f'Error procesando pregunta: {str(e)}'}), 500


@app.route('/api/export/excel')
def api_export_excel():
    global bot
    try:
        if bot is None or bot.last_query_result is None:
            return jsonify({'error': 'No hay datos para exportar'}), 400

        output = bot.exportar_excel()
        if output:
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f'mstr_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            )
        return jsonify({'error': 'Error generando Excel'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/train', methods=['POST'])
def api_train():
    global bot
    try:
        data    = request.get_json()
        texto   = data.get('texto', '').strip()
        intent  = data.get('intencion', '')
        hint    = data.get('hint', '')

        if not texto or not intent:
            return jsonify({'success': False, 'error': 'Faltan campos'}), 400

        if bot and bot.db.enabled:
            ok = bot.db.agregar_intencion(texto, intent, hint)
            if ok:
                bot.nlp._cargar_intenciones_bd()  # Refrescar NLP en caliente
            return jsonify({'success': ok})

        return jsonify({'success': False, 'error': 'Supabase no disponible'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/supabase-sql')
def api_supabase_sql():
    """Devuelve el SQL de setup para copiar en Supabase"""
    global bot
    if bot:
        sql = bot.db.get_sql_setup()
        return app.response_class(response=sql, mimetype='text/plain')
    return "Bot no disponible", 500


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})


# ── INICIALIZACIÓN AL ARRANCAR (gunicorn + python directo) ───────────────────
# Se ejecuta cuando gunicorn importa el módulo, ANTES de servir requests
inicializar_bot()

# ── MAIN (solo para desarrollo local) ────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV', 'production') == 'development'
    sep = '=' * 60
    print(f"\n{sep}")
    print(f"  Servidor en http://0.0.0.0:{port}")
    print(sep)
    app.run(debug=debug, host='0.0.0.0', port=port, use_reloader=False)
