# app.py (seu arquivo principal do backend Flask)
from flask import Flask, request, jsonify, g
from flask_cors import CORS
import uuid
import logging
import os
from datetime import datetime, timedelta, timezone
import urllib.parse
import json
import sqlite3

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # logging.FileHandler("backend_log.txt", encoding="utf-8"), # Descomente para logar em arquivo (efêmero no Render)
        logging.StreamHandler() # Envia logs para o console (visível nos logs do Render)
    ]
)

app = Flask(__name__)
CORS(app) # Habilita CORS para todas as rotas (necessário para o seu frontend)

# Nome do arquivo do banco de dados SQLite
# ATENÇÃO: No Render Free Tier, este arquivo será efêmero (dados perdidos em restarts/deploys).
# Para persistência real, use um banco de dados externo como PostgreSQL.
DATABASE = 'backend_sessions.db'

# Lógica otimizada de gerenciamento de conexão com o banco de dados.
# A conexão é aberta por requisição e fechada automaticamente.
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        # Usar parse_decltypes para converter TIMESTAMP do SQLite para objetos datetime do Python
        db = g._database = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        db.row_factory = sqlite3.Row # Retorna linhas como objetos de dicionário
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db_backend():
    # Esta função usa uma conexão direta pois roda fora do contexto da aplicação.
    with sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                full_url TEXT,
                browser_data TEXT,
                server_data TEXT,
                tracking_data TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_heartbeats (
                bot_id TEXT PRIMARY KEY,
                last_heartbeat TIMESTAMP -- SQLite TIMESTAMP é armazenado como string ISO 8601 por padrão
            )
        ''')
        conn.commit()
    logging.info("Backend: Banco de dados SQLite inicializado com sucesso.")

# Garante que o banco de dados seja inicializado na primeira execução
with app.app_context():
    init_db_backend()

# --- Rotas de Sessão (mantidas do seu código original) ---
@app.route('/api/create-session', methods=['POST'])
def create_session():
    payload = request.get_json()
    if not payload or 'fullUrl' not in payload or 'browserData' not in payload:
        logging.warning("create_session: Requisição POST inválida. Payload incompleto.")
        return jsonify({'error': 'Payload incompleto. "fullUrl" e "browserData" são necessários.'}), 400

    session_id = str(uuid.uuid4())

    forwarded_for = request.headers.get('X-Forwarded-For')
    ip_address = forwarded_for.split(',')[0].strip() if forwarded_for else request.remote_addr

    full_url = payload['fullUrl']
    browser_data = payload.get('browserData', {})
    parsed_url = urllib.parse.urlparse(full_url)
    query_params = urllib.parse.parse_qs(parsed_url.query)

    utm_params = {
        'utm_source': query_params.get('utm_source', [None])[0],
        'utm_medium': query_params.get('utm_medium', [None])[0],
        'utm_campaign': query_params.get('utm_campaign', [None])[0],
        'utm_content': query_params.get('utm_content', [None])[0],
        'utm_term': query_params.get('utm_term', [None])[0],
        'fbclid': query_params.get('fbclid', [None])[0],
        'gclid': query_params.get('gclid', [None])[0],
        'ttclid': query_params.get('ttclid', [None])[0],
    }

    cookie_params = {
        'fbp': browser_data.get('fbpCookie'),
        'fbc': query_params.get('fbclid', [None])[0] or browser_data.get('fbcCookie'),
    }

    tracking_data_extracted = {**utm_params, **cookie_params}
    tracking_data_for_log = {k: v for k, v in tracking_data_extracted.items() if v is not None}

    session_data = {
        'fullUrl': full_url,
        'browserData': browser_data,
        'serverData': {
            'ipAddress': ip_address,
            'timestamp': datetime.now(timezone.utc).isoformat() # Usando timezone UTC para padronização.
        },
        'trackingData': tracking_data_extracted
    }

    db = get_db()
    try:
        cursor = db.cursor()
        cursor.execute(
            "INSERT INTO sessions (session_id, full_url, browser_data, server_data, tracking_data) VALUES (?, ?, ?, ?, ?)",
            (session_id,
             session_data['fullUrl'],
             json.dumps(session_data['browserData']),
             json.dumps(session_data['serverData']),
             json.dumps(session_data['trackingData']))
        )
        db.commit()
        logging.info(f"create_session: Sessão criada e salva em DB: {session_id} com IP: {ip_address}. UTMs/Cookies: {json.dumps(tracking_data_for_log)}")
        logging.debug(f"Detalhes completos da sessão {session_id} salvos: {json.dumps(session_data, indent=2)}")

        return jsonify({'session_id': session_id})
    except sqlite3.IntegrityError as e:
        logging.error(f"create_session: Erro de integridade ao salvar sessão {session_id}: {e}")
        return jsonify({'error': 'Erro ao criar sessão: ID duplicado'}), 500
    except Exception as e:
        logging.exception(f"create_session: Erro inesperado ao salvar sessão {session_id}.")
        return jsonify({'error': 'Erro interno do servidor'}), 500


@app.route('/api/get-session/<session_id>', methods=['GET'])
def get_session(session_id):
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT full_url, browser_data, server_data, tracking_data FROM sessions WHERE session_id = ?", (session_id,))
        row = cursor.fetchone()

        if not row:
            logging.warning(f"get_session: Sessão não encontrada para session_id: {session_id} no DB.")
            return jsonify({'error': 'Sessão não encontrada'}), 404

        full_url, browser_data_json, server_data_json, tracking_data_json = row

        response_data = {
            'fullUrl': full_url,
            'browserData': json.loads(browser_data_json),
            'serverData': json.loads(server_data_json),
            'trackingData': json.loads(tracking_data_json)
        }

        logging.info(f"get_session: Dados recuperados do DB para session_id: {session_id}")
        return jsonify(response_data)
    except Exception as e:
        logging.exception(f"get_session: Erro inesperado ao recuperar sessão {session_id}.")
        return jsonify({'error': 'Erro interno do servidor'}), 500

# --- Rotas de Heartbeat do Bot (corrigidas) ---
@app.route('/api/bot-heartbeat', methods=['POST'])
def bot_heartbeat():
    try:
        data = request.get_json()
        bot_id = data.get('bot_id')
        if not bot_id:
            logging.warning("Heartbeat recebido sem bot_id.")
            return jsonify({'error': 'bot_id é necessário'}), 400

        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO bot_heartbeats (bot_id, last_heartbeat) VALUES (?, ?)",
            (bot_id, datetime.now(timezone.utc)) # Armazena o timestamp em UTC
        )
        db.commit()
        logging.info(f"Backend: Heartbeat recebido para bot_id: {bot_id}")
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logging.exception(f"Backend: Erro ao receber heartbeat para bot_id: {bot_id}.")
        return jsonify({'error': 'Erro interno do servidor'}), 500

@app.route('/api/bot-status/<bot_id>', methods=['GET'])
def get_bot_status(bot_id):
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT last_heartbeat FROM bot_heartbeats WHERE bot_id = ?", (bot_id,))
        row = cursor.fetchone()

        if not row:
            logging.warning(f"Backend: Status do bot não encontrado para bot_id: {bot_id}.")
            return jsonify({'active': False, 'message': 'Bot não registrado ou inativo.'}), 200

        last_heartbeat = row[0] # last_heartbeat já é um objeto datetime graças a detect_types
        
        # --- CORREÇÃO APLICADA AQUI ---
        # O bot envia heartbeat a cada 5 minutos (300 segundos).
        # Consideramos ativo se o último heartbeat foi recebido nos últimos 6 minutos (360 segundos).
        active_threshold_seconds = 360 # Ajuste conforme a frequência de envio do seu bot + margem
        is_active = (datetime.now(timezone.utc) - last_heartbeat) < timedelta(seconds=active_threshold_seconds)

        logging.info(f"Backend: Status consultado para bot_id: {bot_id}. Ativo: {is_active} (Último heartbeat: {last_heartbeat.isoformat()})")
        return jsonify({'active': is_active, 'last_heartbeat': last_heartbeat.isoformat()}), 200
    except Exception as e:
        logging.exception(f"Backend: Erro ao consultar status do bot para bot_id: {bot_id}.")
        return jsonify({'error': 'Erro interno do servidor'}), 500

@app.route('/')
def index():
    return 'Backend de Rastreamento Avançado está no ar!'

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
