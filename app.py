from flask import Flask, request, jsonify
from flask_cors import CORS
import uuid
import logging
import os
from datetime import datetime, timedelta # NOVO: Importado timedelta
import urllib.parse
import json
import sqlite3
# --- NOVAS IMPORTAÇÕES ---
# Removidas importações asyncio, aiohttp, hashlib, dotenv pois a CAPI foi movida para o bot
# e não são mais necessárias aqui para evitar o RuntimeError.

# --- NOVO: Carregar variáveis de ambiente ---
# load_dotenv() # Removido, pois as variáveis do Facebook não são mais usadas aqui

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("backend_log.txt", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)
CORS(app)

# --- Configuração do Banco de Dados SQLite para o Backend ---
DATABASE = 'backend_sessions.db' # MUDADO: Nome do DB para consistência

def init_db_backend():
    conn = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            full_url TEXT,
            browser_data TEXT, -- Armazenar como JSON string
            server_data TEXT,  -- Armazenar como JSON string
            tracking_data TEXT -- Armazenar como JSON string
        )
    ''')
    # NOVO: Tabela para heartbeats do bot
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_heartbeats (
            bot_id TEXT PRIMARY KEY,
            last_heartbeat TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logging.info("Backend: Banco de dados SQLite inicializado com sucesso.")

# Chamar a inicialização do DB ao iniciar o Flask app
with app.app_context():
    init_db_backend()

# --- NOVO: LÓGICA E FUNÇÕES DA API DE CONVERSÕES ---
# REMOVIDO: Toda a lógica e funções da CAPI do Facebook foram removidas daqui,
# pois a responsabilidade de enviar eventos CAPI foi movida para o bot do Telegram.
# Isso evita o RuntimeError e a duplicação de eventos.

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
        'fbclid': query_params.get('fbclid', [None])[0],  # Captura o fbclid da URL
        'gclid': query_params.get('gclid', [None])[0],
        'ttclid': query_params.get('ttclid', [None])[0],
    }

    cookie_params = {
        'fbp': browser_data.get('fbpCookie'),
        # Agora, fazemos o fbc ser igual ao fbclid, se ele existir.
        # Se não houver fbclid, tenta pegar do cookie, caso contrário None.
        'fbc': query_params.get('fbclid', [None])[0] or browser_data.get('fbcCookie'), 
    }

    # Combina todos os dados de rastreamento
    tracking_data_extracted = {**utm_params, **cookie_params}
    # Remove valores None para não poluir o log, mas mantém a chave para o bot
    tracking_data_for_log = {k: v for k, v in tracking_data_extracted.items() if v is not None}
    
    # Monta o objeto de sessão completo
    session_data = {
        'fullUrl': full_url,
        'browserData': browser_data, 
        'serverData': {
            'ipAddress': ip_address,
            'timestamp': datetime.now().isoformat()
        },
        'trackingData': tracking_data_extracted # Armazena os parâmetros extraídos aqui (incluindo Nones)
    }

    # --- NOVO: Armazenar no SQLite ---
    # CORREÇÃO DE INDENTAÇÃO: Este bloco agora está alinhado corretamente
    conn = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO sessions (session_id, full_url, browser_data, server_data, tracking_data) VALUES (?, ?, ?, ?, ?)",
            (session_id, 
             session_data['fullUrl'], 
             json.dumps(session_data['browserData']), 
             json.dumps(session_data['serverData']), 
             json.dumps(session_data['trackingData']))
        )
        conn.commit()
        # MUDADO: Log de criação de sessão com UTMs/Cookies
        logging.info(f"create_session: Sessão criada e salva em DB: {session_id} com IP: {ip_address}. UTMs/Cookies: {json.dumps(tracking_data_for_log)}")
        logging.debug(f"Detalhes completos da sessão {session_id} salvos: {json.dumps(session_data, indent=2)}")

        # REMOVIDO: Disparo do evento PageView foi movido para o bot do Telegram

        return jsonify({'session_id': session_id})
    except sqlite3.IntegrityError as e:
        logging.error(f"create_session: Erro de integridade ao salvar sessão {session_id}: {e}")
        return jsonify({'error': 'Erro ao criar sessão: ID duplicado'}), 500
    except Exception as e:
        logging.exception(f"create_session: Erro inesperado ao salvar sessão {session_id}.")
        return jsonify({'error': 'Erro interno do servidor'}), 500
    finally:
        conn.close()
    # --- FIM NOVO ---

@app.route('/api/get-session/<session_id>', methods=['GET'])
def get_session(session_id):
    """
    Endpoint para o bot recuperar os dados de rastreamento enriquecidos.
    Agora recupera do SQLite.
    """
    conn = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT full_url, browser_data, server_data, tracking_data FROM sessions WHERE session_id = ?", (session_id,))
        row = cursor.fetchone()
        if not row:
            logging.warning(f"get_session: Sessão não encontrada para session_id: {session_id} no DB.")
            return jsonify({'error': 'Sessão não encontrada'}), 404
        
        full_url, browser_data_json, server_data_json, tracking_data_json = row
        
        # Converte de volta para dicionários Python
        browser_data = json.loads(browser_data_json)
        server_data = json.loads(server_data_json)
        tracking_data = json.loads(tracking_data_json)

        # Monta o objeto de resposta como era antes
        response_data = {
            'fullUrl': full_url,
            'browserData': browser_data,
            'serverData': server_data,
            'trackingData': tracking_data # Retorna o trackingData já extraído
        }
        
        logging.info(f"get_session: Dados recuperados do DB para session_id: {session_id}")
        return jsonify(response_data)
    except Exception as e:
        logging.exception(f"get_session: Erro inesperado ao recuperar sessão {session_id}.")
        return jsonify({'error': 'Erro interno do servidor'}), 500
    finally:
        conn.close()

# NOVO: Endpoint para o bot enviar o heartbeat
@app.route('/api/bot-heartbeat', methods=['POST'])
def bot_heartbeat():
    try:
        data = request.get_json()
        bot_id = data.get('bot_id')
        if not bot_id:
            return jsonify({'error': 'bot_id é necessário'}), 400

        conn = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cursor = conn.cursor()
        
        # Atualiza ou insere o último heartbeat
        cursor.execute(
            "INSERT OR REPLACE INTO bot_heartbeats (bot_id, last_heartbeat) VALUES (?, ?)",
            (bot_id, datetime.now())
        )
        conn.commit()
        conn.close()
        logging.info(f"Backend: Heartbeat recebido para bot_id: {bot_id}")
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logging.exception(f"Backend: Erro ao receber heartbeat para bot_id: {bot_id}.")
        return jsonify({'error': 'Erro interno do servidor'}), 500

# NOVO: Endpoint para o site consultar o status do bot
@app.route('/api/bot-status/<bot_id>', methods=['GET'])
def get_bot_status(bot_id):
    try:
        conn = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cursor = conn.cursor()
        
        cursor.execute("SELECT last_heartbeat FROM bot_heartbeats WHERE bot_id = ?", (bot_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            logging.warning(f"Backend: Status do bot não encontrado para bot_id: {bot_id}.")
            return jsonify({'active': False, 'message': 'Bot não registrado ou inativo.'}), 200
        
        last_heartbeat = row[0]
        # Considere o bot ativo se o último heartbeat foi nos últimos 60 segundos
        is_active = (datetime.now() - last_heartbeat) < timedelta(seconds=60)
        
        logging.info(f"Backend: Status consultado para bot_id: {bot_id}. Ativo: {is_active}")
        return jsonify({'active': is_active, 'last_heartbeat': last_heartbeat.isoformat()}), 200
    except Exception as e:
        logging.exception(f"Backend: Erro ao consultar status do bot para bot_id: {bot_id}.")
        return jsonify({'error': 'Erro interno do servidor'}), 500

@app.route('/')
def index():
    """Endpoint de saúde."""
    return 'Backend de Rastreamento Avançado está no ar!'

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
