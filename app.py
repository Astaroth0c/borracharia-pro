"""
╔══════════════════════════════════════════════════════════╗
║      🔩  BORRACHARIA PRO  — Versão Web  v1.0            ║
║   Flask + PostgreSQL + Render                            ║
║   Trial 1h → PIX → 30 dias de acesso                    ║
║   Módulo exclusivo: Produtor Rural + Empréstimo Pneus    ║
║   Criado por Robinho — Orange Tech Solutions             ║
╚══════════════════════════════════════════════════════════╝
"""
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from functools import wraps
import psycopg
import psycopg.rows
import os
import datetime
import hashlib
import secrets
import requests as http_req

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
DATABASE_URL    = os.environ.get("DATABASE_URL", "")
MP_ACCESS_TOKEN = os.environ.get("MP_ACCESS_TOKEN", "")
MP_PUBLIC_KEY   = os.environ.get("MP_PUBLIC_KEY", "")
ADMIN_TOKEN     = os.environ.get("ADMIN_TOKEN", "robinho_admin_2024")
PRECO_MENSAL    = 120.00
TRIAL_HORAS     = 1

# ─────────────────────────────────────────────────────────
# BANCO DE DADOS
# ─────────────────────────────────────────────────────────
def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=psycopg.rows.dict_row)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
            CREATE TABLE IF NOT EXISTS empresas (
                id               SERIAL PRIMARY KEY,
                nome             TEXT NOT NULL,
                usuario          TEXT UNIQUE NOT NULL,
                senha_hash       TEXT NOT NULL,
                plano            TEXT DEFAULT 'trial',
                sistema          TEXT DEFAULT 'borracharia',
                trial_inicio     TIMESTAMP DEFAULT NOW(),
                plano_valido_ate TIMESTAMP,
                criado_em        DATE DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS clientes (
                id         SERIAL PRIMARY KEY,
                empresa_id INT REFERENCES empresas(id) ON DELETE CASCADE,
                nome       TEXT NOT NULL,
                telefone   TEXT,
                whatsapp   TEXT,
                email      TEXT,
                cpfcnpj    TEXT,
                tipo       TEXT DEFAULT 'Pessoa Física',
                endereco   TEXT,
                cidade     TEXT,
                uf         TEXT,
                obs        TEXT,
                criado_em  DATE DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS veiculos (
                id         SERIAL PRIMARY KEY,
                empresa_id INT REFERENCES empresas(id) ON DELETE CASCADE,
                cliente_id INT REFERENCES clientes(id) ON DELETE SET NULL,
                placa      TEXT,
                modelo     TEXT,
                marca      TEXT,
                ano        TEXT,
                tipo       TEXT DEFAULT 'Carro',
                obs        TEXT
            );
            CREATE TABLE IF NOT EXISTS produtores_rurais (
                id              SERIAL PRIMARY KEY,
                empresa_id      INT REFERENCES empresas(id) ON DELETE CASCADE,
                cliente_id      INT REFERENCES clientes(id) ON DELETE CASCADE,
                nome_fazenda    TEXT,
                tipo_maquina    TEXT,
                medida_pneu     TEXT,
                marca_pneu      TEXT,
                forma_pagto     TEXT DEFAULT 'Dinheiro',
                obs             TEXT
            );
            CREATE TABLE IF NOT EXISTS estoque_pneus (
                id          SERIAL PRIMARY KEY,
                empresa_id  INT REFERENCES empresas(id) ON DELETE CASCADE,
                tipo        TEXT DEFAULT 'Novo',
                condicao    TEXT DEFAULT 'Novo',
                medida      TEXT NOT NULL,
                marca       TEXT,
                fornecedor  TEXT,
                qtd         NUMERIC DEFAULT 0,
                qtd_min     NUMERIC DEFAULT 1,
                custo       NUMERIC DEFAULT 0,
                preco_venda NUMERIC DEFAULT 0,
                obs         TEXT,
                atualizado  DATE DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS carcacas (
                id         SERIAL PRIMARY KEY,
                empresa_id INT REFERENCES empresas(id) ON DELETE CASCADE,
                medida     TEXT NOT NULL,
                marca      TEXT,
                status     TEXT DEFAULT 'disponivel',
                obs        TEXT,
                criado_em  DATE DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS os (
                id                SERIAL PRIMARY KEY,
                empresa_id        INT REFERENCES empresas(id) ON DELETE CASCADE,
                tipo_os           TEXT DEFAULT 'Serviço',
                data_abert        DATE DEFAULT CURRENT_DATE,
                cliente_id        INT REFERENCES clientes(id) ON DELETE SET NULL,
                cliente_nome      TEXT,
                veiculo_placa     TEXT,
                veiculo_modelo    TEXT,
                servicos          TEXT,
                pneu_medida       TEXT,
                pneu_marca        TEXT,
                pneu_condicao     TEXT,
                tipo_montagem     TEXT DEFAULT 'cortesia',
                valor_montagem    NUMERIC DEFAULT 0,
                total             NUMERIC DEFAULT 0,
                desconto          NUMERIC DEFAULT 0,
                forma_pagto       TEXT DEFAULT 'Dinheiro',
                pago              TEXT DEFAULT 'Sim',
                status            TEXT DEFAULT 'Aberta',
                carcaca_id        INT REFERENCES carcacas(id) ON DELETE SET NULL,
                recauchutadora    TEXT,
                data_envio        DATE,
                data_prev_retorno DATE,
                data_conclusao    DATE,
                tecnico           TEXT,
                obs               TEXT,
                criado_em         DATE DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS emprestimos (
                id              SERIAL PRIMARY KEY,
                empresa_id      INT REFERENCES empresas(id) ON DELETE CASCADE,
                os_id           INT REFERENCES os(id) ON DELETE CASCADE,
                carcaca_id      INT REFERENCES carcacas(id) ON DELETE SET NULL,
                cliente_id      INT REFERENCES clientes(id) ON DELETE SET NULL,
                cliente_nome    TEXT,
                medida          TEXT,
                data_saida      DATE DEFAULT CURRENT_DATE,
                data_devolucao  DATE,
                status          TEXT DEFAULT 'emprestado',
                obs             TEXT
            );
            CREATE TABLE IF NOT EXISTS agendamentos (
                id           SERIAL PRIMARY KEY,
                empresa_id   INT REFERENCES empresas(id) ON DELETE CASCADE,
                data_hora    TIMESTAMP NOT NULL,
                cliente_nome TEXT,
                telefone     TEXT,
                veiculo      TEXT,
                servico      TEXT,
                status       TEXT DEFAULT 'agendado',
                obs          TEXT,
                criado_em    DATE DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS pdv (
                id         SERIAL PRIMARY KEY,
                empresa_id INT REFERENCES empresas(id) ON DELETE CASCADE,
                data       DATE DEFAULT CURRENT_DATE,
                cliente_nome TEXT,
                itens      TEXT,
                total      NUMERIC DEFAULT 0,
                desconto   NUMERIC DEFAULT 0,
                forma_pagto TEXT DEFAULT 'Dinheiro',
                troco      NUMERIC DEFAULT 0,
                obs        TEXT,
                criado_em  TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS financeiro (
                id         SERIAL PRIMARY KEY,
                empresa_id INT REFERENCES empresas(id) ON DELETE CASCADE,
                data       DATE DEFAULT CURRENT_DATE,
                tipo       TEXT,
                categoria  TEXT,
                descricao  TEXT,
                valor      NUMERIC DEFAULT 0,
                forma_pagto TEXT,
                pago       TEXT DEFAULT 'Sim',
                obs        TEXT
            );
            CREATE TABLE IF NOT EXISTS proprietarios_carcacas (
                id                  SERIAL PRIMARY KEY,
                empresa_id          INT REFERENCES empresas(id) ON DELETE CASCADE,
                cliente_id          INT REFERENCES clientes(id) ON DELETE CASCADE,
                recauchutadora      TEXT,
                percentual_comissao NUMERIC DEFAULT 0,
                obs                 TEXT,
                criado_em           DATE DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS carcacas_proprietario (
                id              SERIAL PRIMARY KEY,
                empresa_id      INT REFERENCES empresas(id) ON DELETE CASCADE,
                proprietario_id INT REFERENCES proprietarios_carcacas(id) ON DELETE CASCADE,
                numero_serie    TEXT,
                medida          TEXT NOT NULL,
                marca           TEXT,
                aro             TEXT,
                categoria       TEXT DEFAULT 'Carro',
                valor_comercial NUMERIC DEFAULT 0,
                status          TEXT DEFAULT 'disponivel',
                obs             TEXT,
                criado_em       DATE DEFAULT CURRENT_DATE
            );
            CREATE TABLE IF NOT EXISTS comissoes_recauchutadora (
                id                  SERIAL PRIMARY KEY,
                empresa_id          INT REFERENCES empresas(id) ON DELETE CASCADE,
                proprietario_id     INT REFERENCES proprietarios_carcacas(id) ON DELETE CASCADE,
                carcaca_prop_id     INT REFERENCES carcacas_proprietario(id) ON DELETE SET NULL,
                recauchutadora      TEXT,
                data_envio          DATE DEFAULT CURRENT_DATE,
                data_prev_retorno   DATE,
                data_retorno        DATE,
                data_pagamento      DATE,
                valor_pneu          NUMERIC DEFAULT 0,
                comissao_percentual NUMERIC DEFAULT 0,
                comissao_valor      NUMERIC DEFAULT 0,
                pago                TEXT DEFAULT 'Nao',
                obs                 TEXT
            );
            CREATE TABLE IF NOT EXISTS permutas (
                id                     SERIAL PRIMARY KEY,
                empresa_id             INT REFERENCES empresas(id) ON DELETE CASCADE,
                cliente_id             INT REFERENCES clientes(id) ON DELETE SET NULL,
                cliente_nome           TEXT,
                data                   DATE DEFAULT CURRENT_DATE,
                servico_permutado      TEXT,
                valor_servico          NUMERIC DEFAULT 0,
                total_carcacas         INT DEFAULT 0,
                valor_total_carcacas   NUMERIC DEFAULT 0,
                saldo                  NUMERIC DEFAULT 0,
                obs                    TEXT,
                criado_em              TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS itens_permuta (
                id             SERIAL PRIMARY KEY,
                permuta_id     INT REFERENCES permutas(id) ON DELETE CASCADE,
                empresa_id     INT REFERENCES empresas(id) ON DELETE CASCADE,
                medida         TEXT NOT NULL,
                marca          TEXT,
                categoria      TEXT DEFAULT 'Carro',
                valor_unitario NUMERIC DEFAULT 0,
                qtd            INT DEFAULT 1,
                subtotal       NUMERIC DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS pagamentos (
                id            SERIAL PRIMARY KEY,
                empresa_id    INT REFERENCES empresas(id) ON DELETE CASCADE,
                mp_payment_id TEXT,
                valor         NUMERIC DEFAULT 0,
                status        TEXT DEFAULT 'pending',
                criado_em     TIMESTAMP DEFAULT NOW(),
                pago_em       TIMESTAMP
            );
            """)

def hash_senha(s): return hashlib.sha256(s.encode()).hexdigest()

def fmtR(v):
    try: return f"R$ {float(v):,.2f}".replace(",","X").replace(".",",").replace("X",".")
    except: return "R$ 0,00"

def pneu_status(qtd, qtd_min):
    try:
        q, m = float(qtd or 0), float(qtd_min or 1)
        if q <= 0: return "Sem Estoque"
        if q <= m: return "Crítico"
        if q <= m * 1.5: return "Baixo"
        return "OK"
    except: return ""

# ─────────────────────────────────────────────────────────
# CONTROLE DE PLANO / TRIAL
# ─────────────────────────────────────────────────────────
def get_empresa_status(empresa_id):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM empresas WHERE id=%s AND sistema='borracharia'", (empresa_id,))
            emp = c.fetchone()
    if not emp: return "expirado"
    agora = datetime.datetime.now()
    if emp["plano"] == "ativo" and emp["plano_valido_ate"]:
        return "ativo" if emp["plano_valido_ate"] > agora else "expirado"
    if emp["plano"] == "trial" and emp["trial_inicio"]:
        fim = emp["trial_inicio"] + datetime.timedelta(hours=TRIAL_HORAS)
        if agora < fim:
            return f"trial:{int((fim - agora).total_seconds() / 60)}"
    return "expirado"

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "empresa_id" not in session: return redirect(url_for("login"))
        if get_empresa_status(session["empresa_id"]) == "expirado":
            return redirect(url_for("assinar"))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get("token") or request.headers.get("X-Admin-Token")
        if token != ADMIN_TOKEN: return jsonify({"erro": "Acesso negado"}), 403
        return f(*args, **kwargs)
    return decorated

# ─────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────
@app.route("/", methods=["GET","POST"])
def login():
    if "empresa_id" in session: return redirect(url_for("painel"))
    erro = None
    if request.method == "POST":
        u = request.form.get("usuario","").strip()
        s = request.form.get("senha","").strip()
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("SELECT * FROM empresas WHERE usuario=%s AND senha_hash=%s AND sistema='borracharia'",(u,hash_senha(s)))
                emp = c.fetchone()
        if emp:
            session["empresa_id"] = emp["id"]
            session["empresa_nome"] = emp["nome"]
            if get_empresa_status(emp["id"]) == "expirado":
                return redirect(url_for("assinar"))
            return redirect(url_for("painel"))
        erro = "Usuário ou senha incorretos."
    return render_template("login.html", erro=erro)

@app.route("/logout")
def logout():
    session.clear(); return redirect(url_for("login"))

@app.route("/registrar", methods=["GET","POST"])
def registrar():
    erro = None
    if request.method == "POST":
        nome = request.form.get("nome","").strip()
        usuario = request.form.get("usuario","").strip()
        senha = request.form.get("senha","").strip()
        try:
            with get_conn() as conn:
                with conn.cursor() as c:
                    c.execute("INSERT INTO empresas (nome,usuario,senha_hash,plano,sistema,trial_inicio) VALUES (%s,%s,%s,'trial','borracharia',NOW())",
                              (nome, usuario, hash_senha(senha)))
            flash("Conta criada! Você tem 1 hora de acesso gratuito. 🔩","success")
            return redirect(url_for("login"))
        except psycopg.errors.UniqueViolation: erro = "Usuário já existe."
        except Exception as e: erro = f"Erro: {e}"
    return render_template("registrar.html", erro=erro)

# ─────────────────────────────────────────────────────────
# ASSINATURA / PIX
# ─────────────────────────────────────────────────────────
@app.route("/assinar")
def assinar():
    if "empresa_id" not in session: return redirect(url_for("login"))
    return render_template("assinar.html",
        empresa_nome=session.get("empresa_nome",""),
        preco=fmtR(PRECO_MENSAL), mp_public_key=MP_PUBLIC_KEY)

@app.route("/gerar_pix", methods=["POST"])
def gerar_pix():
    if "empresa_id" not in session: return jsonify({"erro":"Não autorizado"}), 401
    eid = session["empresa_id"]
    base_url = request.host_url.rstrip("/")
    payload = {
        "transaction_amount": PRECO_MENSAL,
        "description": f"Borracharia Pro — 30 dias — {session.get('empresa_nome','')}",
        "payment_method_id": "pix",
        "payer": {"email": f"borracharia_{eid}@orangetech.pro", "first_name": "Borracharia", "last_name": "Pro"},
        "notification_url": f"{base_url}/webhook/mp",
        "external_reference": str(eid),
        "date_of_expiration": (datetime.datetime.now() + datetime.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
    }
    try:
        resp = http_req.post("https://api.mercadopago.com/v1/payments", json=payload,
            headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}",
                     "X-Idempotency-Key": f"borr_{eid}_{int(datetime.datetime.now().timestamp())}",
                     "Content-Type": "application/json"}, timeout=15)
        data = resp.json()
        if resp.status_code in (200,201):
            with get_conn() as conn:
                with conn.cursor() as c:
                    c.execute("INSERT INTO pagamentos (empresa_id,mp_payment_id,valor,status) VALUES (%s,%s,%s,'pending')",
                              (eid, str(data.get("id")), PRECO_MENSAL))
            pix = data.get("point_of_interaction",{}).get("transaction_data",{})
            return jsonify({"qr_code": pix.get("qr_code",""), "qr_code_base64": pix.get("qr_code_base64",""), "payment_id": data.get("id")})
        return jsonify({"erro": data.get("message","Erro ao gerar PIX")}), 400
    except Exception as e: return jsonify({"erro": str(e)}), 500

@app.route("/verificar_pagamento/<int:pid>")
def verificar_pagamento(pid):
    if "empresa_id" not in session: return jsonify({"status":"erro"}), 401
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT status FROM pagamentos WHERE mp_payment_id=%s AND empresa_id=%s",(str(pid),session["empresa_id"]))
            p = c.fetchone()
    return jsonify({"status":"aprovado" if p and p["status"]=="approved" else "pendente"})

@app.route("/webhook/mp", methods=["POST"])
def webhook_mp():
    data = request.get_json(silent=True) or {}
    tipo = data.get("type","")
    payment_id = data.get("data",{}).get("id") if "payment" in tipo else None
    if not payment_id: return jsonify({"ok":True}), 200
    try:
        resp = http_req.get(f"https://api.mercadopago.com/v1/payments/{payment_id}",
            headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"}, timeout=10)
        pd = resp.json()
    except: return jsonify({"ok":True}), 200
    status = pd.get("status")
    eid_str = pd.get("external_reference")
    if not eid_str: return jsonify({"ok":True}), 200
    eid = int(eid_str)
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE pagamentos SET status=%s, pago_em=%s WHERE mp_payment_id=%s",
                      (status, datetime.datetime.now() if status=="approved" else None, str(payment_id)))
            if status == "approved":
                c.execute("UPDATE empresas SET plano='ativo', plano_valido_ate=NOW()+INTERVAL '30 days' WHERE id=%s",(eid,))
    return jsonify({"ok":True}), 200

# ─────────────────────────────────────────────────────────
# PAINEL
# ─────────────────────────────────────────────────────────
@app.route("/painel")
@login_required
def painel():
    eid = session["empresa_id"]
    status = get_empresa_status(eid)
    trial_min = int(status.split(":")[1]) if status.startswith("trial:") else None
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) as n FROM clientes WHERE empresa_id=%s",(eid,)); n_cli = c.fetchone()["n"]
            c.execute("SELECT COUNT(*) as n FROM os WHERE empresa_id=%s AND status NOT IN ('Concluída','Cancelada')",(eid,)); n_os = c.fetchone()["n"]
            c.execute("SELECT COUNT(*) as n FROM emprestimos WHERE empresa_id=%s AND status='emprestado'",(eid,)); n_emp = c.fetchone()["n"]
            c.execute("SELECT COUNT(*) as n FROM agendamentos WHERE empresa_id=%s AND data_hora::date=CURRENT_DATE AND status='agendado'",(eid,)); n_agenda = c.fetchone()["n"]
            c.execute("SELECT COALESCE(SUM(valor),0) as t FROM financeiro WHERE empresa_id=%s AND tipo='Entrada' AND data=CURRENT_DATE",(eid,)); hoje = c.fetchone()["t"]
            c.execute("SELECT COALESCE(SUM(valor),0) as t FROM financeiro WHERE empresa_id=%s AND tipo='Entrada' AND EXTRACT(MONTH FROM data)=EXTRACT(MONTH FROM CURRENT_DATE)",(eid,)); mes = c.fetchone()["t"]
            c.execute("SELECT * FROM os WHERE empresa_id=%s AND status NOT IN ('Concluída','Cancelada') ORDER BY id DESC LIMIT 8",(eid,)); ultimas_os = c.fetchall()
            c.execute("SELECT * FROM emprestimos WHERE empresa_id=%s AND status='emprestado' ORDER BY data_saida LIMIT 5",(eid,)); emprestimos = c.fetchall()
            c.execute("SELECT * FROM agendamentos WHERE empresa_id=%s AND data_hora >= NOW() ORDER BY data_hora LIMIT 5",(eid,)); proximos = c.fetchall()
    return render_template("painel.html", n_cli=n_cli, n_os=n_os, n_emp=n_emp, n_agenda=n_agenda,
        hoje=fmtR(hoje), mes=fmtR(mes), ultimas_os=ultimas_os, emprestimos=emprestimos,
        proximos=proximos, trial_min=trial_min, fmtR=fmtR)

# ─────────────────────────────────────────────────────────
# CLIENTES
# ─────────────────────────────────────────────────────────
@app.route("/clientes")
@login_required
def clientes():
    eid = session["empresa_id"]; q = request.args.get("q","")
    with get_conn() as conn:
        with conn.cursor() as c:
            if q: c.execute("SELECT * FROM clientes WHERE empresa_id=%s AND (LOWER(nome) LIKE %s OR telefone LIKE %s) ORDER BY nome",(eid,f"%{q.lower()}%",f"%{q}%"))
            else: c.execute("SELECT * FROM clientes WHERE empresa_id=%s ORDER BY nome",(eid,))
            rows = c.fetchall()
    return render_template("clientes.html", clientes=rows, q=q)

@app.route("/clientes/novo", methods=["GET","POST"])
@login_required
def cliente_novo():
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO clientes (empresa_id,nome,telefone,whatsapp,email,cpfcnpj,tipo,endereco,cidade,uf,obs) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (session["empresa_id"],f["nome"],f.get("telefone"),f.get("whatsapp"),f.get("email"),f.get("cpfcnpj"),f.get("tipo"),f.get("endereco"),f.get("cidade"),f.get("uf"),f.get("obs")))
        flash("Cliente cadastrado! ✔","success"); return redirect(url_for("clientes"))
    return render_template("form_cliente.html", cliente=None, titulo="Novo Cliente")

@app.route("/clientes/editar/<int:cid>", methods=["GET","POST"])
@login_required
def cliente_editar(cid):
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM clientes WHERE id=%s AND empresa_id=%s",(cid,eid)); cli = c.fetchone()
    if not cli: return redirect(url_for("clientes"))
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE clientes SET nome=%s,telefone=%s,whatsapp=%s,email=%s,cpfcnpj=%s,tipo=%s,endereco=%s,cidade=%s,uf=%s,obs=%s WHERE id=%s AND empresa_id=%s",
                    (f["nome"],f.get("telefone"),f.get("whatsapp"),f.get("email"),f.get("cpfcnpj"),f.get("tipo"),f.get("endereco"),f.get("cidade"),f.get("uf"),f.get("obs"),cid,eid))
        flash("Cliente atualizado! ✔","success"); return redirect(url_for("clientes"))
    return render_template("form_cliente.html", cliente=cli, titulo="Editar Cliente")

@app.route("/clientes/excluir/<int:cid>", methods=["POST"])
@login_required
def cliente_excluir(cid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM clientes WHERE id=%s AND empresa_id=%s",(cid,session["empresa_id"]))
    flash("Cliente excluído.","info"); return redirect(url_for("clientes"))

# ─────────────────────────────────────────────────────────
# PRODUTORES RURAIS
# ─────────────────────────────────────────────────────────
@app.route("/produtores")
@login_required
def produtores():
    eid = session["empresa_id"]; q = request.args.get("q","")
    with get_conn() as conn:
        with conn.cursor() as c:
            sql = """SELECT p.*, c.nome as cliente_nome, c.telefone,
                     (SELECT COUNT(*) FROM emprestimos e WHERE e.empresa_id=%s AND e.cliente_id=c.id AND e.status='emprestado') as emp_abertos
                     FROM produtores_rurais p JOIN clientes c ON c.id=p.cliente_id
                     WHERE p.empresa_id=%s"""
            params = [eid, eid]
            if q:
                sql += " AND (LOWER(c.nome) LIKE %s OR LOWER(p.nome_fazenda) LIKE %s)"
                params += [f"%{q.lower()}%", f"%{q.lower()}%"]
            sql += " ORDER BY c.nome"
            c.execute(sql, params); rows = c.fetchall()
    return render_template("produtores.html", produtores=rows, q=q)

@app.route("/produtores/novo", methods=["GET","POST"])
@login_required
def produtor_novo():
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id,nome FROM clientes WHERE empresa_id=%s ORDER BY nome",(eid,)); cli_list = c.fetchall()
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO produtores_rurais (empresa_id,cliente_id,nome_fazenda,tipo_maquina,medida_pneu,marca_pneu,forma_pagto,obs) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (eid,f["cliente_id"],f.get("nome_fazenda"),f.get("tipo_maquina"),f.get("medida_pneu"),f.get("marca_pneu"),f.get("forma_pagto"),f.get("obs")))
        flash("Produtor rural cadastrado! 🌾","success"); return redirect(url_for("produtores"))
    return render_template("form_produtor.html", cli_list=cli_list, produtor=None, titulo="Novo Produtor Rural")

@app.route("/produtores/editar/<int:pid>", methods=["GET","POST"])
@login_required
def produtor_editar(pid):
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM produtores_rurais WHERE id=%s AND empresa_id=%s",(pid,eid)); prod = c.fetchone()
            c.execute("SELECT id,nome FROM clientes WHERE empresa_id=%s ORDER BY nome",(eid,)); cli_list = c.fetchall()
    if not prod: return redirect(url_for("produtores"))
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE produtores_rurais SET cliente_id=%s,nome_fazenda=%s,tipo_maquina=%s,medida_pneu=%s,marca_pneu=%s,forma_pagto=%s,obs=%s WHERE id=%s AND empresa_id=%s",
                    (f["cliente_id"],f.get("nome_fazenda"),f.get("tipo_maquina"),f.get("medida_pneu"),f.get("marca_pneu"),f.get("forma_pagto"),f.get("obs"),pid,eid))
        flash("Produtor atualizado! ✔","success"); return redirect(url_for("produtores"))
    return render_template("form_produtor.html", cli_list=cli_list, produtor=prod, titulo="Editar Produtor Rural")

@app.route("/produtores/excluir/<int:pid>", methods=["POST"])
@login_required
def produtor_excluir(pid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM produtores_rurais WHERE id=%s AND empresa_id=%s",(pid,session["empresa_id"]))
    flash("Produtor excluído.","info"); return redirect(url_for("produtores"))

# ─────────────────────────────────────────────────────────
# ESTOQUE DE PNEUS E CARCAÇAS
# ─────────────────────────────────────────────────────────
@app.route("/estoque")
@login_required
def estoque():
    eid = session["empresa_id"]; q = request.args.get("q","")
    with get_conn() as conn:
        with conn.cursor() as c:
            if q: c.execute("SELECT * FROM estoque_pneus WHERE empresa_id=%s AND (LOWER(medida) LIKE %s OR LOWER(marca) LIKE %s) ORDER BY medida",(eid,f"%{q.lower()}%",f"%{q.lower()}%"))
            else: c.execute("SELECT * FROM estoque_pneus WHERE empresa_id=%s ORDER BY tipo,medida",(eid,))
            pneus = c.fetchall()
            c.execute("SELECT *, CASE WHEN status='disponivel' THEN '✅ Disponível' ELSE '🔄 Emprestada' END as status_label FROM carcacas WHERE empresa_id=%s ORDER BY medida",(eid,)); carcacas = c.fetchall()
    return render_template("estoque.html", pneus=pneus, carcacas=carcacas, q=q, pneu_status=pneu_status, fmtR=fmtR)

@app.route("/estoque/pneu/novo", methods=["GET","POST"])
@login_required
def pneu_novo():
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO estoque_pneus (empresa_id,tipo,condicao,medida,marca,fornecedor,qtd,qtd_min,custo,preco_venda,obs) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (session["empresa_id"],f.get("tipo"),f.get("condicao"),f["medida"],f.get("marca"),f.get("fornecedor"),
                     float(f.get("qtd") or 0),float(f.get("qtd_min") or 1),float(f.get("custo") or 0),float(f.get("preco_venda") or 0),f.get("obs")))
        flash("Pneu cadastrado! ✔","success"); return redirect(url_for("estoque"))
    return render_template("form_pneu.html", pneu=None, titulo="Novo Pneu")

@app.route("/estoque/pneu/editar/<int:pid>", methods=["GET","POST"])
@login_required
def pneu_editar(pid):
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM estoque_pneus WHERE id=%s AND empresa_id=%s",(pid,eid)); pneu = c.fetchone()
    if not pneu: return redirect(url_for("estoque"))
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE estoque_pneus SET tipo=%s,condicao=%s,medida=%s,marca=%s,fornecedor=%s,qtd=%s,qtd_min=%s,custo=%s,preco_venda=%s,obs=%s,atualizado=CURRENT_DATE WHERE id=%s AND empresa_id=%s",
                    (f.get("tipo"),f.get("condicao"),f["medida"],f.get("marca"),f.get("fornecedor"),
                     float(f.get("qtd") or 0),float(f.get("qtd_min") or 1),float(f.get("custo") or 0),float(f.get("preco_venda") or 0),f.get("obs"),pid,eid))
        flash("Pneu atualizado! ✔","success"); return redirect(url_for("estoque"))
    return render_template("form_pneu.html", pneu=pneu, titulo="Editar Pneu")

@app.route("/estoque/pneu/excluir/<int:pid>", methods=["POST"])
@login_required
def pneu_excluir(pid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM estoque_pneus WHERE id=%s AND empresa_id=%s",(pid,session["empresa_id"]))
    flash("Pneu excluído.","info"); return redirect(url_for("estoque"))

@app.route("/estoque/carcaca/nova", methods=["GET","POST"])
@login_required
def carcaca_nova():
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO carcacas (empresa_id,medida,marca,status,obs) VALUES (%s,%s,%s,'disponivel',%s)",
                    (session["empresa_id"],f["medida"],f.get("marca"),f.get("obs")))
        flash("Carcaça cadastrada! ✔","success"); return redirect(url_for("estoque"))
    return render_template("form_carcaca.html", carcaca=None, titulo="Nova Carcaça")

@app.route("/estoque/carcaca/excluir/<int:cid>", methods=["POST"])
@login_required
def carcaca_excluir(cid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM carcacas WHERE id=%s AND empresa_id=%s",(cid,session["empresa_id"]))
    flash("Carcaça excluída.","info"); return redirect(url_for("estoque"))

# ─────────────────────────────────────────────────────────
# ORDENS DE SERVIÇO
# ─────────────────────────────────────────────────────────
OS_STATUS = ["Aberta","Carcaça Emprestada","Em Recauchutagem","Pneu Pronto","Retorno Agendado","Concluída","Cancelada"]

@app.route("/os")
@login_required
def ordens():
    eid = session["empresa_id"]; q = request.args.get("q",""); status = request.args.get("status","")
    with get_conn() as conn:
        with conn.cursor() as c:
            sql = "SELECT * FROM os WHERE empresa_id=%s"; params = [eid]
            if status: sql += " AND status=%s"; params.append(status)
            if q: sql += " AND (LOWER(cliente_nome) LIKE %s OR LOWER(veiculo_placa) LIKE %s)"; params += [f"%{q.lower()}%",f"%{q.lower()}%"]
            sql += " ORDER BY id DESC"; c.execute(sql,params); rows = c.fetchall()
    return render_template("os.html", ordens=rows, q=q, status=status, fmtR=fmtR, os_status=OS_STATUS)

@app.route("/os/nova", methods=["GET","POST"])
@login_required
def os_nova():
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id,nome FROM clientes WHERE empresa_id=%s ORDER BY nome",(eid,)); cli_list = c.fetchall()
            c.execute("SELECT * FROM carcacas WHERE empresa_id=%s AND status='disponivel' ORDER BY medida",(eid,)); carcacas_disp = c.fetchall()
    if request.method == "POST":
        f = request.form
        tipo_montagem = f.get("tipo_montagem","cortesia")
        valor_montagem = float(f.get("valor_montagem") or 0) if tipo_montagem != "cortesia" else 0
        total = float(f.get("total") or 0)
        carcaca_id = f.get("carcaca_id") or None

        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO os (empresa_id,tipo_os,data_abert,cliente_id,cliente_nome,veiculo_placa,
                        veiculo_modelo,servicos,pneu_medida,pneu_marca,pneu_condicao,tipo_montagem,
                        valor_montagem,total,desconto,forma_pagto,pago,status,carcaca_id,
                        recauchutadora,data_prev_retorno,tecnico,obs)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                """, (eid, f.get("tipo_os","Serviço"),
                      f.get("data_abert") or datetime.date.today(),
                      f.get("cliente_id") or None, f["cliente_nome"], f.get("veiculo_placa"),
                      f.get("veiculo_modelo"), f.get("servicos"), f.get("pneu_medida"),
                      f.get("pneu_marca"), f.get("pneu_condicao"), tipo_montagem,
                      valor_montagem, total, float(f.get("desconto") or 0),
                      f.get("forma_pagto","Dinheiro"), f.get("pago","Sim"),
                      f.get("status","Aberta"), carcaca_id,
                      f.get("recauchutadora"), f.get("data_prev_retorno") or None,
                      f.get("tecnico"), f.get("obs")))
                os_id = c.fetchone()["id"]

                # Se carcaça emprestada, registra empréstimo e muda status da carcaça
                if carcaca_id and f.get("status") in ("Carcaça Emprestada",):
                    c.execute("SELECT medida FROM carcacas WHERE id=%s",(carcaca_id,)); car = c.fetchone()
                    c.execute("UPDATE carcacas SET status='emprestada' WHERE id=%s",(carcaca_id,))
                    c.execute("""INSERT INTO emprestimos (empresa_id,os_id,carcaca_id,cliente_id,cliente_nome,medida,data_saida,status)
                                 VALUES (%s,%s,%s,%s,%s,%s,CURRENT_DATE,'emprestado')""",
                              (eid, os_id, carcaca_id, f.get("cliente_id") or None, f["cliente_nome"],
                               car["medida"] if car else f.get("pneu_medida")))

                # Registra no financeiro se pago
                if f.get("pago") == "Sim" and total > 0:
                    c.execute("INSERT INTO financeiro (empresa_id,data,tipo,categoria,descricao,valor,forma_pagto,pago) VALUES (%s,CURRENT_DATE,'Entrada','Serviço',%s,%s,%s,'Sim')",
                              (eid, f"OS #{os_id} — {f['cliente_nome']}", total, f.get("forma_pagto","Dinheiro")))

        flash("OS criada com sucesso! 🔩","success"); return redirect(url_for("ordens"))
    return render_template("form_os.html", os=None, titulo="Nova OS", cli_list=cli_list,
        carcacas_disp=carcacas_disp, os_status=OS_STATUS, hoje=datetime.date.today())

@app.route("/os/editar/<int:oid>", methods=["GET","POST"])
@login_required
def os_editar(oid):
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM os WHERE id=%s AND empresa_id=%s",(oid,eid)); ordem = c.fetchone()
            c.execute("SELECT id,nome FROM clientes WHERE empresa_id=%s ORDER BY nome",(eid,)); cli_list = c.fetchall()
            c.execute("SELECT * FROM carcacas WHERE empresa_id=%s AND (status='disponivel' OR id=%s) ORDER BY medida",(eid, ordem["carcaca_id"] or 0)); carcacas_disp = c.fetchall()
    if not ordem: return redirect(url_for("ordens"))
    if request.method == "POST":
        f = request.form
        status_ant = ordem["status"]
        status_novo = f.get("status", ordem["status"])
        tipo_montagem = f.get("tipo_montagem","cortesia")
        valor_montagem = float(f.get("valor_montagem") or 0) if tipo_montagem != "cortesia" else 0
        carcaca_id = f.get("carcaca_id") or None

        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("""UPDATE os SET tipo_os=%s,data_abert=%s,cliente_id=%s,cliente_nome=%s,
                    veiculo_placa=%s,veiculo_modelo=%s,servicos=%s,pneu_medida=%s,pneu_marca=%s,
                    pneu_condicao=%s,tipo_montagem=%s,valor_montagem=%s,total=%s,desconto=%s,
                    forma_pagto=%s,pago=%s,status=%s,carcaca_id=%s,recauchutadora=%s,
                    data_envio=%s,data_prev_retorno=%s,data_conclusao=%s,tecnico=%s,obs=%s
                    WHERE id=%s AND empresa_id=%s""",
                    (f.get("tipo_os"), f.get("data_abert") or None,
                     f.get("cliente_id") or None, f["cliente_nome"],
                     f.get("veiculo_placa"), f.get("veiculo_modelo"), f.get("servicos"),
                     f.get("pneu_medida"), f.get("pneu_marca"), f.get("pneu_condicao"),
                     tipo_montagem, valor_montagem,
                     float(f.get("total") or 0), float(f.get("desconto") or 0),
                     f.get("forma_pagto"), f.get("pago"), status_novo, carcaca_id,
                     f.get("recauchutadora"), f.get("data_envio") or None,
                     f.get("data_prev_retorno") or None,
                     datetime.date.today() if status_novo == "Concluída" else None,
                     f.get("tecnico"), f.get("obs"), oid, eid))

                # Carcaça emprestada → muda status
                if status_novo == "Carcaça Emprestada" and status_ant != "Carcaça Emprestada" and carcaca_id:
                    c.execute("SELECT medida FROM carcacas WHERE id=%s",(carcaca_id,)); car = c.fetchone()
                    c.execute("UPDATE carcacas SET status='emprestada' WHERE id=%s",(carcaca_id,))
                    c.execute("SELECT id FROM emprestimos WHERE os_id=%s",(oid,)); emp_exist = c.fetchone()
                    if not emp_exist:
                        c.execute("INSERT INTO emprestimos (empresa_id,os_id,carcaca_id,cliente_id,cliente_nome,medida,data_saida,status) VALUES (%s,%s,%s,%s,%s,%s,CURRENT_DATE,'emprestado')",
                                  (eid,oid,carcaca_id,f.get("cliente_id") or None,f["cliente_nome"],car["medida"] if car else ""))

                # OS concluída → devolve carcaça
                if status_novo == "Concluída" and ordem["carcaca_id"]:
                    c.execute("UPDATE carcacas SET status='disponivel' WHERE id=%s",(ordem["carcaca_id"],))
                    c.execute("UPDATE emprestimos SET status='devolvido', data_devolucao=CURRENT_DATE WHERE os_id=%s",(oid,))

        flash("OS atualizada! ✔","success"); return redirect(url_for("ordens"))
    return render_template("form_os.html", os=ordem, titulo=f"Editar OS #{oid}",
        cli_list=cli_list, carcacas_disp=carcacas_disp, os_status=OS_STATUS,
        hoje=datetime.date.today())

@app.route("/os/excluir/<int:oid>", methods=["POST"])
@login_required
def os_excluir(oid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT carcaca_id FROM os WHERE id=%s AND empresa_id=%s",(oid,session["empresa_id"])); o = c.fetchone()
            if o and o["carcaca_id"]:
                c.execute("UPDATE carcacas SET status='disponivel' WHERE id=%s",(o["carcaca_id"],))
            c.execute("DELETE FROM os WHERE id=%s AND empresa_id=%s",(oid,session["empresa_id"]))
    flash("OS excluída.","info"); return redirect(url_for("ordens"))

@app.route("/os/imprimir/<int:oid>")
@login_required
def os_imprimir(oid):
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM os WHERE id=%s AND empresa_id=%s",(oid,eid)); ordem = c.fetchone()
            c.execute("SELECT nome FROM empresas WHERE id=%s",(eid,)); emp = c.fetchone()
    if not ordem: return redirect(url_for("ordens"))
    return render_template("imprimir_os.html", os=ordem, empresa=emp, fmtR=fmtR)

@app.route("/os/avancar/<int:oid>", methods=["POST"])
@login_required
def os_avancar(oid):
    """Avança o status da OS para a próxima etapa."""
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT status FROM os WHERE id=%s AND empresa_id=%s",(oid,session["empresa_id"])); o = c.fetchone()
    if o and o["status"] in OS_STATUS:
        idx = OS_STATUS.index(o["status"])
        if idx < len(OS_STATUS) - 2:
            novo = OS_STATUS[idx + 1]
            with get_conn() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE os SET status=%s WHERE id=%s AND empresa_id=%s",(novo,oid,session["empresa_id"]))
                    if novo == "Concluída":
                        c.execute("SELECT carcaca_id FROM os WHERE id=%s",(oid,)); o2 = c.fetchone()
                        if o2 and o2["carcaca_id"]:
                            c.execute("UPDATE carcacas SET status='disponivel' WHERE id=%s",(o2["carcaca_id"],))
                            c.execute("UPDATE emprestimos SET status='devolvido',data_devolucao=CURRENT_DATE WHERE os_id=%s",(oid,))
            flash(f"OS avançada para: {novo} ✔","success")
    return redirect(url_for("ordens"))

# ─────────────────────────────────────────────────────────
# EMPRÉSTIMOS
# ─────────────────────────────────────────────────────────
@app.route("/emprestimos")
@login_required
def emprestimos():
    eid = session["empresa_id"]; filtro = request.args.get("filtro","abertos")
    with get_conn() as conn:
        with conn.cursor() as c:
            if filtro == "todos": c.execute("SELECT * FROM emprestimos WHERE empresa_id=%s ORDER BY data_saida DESC",(eid,))
            else: c.execute("SELECT * FROM emprestimos WHERE empresa_id=%s AND status='emprestado' ORDER BY data_saida",(eid,))
            rows = c.fetchall()
    return render_template("emprestimos.html", emprestimos=rows, filtro=filtro)

# ─────────────────────────────────────────────────────────
# AGENDAMENTOS
# ─────────────────────────────────────────────────────────
@app.route("/agenda")
@login_required
def agenda():
    eid = session["empresa_id"]
    data_str = request.args.get("data", datetime.date.today().strftime("%Y-%m-%d"))
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM agendamentos WHERE empresa_id=%s AND data_hora::date=%s ORDER BY data_hora",(eid, data_str)); rows = c.fetchall()
            c.execute("SELECT * FROM agendamentos WHERE empresa_id=%s AND data_hora >= NOW() AND status='agendado' ORDER BY data_hora LIMIT 10",(eid,)); proximos = c.fetchall()
    return render_template("agenda.html", agendamentos=rows, proximos=proximos, data_str=data_str)

@app.route("/agenda/novo", methods=["GET","POST"])
@login_required
def agenda_novo():
    if request.method == "POST":
        f = request.form
        data_hora = f.get("data","") + " " + f.get("hora","08:00")
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO agendamentos (empresa_id,data_hora,cliente_nome,telefone,veiculo,servico,status,obs) VALUES (%s,%s,%s,%s,%s,%s,'agendado',%s)",
                    (session["empresa_id"],data_hora,f["cliente_nome"],f.get("telefone"),f.get("veiculo"),f.get("servico"),f.get("obs")))
        flash("Agendamento criado! 📅","success"); return redirect(url_for("agenda"))
    return render_template("form_agenda.html", ag=None, titulo="Novo Agendamento", hoje=datetime.date.today())

@app.route("/agenda/excluir/<int:aid>", methods=["POST"])
@login_required
def agenda_excluir(aid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM agendamentos WHERE id=%s AND empresa_id=%s",(aid,session["empresa_id"]))
    flash("Agendamento excluído.","info"); return redirect(url_for("agenda"))

@app.route("/agenda/concluir/<int:aid>", methods=["POST"])
@login_required
def agenda_concluir(aid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE agendamentos SET status='concluído' WHERE id=%s AND empresa_id=%s",(aid,session["empresa_id"]))
    flash("Agendamento concluído! ✔","success"); return redirect(url_for("agenda"))

# ─────────────────────────────────────────────────────────
# PDV — PONTO DE VENDA
# ─────────────────────────────────────────────────────────
@app.route("/pdv")
@login_required
def pdv():
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM estoque_pneus WHERE empresa_id=%s AND qtd > 0 ORDER BY tipo,medida",(eid,)); pneus = c.fetchall()
            c.execute("SELECT * FROM pdv WHERE empresa_id=%s ORDER BY criado_em DESC LIMIT 20",(eid,)); vendas = c.fetchall()
    return render_template("pdv.html", pneus=pneus, vendas=vendas, fmtR=fmtR, hoje=datetime.date.today())

@app.route("/pdv/vender", methods=["POST"])
@login_required
def pdv_vender():
    eid = session["empresa_id"]
    f = request.form
    itens = f.get("itens","")
    total = float(f.get("total") or 0)
    desconto = float(f.get("desconto") or 0)
    pago = total - desconto
    recebido = float(f.get("recebido") or pago)
    troco = max(0, recebido - pago)
    forma = f.get("forma_pagto","Dinheiro")

    # Baixa estoque dos itens vendidos
    import json
    try:
        itens_list = json.loads(itens)
        with get_conn() as conn:
            with conn.cursor() as c:
                for item in itens_list:
                    if item.get("pneu_id"):
                        c.execute("UPDATE estoque_pneus SET qtd=qtd-%s,atualizado=CURRENT_DATE WHERE id=%s AND empresa_id=%s",
                                  (item["qtd"], item["pneu_id"], eid))
                c.execute("INSERT INTO pdv (empresa_id,data,cliente_nome,itens,total,desconto,forma_pagto,troco) VALUES (%s,CURRENT_DATE,%s,%s,%s,%s,%s,%s)",
                    (eid, f.get("cliente_nome",""), itens, pago, desconto, forma, troco))
                c.execute("INSERT INTO financeiro (empresa_id,data,tipo,categoria,descricao,valor,forma_pagto,pago) VALUES (%s,CURRENT_DATE,'Entrada','Venda PDV',%s,%s,%s,'Sim')",
                    (eid, f"Venda PDV — {f.get('cliente_nome','Balcão')}", pago, forma))
    except Exception as e:
        flash(f"Erro na venda: {e}","danger"); return redirect(url_for("pdv"))

    flash(f"Venda registrada! Troco: {fmtR(troco)} ✔","success")
    return redirect(url_for("pdv"))

# ─────────────────────────────────────────────────────────
# FINANCEIRO
# ─────────────────────────────────────────────────────────
@app.route("/financeiro")
@login_required
def financeiro():
    eid = session["empresa_id"]; q = request.args.get("q",""); tipo = request.args.get("tipo","")
    with get_conn() as conn:
        with conn.cursor() as c:
            sql = "SELECT * FROM financeiro WHERE empresa_id=%s"; params = [eid]
            if tipo: sql += " AND tipo=%s"; params.append(tipo)
            if q: sql += " AND LOWER(descricao) LIKE %s"; params.append(f"%{q.lower()}%")
            sql += " ORDER BY data DESC, id DESC"; c.execute(sql,params); rows = c.fetchall()
            c.execute("SELECT COALESCE(SUM(valor),0) as t FROM financeiro WHERE empresa_id=%s AND tipo='Entrada'",(eid,)); ent = c.fetchone()["t"]
            c.execute("SELECT COALESCE(SUM(valor),0) as t FROM financeiro WHERE empresa_id=%s AND tipo='Saída'",(eid,)); sai = c.fetchone()["t"]
    return render_template("financeiro.html", lancamentos=rows, q=q, tipo=tipo,
        ent=fmtR(ent), sai=fmtR(sai), saldo=fmtR(float(ent)-float(sai)))

@app.route("/financeiro/novo", methods=["GET","POST"])
@login_required
def financeiro_novo():
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO financeiro (empresa_id,data,tipo,categoria,descricao,valor,forma_pagto,pago,obs) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (session["empresa_id"],f.get("data") or None,f["tipo"],f.get("categoria"),f["descricao"],float(f.get("valor") or 0),f.get("forma_pagto"),f.get("pago"),f.get("obs")))
        flash("Lançamento registrado! ✔","success"); return redirect(url_for("financeiro"))
    return render_template("form_financeiro.html", lanc=None, titulo="Novo Lançamento")

@app.route("/financeiro/excluir/<int:fid>", methods=["POST"])
@login_required
def financeiro_excluir(fid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM financeiro WHERE id=%s AND empresa_id=%s",(fid,session["empresa_id"]))
    flash("Lançamento excluído.","info"); return redirect(url_for("financeiro"))

# ─────────────────────────────────────────────────────────
# ADMIN
# ─────────────────────────────────────────────────────────
@app.route("/admin")
@admin_required
def admin():
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""SELECT e.*,
                (SELECT COUNT(*) FROM clientes WHERE empresa_id=e.id) as total_clientes,
                (SELECT COUNT(*) FROM os WHERE empresa_id=e.id) as total_os,
                (SELECT COALESCE(SUM(valor),0) FROM pagamentos WHERE empresa_id=e.id AND status='approved') as receita
                FROM empresas e WHERE e.sistema='borracharia' ORDER BY e.criado_em DESC"""); empresas = c.fetchall()
            c.execute("SELECT COALESCE(SUM(valor),0) as t FROM pagamentos WHERE status='approved'"); receita_total = c.fetchone()["t"]
    return render_template("admin.html", empresas=empresas, receita_total=fmtR(receita_total),
        agora=datetime.datetime.now(), fmtR=fmtR, get_empresa_status=get_empresa_status,
        token=request.args.get("token"))

@app.route("/admin/liberar/<int:eid>", methods=["POST"])
@admin_required
def admin_liberar(eid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE empresas SET plano='ativo',plano_valido_ate=NOW()+INTERVAL '30 days' WHERE id=%s",(eid,))
    flash(f"Empresa #{eid} liberada por 30 dias!","success")
    return redirect(url_for("admin", token=request.args.get("token")))

@app.route("/admin/bloquear/<int:eid>", methods=["POST"])
@admin_required
def admin_bloquear(eid):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE empresas SET plano='expirado',plano_valido_ate=NOW() WHERE id=%s",(eid,))
    flash(f"Empresa #{eid} bloqueada!","info")
    return redirect(url_for("admin", token=request.args.get("token")))

# ─────────────────────────────────────────────────────────
# PROPRIETÁRIOS DE CARCAÇAS + COMISSÃO RECAUCHUTADORA
# ─────────────────────────────────────────────────────────
@app.route("/proprietarios")
@login_required
def proprietarios():
    eid = session["empresa_id"]
    q = request.args.get("q","")
    with get_conn() as conn:
        with conn.cursor() as c:
            sql = """SELECT p.*, c.nome as cliente_nome, c.telefone,
                     (SELECT COUNT(*) FROM carcacas_proprietario cp WHERE cp.proprietario_id=p.id AND cp.status='disponivel') as carcacas_disponiveis,
                     (SELECT COUNT(*) FROM carcacas_proprietario cp WHERE cp.proprietario_id=p.id AND cp.status='em_recauchutagem') as em_recauchutagem,
                     (SELECT COALESCE(SUM(comissao_valor),0) FROM comissoes_recauchutadora cr WHERE cr.proprietario_id=p.id AND cr.pago='Não') as comissao_pendente
                     FROM proprietarios_carcacas p JOIN clientes c ON c.id=p.cliente_id
                     WHERE p.empresa_id=%s"""
            params = [eid]
            if q:
                sql += " AND (LOWER(c.nome) LIKE %s OR LOWER(p.recauchutadora) LIKE %s)"
                params += [f"%{q.lower()}%", f"%{q.lower()}%"]
            sql += " ORDER BY c.nome"
            c.execute(sql, params)
            rows = c.fetchall()
    return render_template("proprietarios.html", proprietarios=rows, q=q, fmtR=fmtR)

@app.route("/proprietarios/novo", methods=["GET","POST"])
@login_required
def proprietario_novo():
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id,nome FROM clientes WHERE empresa_id=%s ORDER BY nome",(eid,))
            cli_list = c.fetchall()
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("""INSERT INTO proprietarios_carcacas
                    (empresa_id,cliente_id,recauchutadora,percentual_comissao,obs)
                    VALUES (%s,%s,%s,%s,%s)""",
                    (eid, f["cliente_id"], f.get("recauchutadora"),
                     float(f.get("percentual_comissao") or 0), f.get("obs")))
        flash("Proprietário cadastrado! ✔","success")
        return redirect(url_for("proprietarios"))
    return render_template("form_proprietario.html", cli_list=cli_list, prop=None, titulo="Novo Proprietário de Carcaças")

@app.route("/proprietarios/editar/<int:pid>", methods=["GET","POST"])
@login_required
def proprietario_editar(pid):
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM proprietarios_carcacas WHERE id=%s AND empresa_id=%s",(pid,eid))
            prop = c.fetchone()
            c.execute("SELECT id,nome FROM clientes WHERE empresa_id=%s ORDER BY nome",(eid,))
            cli_list = c.fetchall()
            c.execute("""SELECT cp.*, 
                         (SELECT COUNT(*) FROM comissoes_recauchutadora cr WHERE cr.carcaca_prop_id=cp.id) as total_envios
                         FROM carcacas_proprietario cp WHERE cp.proprietario_id=%s ORDER BY cp.numero_serie""",(pid,))
            carcacas = c.fetchall()
            c.execute("SELECT * FROM comissoes_recauchutadora WHERE proprietario_id=%s ORDER BY data_envio DESC",(pid,))
            comissoes = c.fetchall()
    if not prop: return redirect(url_for("proprietarios"))
    if request.method == "POST":
        f = request.form
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE proprietarios_carcacas SET cliente_id=%s,recauchutadora=%s,percentual_comissao=%s,obs=%s WHERE id=%s AND empresa_id=%s",
                    (f["cliente_id"],f.get("recauchutadora"),float(f.get("percentual_comissao") or 0),f.get("obs"),pid,eid))
        flash("Proprietário atualizado! ✔","success")
        return redirect(url_for("proprietarios"))
    return render_template("form_proprietario.html", cli_list=cli_list, prop=prop,
        carcacas=carcacas, comissoes=comissoes, titulo="Editar Proprietário", fmtR=fmtR,
        hoje=datetime.date.today())

@app.route("/proprietarios/carcaca/nova/<int:prop_id>", methods=["POST"])
@login_required
def carcaca_prop_nova(prop_id):
    eid = session["empresa_id"]
    f = request.form
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""INSERT INTO carcacas_proprietario
                (empresa_id,proprietario_id,numero_serie,medida,marca,aro,categoria,valor_comercial,status,obs)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'disponivel',%s)""",
                (eid, prop_id, f.get("numero_serie"), f["medida"], f.get("marca"),
                 f.get("aro"), f.get("categoria","Carro"),
                 float(f.get("valor_comercial") or 0), f.get("obs")))
    flash("Carcaça do proprietário cadastrada! ✔","success")
    return redirect(url_for("proprietario_editar", pid=prop_id))

@app.route("/proprietarios/enviar/<int:prop_id>/<int:carc_id>", methods=["POST"])
@login_required
def enviar_recauchutagem(prop_id, carc_id):
    eid = session["empresa_id"]
    f = request.form
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM carcacas_proprietario WHERE id=%s",(carc_id,))
            carc = c.fetchone()
            c.execute("SELECT * FROM proprietarios_carcacas WHERE id=%s",(prop_id,))
            prop = c.fetchone()
            if carc and prop:
                valor_pneu = float(f.get("valor_pneu") or 0)
                comissao = valor_pneu * (float(prop["percentual_comissao"] or 0) / 100)
                c.execute("""INSERT INTO comissoes_recauchutadora
                    (empresa_id,proprietario_id,carcaca_prop_id,recauchutadora,
                     data_envio,data_prev_retorno,valor_pneu,comissao_percentual,comissao_valor,pago,obs)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'Não',%s)""",
                    (eid, prop_id, carc_id, prop["recauchutadora"],
                     f.get("data_envio") or datetime.date.today(),
                     f.get("data_prev_retorno") or None,
                     valor_pneu, prop["percentual_comissao"], comissao, f.get("obs")))
                c.execute("UPDATE carcacas_proprietario SET status='em_recauchutagem' WHERE id=%s",(carc_id,))
    flash("Pneu enviado para recauchutagem! ✔","success")
    return redirect(url_for("proprietario_editar", pid=prop_id))

@app.route("/proprietarios/retornou/<int:com_id>", methods=["POST"])
@login_required
def pneu_retornou(com_id):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM comissoes_recauchutadora WHERE id=%s",(com_id,))
            com = c.fetchone()
            if com:
                c.execute("UPDATE carcacas_proprietario SET status='disponivel' WHERE id=%s",(com["carcaca_prop_id"],))
                c.execute("UPDATE comissoes_recauchutadora SET data_retorno=CURRENT_DATE WHERE id=%s",(com_id,))
    flash("Pneu retornou da recauchutagem! ✔","success")
    return redirect(url_for("proprietario_editar", pid=com["proprietario_id"] if com else 0))

@app.route("/proprietarios/pagar_comissao/<int:com_id>", methods=["POST"])
@login_required
def pagar_comissao(com_id):
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM comissoes_recauchutadora WHERE id=%s",(com_id,))
            com = c.fetchone()
            if com:
                c.execute("UPDATE comissoes_recauchutadora SET pago='Sim', data_pagamento=CURRENT_DATE WHERE id=%s",(com_id,))
                c.execute("""INSERT INTO financeiro (empresa_id,data,tipo,categoria,descricao,valor,forma_pagto,pago)
                             VALUES (%s,CURRENT_DATE,'Saída','Comissão',%s,%s,'Dinheiro','Sim')""",
                          (eid, f"Comissão recauchutagem — {com['recauchutadora']}", com["comissao_valor"]))
    flash("Comissão paga e registrada no financeiro! ✔","success")
    return redirect(url_for("proprietario_editar", pid=com["proprietario_id"] if com else 0))

# ─────────────────────────────────────────────────────────
# PERMUTA DE CARCAÇAS
# ─────────────────────────────────────────────────────────
@app.route("/permutas")
@login_required
def permutas():
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""SELECT p.*, 
                         (SELECT COUNT(*) FROM itens_permuta ip WHERE ip.permuta_id=p.id) as total_carcacas
                         FROM permutas p WHERE p.empresa_id=%s ORDER BY p.data DESC""", (eid,))
            rows = c.fetchall()
    return render_template("permutas.html", permutas=rows, fmtR=fmtR)

@app.route("/permutas/nova", methods=["GET","POST"])
@login_required
def permuta_nova():
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id,nome FROM clientes WHERE empresa_id=%s ORDER BY nome",(eid,))
            cli_list = c.fetchall()
    if request.method == "POST":
        f = request.form
        medidas = request.form.getlist("medida[]")
        marcas = request.form.getlist("marca[]")
        categorias = request.form.getlist("categoria[]")
        valores = request.form.getlist("valor_unitario[]")
        qtds = request.form.getlist("qtd[]")

        total_carcacas = sum(int(q or 1) for q in qtds)
        valor_total = sum(float(v or 0) * int(q or 1) for v,q in zip(valores,qtds))

        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("""INSERT INTO permutas
                    (empresa_id,cliente_id,cliente_nome,data,servico_permutado,
                     valor_servico,total_carcacas,valor_total_carcacas,saldo,obs)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                    (eid, f.get("cliente_id") or None, f["cliente_nome"],
                     f.get("data") or datetime.date.today(),
                     f.get("servico_permutado"), float(f.get("valor_servico") or 0),
                     total_carcacas, valor_total,
                     float(f.get("valor_servico") or 0) - valor_total,
                     f.get("obs")))
                perm_id = c.fetchone()["id"]

                for med,mar,cat,val,qtd in zip(medidas,marcas,categorias,valores,qtds):
                    if med:
                        q = int(qtd or 1)
                        v = float(val or 0)
                        c.execute("""INSERT INTO itens_permuta
                            (permuta_id,empresa_id,medida,marca,categoria,valor_unitario,qtd,subtotal)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (perm_id,eid,med,mar,cat,v,q,v*q))
                        # Adiciona as carcaças recebidas ao estoque
                        for _ in range(q):
                            c.execute("""INSERT INTO carcacas (empresa_id,medida,marca,status,obs)
                                         VALUES (%s,%s,%s,'disponivel','Recebida por permuta')""",
                                      (eid,med,mar))

                # Registra no financeiro se há diferença a pagar
                saldo = float(f.get("valor_servico") or 0) - valor_total
                if saldo > 0:
                    c.execute("""INSERT INTO financeiro (empresa_id,data,tipo,categoria,descricao,valor,forma_pagto,pago)
                                 VALUES (%s,CURRENT_DATE,'Entrada','Permuta',%s,%s,'Permuta','Sim')""",
                              (eid, f"Permuta — {f['cliente_nome']} — saldo a receber", saldo))
                elif saldo < 0:
                    c.execute("""INSERT INTO financeiro (empresa_id,data,tipo,categoria,descricao,valor,forma_pagto,pago)
                                 VALUES (%s,CURRENT_DATE,'Saída','Permuta',%s,%s,'Permuta','Sim')""",
                              (eid, f"Permuta — {f['cliente_nome']} — saldo a pagar", abs(saldo)))

        flash("Permuta registrada! Carcaças adicionadas ao estoque! ✔","success")
        return redirect(url_for("permutas"))
    return render_template("form_permuta.html", cli_list=cli_list, hoje=datetime.date.today())

@app.route("/permutas/ver/<int:pid>")
@login_required
def permuta_ver(pid):
    eid = session["empresa_id"]
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM permutas WHERE id=%s AND empresa_id=%s",(pid,eid))
            perm = c.fetchone()
            c.execute("SELECT * FROM itens_permuta WHERE permuta_id=%s",(pid,))
            itens = c.fetchall()
    if not perm: return redirect(url_for("permutas"))
    return render_template("ver_permuta.html", perm=perm, itens=itens, fmtR=fmtR)

# ─────────────────────────────────────────────────────────
# INICIALIZAÇÃO
# ─────────────────────────────────────────────────────────
try:
    init_db()
    print("✅ Borracharia Pro — banco inicializado!")
except Exception as e:
    print(f"⚠️  init_db: {e}")

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))