import zmq
import msgpack
import json
import os
import re
from socket import gethostname, gethostbyname
from time import time

NOME = os.environ.get("NOME", "servidor")
ENDERECO = gethostbyname(gethostname())  # ip desse container na rede do docker
PASTA = "dados"
ARQ_LOGINS = PASTA + "/logins.txt"
ARQ_CANAIS = PASTA + "/canais.json"
ARQ_MENSAGENS = PASTA + "/mensagens.txt"

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

# socket para publicar nos canais (passa pelo proxy)
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")

# socket para falar com o servico de referencia
ref = context.socket(zmq.REQ)
ref.connect("tcp://referencia:5559")

relogio = 0        # relogio logico
ajuste = 0         # correcao do relogio fisico
qtd_mensagens = 0  # mensagens de clientes recebidas

os.makedirs(PASTA, exist_ok=True)

# carrega os canais que ja estavam salvos
canais = []
if os.path.exists(ARQ_CANAIS):
    with open(ARQ_CANAIS) as f:
        canais = json.load(f)


def agora():
    # hora do container corrigida pela referencia
    return time() + ajuste


def salvar_login(usuario, timestamp):
    with open(ARQ_LOGINS, "a") as f:
        f.write(json.dumps({"usuario": usuario, "timestamp": timestamp}) + "\n")


def salvar_canais():
    with open(ARQ_CANAIS, "w") as f:
        json.dump(canais, f)


def salvar_mensagem(publicacao):
    with open(ARQ_MENSAGENS, "a") as f:
        f.write(json.dumps(publicacao) + "\n")


def nome_valido(nome):
    # so letra minuscula, numero, _ e -  (max 20)
    if not isinstance(nome, str):
        return False
    return re.match("^[a-z0-9_-]{1,20}$", nome) is not None


def pedir_referencia(msg):
    global relogio
    relogio += 1
    msg["relogio"] = relogio
    msg["timestamp"] = agora()
    print(f"[{NOME}] ENVIADO para referencia:  {msg}", flush=True)
    ref.send(msgpack.packb(msg))
    resposta = msgpack.unpackb(ref.recv())
    relogio = max(relogio, resposta["relogio"])
    print(f"[{NOME}] RECEBIDO da referencia: {resposta}", flush=True)
    return resposta


def heartbeat():
    global ajuste
    resposta = pedir_referencia({"tipo": "heartbeat", "nome": NOME, "endereco": ENDERECO})
    # aproveita a resposta para acertar o relogio
    ajuste = resposta["hora"] - time()
    print(f"[{NOME}] relogio ajustado em {ajuste:.6f}s", flush=True)
    pedir_referencia({"tipo": "lista"})


def login(msg):
    usuario = msg.get("usuario")
    if not nome_valido(usuario):
        return {"tipo": "login", "status": "erro", "erro": "nome de usuario invalido"}
    salvar_login(usuario, msg["timestamp"])
    return {"tipo": "login", "status": "ok"}


def criar_canal(msg):
    canal = msg.get("canal")
    if not nome_valido(canal):
        return {"tipo": "canal", "status": "erro", "erro": "nome de canal invalido"}
    if canal in canais:
        return {"tipo": "canal", "status": "erro", "erro": "canal ja existe"}
    canais.append(canal)
    salvar_canais()
    return {"tipo": "canal", "status": "ok"}


def listar_canais(msg):
    return {"tipo": "listar", "status": "ok", "canais": canais}


def publicar(msg):
    global relogio
    canal = msg.get("canal")
    texto = msg.get("mensagem")
    if not nome_valido(canal):
        return {"tipo": "publicar", "status": "erro", "erro": "nome de canal invalido"}
    if not isinstance(texto, str) or texto == "":
        return {"tipo": "publicar", "status": "erro", "erro": "mensagem vazia"}
    # nao da pra conferir se o canal existe porque ele pode ter sido
    # criado em outro servidor (o broker distribui as requisicoes)

    relogio += 1
    publicacao = {
        "tipo": "publicacao",
        "canal": canal,
        "usuario": msg.get("usuario"),
        "mensagem": texto,
        "servidor": NOME,
        "timestamp": agora(),
        "relogio": relogio,
    }
    # o topico e o nome do canal
    pub.send_multipart([canal.encode(), msgpack.packb(publicacao)])
    print(f"[{NOME}] PUBLICADO em {canal}: {publicacao}", flush=True)
    salvar_mensagem(publicacao)
    return {"tipo": "publicar", "status": "ok"}


rank = pedir_referencia({"tipo": "rank", "nome": NOME, "endereco": ENDERECO})["rank"]
print(f"[{NOME}] iniciado com rank {rank}, {len(canais)} canais carregados", flush=True)

while True:
    msg = msgpack.unpackb(socket.recv())
    relogio = max(relogio, msg["relogio"])
    print(f"[{NOME}] RECEBIDO: {msg}", flush=True)

    tipo = msg.get("tipo")
    if tipo == "login":
        resposta = login(msg)
    elif tipo == "canal":
        resposta = criar_canal(msg)
    elif tipo == "listar":
        resposta = listar_canais(msg)
    elif tipo == "publicar":
        resposta = publicar(msg)
    else:
        resposta = {"tipo": tipo, "status": "erro", "erro": "tipo de mensagem desconhecido"}

    relogio += 1
    resposta["servidor"] = NOME
    resposta["timestamp"] = agora()
    resposta["relogio"] = relogio
    socket.send(msgpack.packb(resposta))
    print(f"[{NOME}] ENVIADO:  {resposta}", flush=True)

    # a cada 10 mensagens de clientes avisa a referencia que esta vivo
    qtd_mensagens += 1
    if qtd_mensagens % 10 == 0:
        heartbeat()
