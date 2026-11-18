import zmq
import msgpack
import json
import os
import re
from time import time

NOME = os.environ.get("NOME", "servidor")
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

os.makedirs(PASTA, exist_ok=True)

# carrega os canais que ja estavam salvos
canais = []
if os.path.exists(ARQ_CANAIS):
    with open(ARQ_CANAIS) as f:
        canais = json.load(f)


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
    canal = msg.get("canal")
    texto = msg.get("mensagem")
    if not nome_valido(canal):
        return {"tipo": "publicar", "status": "erro", "erro": "nome de canal invalido"}
    if not isinstance(texto, str) or texto == "":
        return {"tipo": "publicar", "status": "erro", "erro": "mensagem vazia"}
    # nao da pra conferir se o canal existe porque ele pode ter sido
    # criado em outro servidor (o broker distribui as requisicoes)

    publicacao = {
        "tipo": "publicacao",
        "canal": canal,
        "usuario": msg.get("usuario"),
        "mensagem": texto,
        "servidor": NOME,
        "timestamp": time(),
    }
    # o topico e o nome do canal
    pub.send_multipart([canal.encode(), msgpack.packb(publicacao)])
    print(f"[{NOME}] PUBLICADO em {canal}: {publicacao}", flush=True)
    salvar_mensagem(publicacao)
    return {"tipo": "publicar", "status": "ok"}


print(f"[{NOME}] iniciado, {len(canais)} canais carregados", flush=True)

while True:
    msg = msgpack.unpackb(socket.recv())
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

    resposta["servidor"] = NOME
    resposta["timestamp"] = time()
    socket.send(msgpack.packb(resposta))
    print(f"[{NOME}] ENVIADO:  {resposta}", flush=True)
