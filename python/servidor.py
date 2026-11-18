import zmq
import msgpack
import json
import os
import re
import threading
from socket import gethostname, gethostbyname
from time import time, sleep

NOME = os.environ.get("NOME", "servidor")
ENDERECO = gethostbyname(gethostname())  # ip desse container na rede do docker
PASTA = "dados"
ARQ_LOGINS = PASTA + "/logins.txt"
ARQ_CANAIS = PASTA + "/canais.json"
ARQ_MENSAGENS = PASTA + "/mensagens.txt"
PORTA_SERVIDORES = 5560  # porta onde um servidor atende os outros

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

# socket para publicar nos canais (passa pelo proxy)
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")

# socket para ouvir quem e o coordenador
sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
sub.setsockopt_string(zmq.SUBSCRIBE, "servers")

# socket para falar com o servico de referencia
ref = context.socket(zmq.REQ)
ref.connect("tcp://referencia:5559")

relogio = 0        # relogio logico
ajuste = 0         # correcao do relogio fisico
qtd_mensagens = 0  # mensagens de clientes recebidas
rank = 0

coordenador = None      # nome do servidor eleito
rank_coordenador = 0
endereco_coordenador = None
precisa_eleicao = True  # comeca sem coordenador, entao ja pede eleicao
diferencas = {}         # usado so pelo coordenador (berkeley): nome -> diferenca de relogio

# o relogio logico e usado pela thread que atende os servidores tambem
trava = threading.Lock()

os.makedirs(PASTA, exist_ok=True)

# carrega os canais que ja estavam salvos
canais = []
if os.path.exists(ARQ_CANAIS):
    with open(ARQ_CANAIS) as f:
        canais = json.load(f)


def agora():
    # hora do container corrigida pelo coordenador
    return time() + ajuste


def incrementar_relogio():
    global relogio
    with trava:
        relogio += 1
        return relogio


def atualizar_relogio(recebido):
    global relogio
    with trava:
        relogio = max(relogio, recebido)


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
    msg["relogio"] = incrementar_relogio()
    msg["timestamp"] = agora()
    print(f"[{NOME}] ENVIADO para referencia:  {msg}", flush=True)
    ref.send(msgpack.packb(msg))
    resposta = msgpack.unpackb(ref.recv())
    atualizar_relogio(resposta["relogio"])
    print(f"[{NOME}] RECEBIDO da referencia: {resposta}", flush=True)
    return resposta


def pedir_servidor(destino, endereco, msg):
    # requisicao direta para outro servidor
    # devolve None se ele nao responder em 1.5s
    # conecta pelo ip (que vem da referencia) e nao pelo nome do container: quando
    # um container para o nome dele some da rede, o zmq ficava uns 5s procurando
    # o nome e enquanto isso o servidor nao respondia mais ninguem
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.LINGER, 0)
    s.setsockopt(zmq.RCVTIMEO, 1500)
    s.connect(f"tcp://{endereco}:{PORTA_SERVIDORES}")
    msg["relogio"] = incrementar_relogio()
    msg["timestamp"] = agora()
    print(f"[{NOME}] ENVIADO para {destino}:  {msg}", flush=True)
    s.send(msgpack.packb(msg))
    try:
        resposta = msgpack.unpackb(s.recv())
        atualizar_relogio(resposta["relogio"])
        print(f"[{NOME}] RECEBIDO de {destino}: {resposta}", flush=True)
    except zmq.Again:
        resposta = None
        print(f"[{NOME}] {destino} nao respondeu", flush=True)
    s.close()
    return resposta


def heartbeat():
    pedir_referencia({"tipo": "heartbeat", "nome": NOME, "endereco": ENDERECO})


def anunciar_coordenador():
    msg = {
        "tipo": "coordenador",
        "coordenador": NOME,
        "rank": rank,
        "endereco": ENDERECO,
        "relogio": incrementar_relogio(),
        "timestamp": agora(),
    }
    pub.send_multipart([b"servers", msgpack.packb(msg)])
    print(f"[{NOME}] PUBLICADO em servers: {msg}", flush=True)


def eleicao():
    # algoritmo do valentao: pergunta para todo mundo que tem rank maior
    # se ninguem responder eu sou o coordenador
    global coordenador, rank_coordenador, endereco_coordenador
    print(f"[{NOME}] iniciando eleicao", flush=True)
    servidores = pedir_referencia({"tipo": "lista"})["servidores"]

    alguem_respondeu = False
    for s in servidores:
        if s["rank"] > rank:
            resposta = pedir_servidor(s["nome"], s["endereco"], {"tipo": "eleicao", "nome": NOME})
            if resposta is not None:
                alguem_respondeu = True

    if alguem_respondeu:
        # tem servidor com rank maior vivo, ele que vai avisar no topico servers
        coordenador = None
    else:
        coordenador = NOME
        rank_coordenador = rank
        endereco_coordenador = ENDERECO
        diferencas.clear()
        anunciar_coordenador()


def sincronizar_relogio():
    global ajuste, precisa_eleicao
    if coordenador is None:
        precisa_eleicao = True
        return
    if coordenador == NOME:
        return

    inicio = time()
    msg = {"tipo": "relogio", "nome": NOME, "hora": agora()}
    resposta = pedir_servidor(coordenador, endereco_coordenador, msg)
    if resposta is None:
        # coordenador caiu
        precisa_eleicao = True
        return

    # soma metade do tempo de ida e volta para compensar a latencia
    latencia = (time() - inicio) / 2
    ajuste = resposta["hora"] + latencia - time()
    print(f"[{NOME}] relogio sincronizado com {coordenador}, ajuste de {ajuste:.6f}s", flush=True)


def berkeley(msg):
    # roda no coordenador. guarda a diferenca do relogio de cada servidor que pediu,
    # tira a media (contando o proprio coordenador com diferenca 0) e todo mundo
    # anda para essa media
    global ajuste
    diferencas[msg["nome"]] = msg["hora"] - agora()
    media = sum(diferencas.values()) / (len(diferencas) + 1)
    # diferenca menor que 10ms e so o atraso da rede, nao vale mexer no relogio
    if abs(media) > 0.01:
        ajuste += media
        for n in diferencas:
            diferencas[n] -= media
    # quem pediu vai acertar o relogio com a resposta
    diferencas[msg["nome"]] = 0
    return {"tipo": "relogio", "status": "ok", "hora": agora()}


def atender_servidores():
    # roda em uma thread separada para responder os outros servidores
    # mesmo quando o loop principal esta ocupado
    global precisa_eleicao
    rep = context.socket(zmq.REP)
    rep.bind(f"tcp://*:{PORTA_SERVIDORES}")
    while True:
        msg = msgpack.unpackb(rep.recv())
        atualizar_relogio(msg["relogio"])
        print(f"[{NOME}] RECEBIDO de {msg.get('nome')}: {msg}", flush=True)

        if msg["tipo"] == "eleicao":
            # responde OK e faz a propria eleicao
            resposta = {"tipo": "eleicao", "status": "ok"}
            precisa_eleicao = True
        elif msg["tipo"] == "relogio":
            if coordenador == NOME:
                resposta = berkeley(msg)
            else:
                resposta = {"tipo": "relogio", "status": "erro", "erro": "nao sou o coordenador"}
        else:
            resposta = {"tipo": msg["tipo"], "status": "erro", "erro": "tipo de mensagem desconhecido"}

        resposta["servidor"] = NOME
        resposta["relogio"] = incrementar_relogio()
        resposta["timestamp"] = agora()
        rep.send(msgpack.packb(resposta))
        print(f"[{NOME}] ENVIADO para {msg.get('nome')}:  {resposta}", flush=True)


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
        "timestamp": agora(),
        "relogio": incrementar_relogio(),
    }
    # o topico e o nome do canal
    pub.send_multipart([canal.encode(), msgpack.packb(publicacao)])
    print(f"[{NOME}] PUBLICADO em {canal}: {publicacao}", flush=True)
    salvar_mensagem(publicacao)
    return {"tipo": "publicar", "status": "ok"}


def atender_cliente():
    global qtd_mensagens
    msg = msgpack.unpackb(socket.recv())
    atualizar_relogio(msg["relogio"])
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
    resposta["relogio"] = incrementar_relogio()
    resposta["timestamp"] = agora()
    socket.send(msgpack.packb(resposta))
    print(f"[{NOME}] ENVIADO:  {resposta}", flush=True)

    qtd_mensagens += 1
    # a cada 10 mensagens de clientes avisa a referencia que esta vivo
    if qtd_mensagens % 10 == 0:
        heartbeat()
    # a cada 15 acerta o relogio com o coordenador
    if qtd_mensagens % 15 == 0:
        sincronizar_relogio()


threading.Thread(target=atender_servidores, daemon=True).start()

rank = pedir_referencia({"tipo": "rank", "nome": NOME, "endereco": ENDERECO})["rank"]
print(f"[{NOME}] iniciado com rank {rank}, {len(canais)} canais carregados", flush=True)

# espera os sockets do pub/sub conectarem, senao o primeiro aviso de coordenador se perde
sleep(2)

poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)
poller.register(sub, zmq.POLLIN)

while True:
    eventos = dict(poller.poll(1000))

    if sub in eventos:
        topico, dados = sub.recv_multipart()
        msg = msgpack.unpackb(dados)
        atualizar_relogio(msg["relogio"])
        print(f"[{NOME}] RECEBIDO do topico servers: {msg}", flush=True)
        coordenador = msg["coordenador"]
        endereco_coordenador = msg["endereco"]
        rank_coordenador = msg["rank"]
        print(f"[{NOME}] coordenador agora e {coordenador}", flush=True)
        if rank_coordenador < rank:
            # meu rank e maior que o do coordenador, entao eu que deveria ser
            precisa_eleicao = True

    if socket in eventos:
        atender_cliente()

    if precisa_eleicao:
        precisa_eleicao = False
        eleicao()
