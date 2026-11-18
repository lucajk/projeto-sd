import zmq
import msgpack
import os
import random
from time import time, sleep

NOME = os.environ.get("NOME", "bot")
NOMES_CANAIS = ["geral", "jogos", "musica", "filmes", "estudos", "futebol", "memes", "series"]
FRASES = ["oi pessoal", "alguem ai?", "bom dia", "kkkkk", "concordo", "nao sei", "que legal", "to com sono", "bora", "vou almocar"]

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")

# socket que recebe as publicacoes dos canais
sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
inscritos = []


def enviar(msg):
    msg["timestamp"] = time()
    print(f"[{NOME}] ENVIADO:  {msg}", flush=True)
    socket.send(msgpack.packb(msg))
    resposta = msgpack.unpackb(socket.recv())
    print(f"[{NOME}] RECEBIDO: {resposta}", flush=True)
    return resposta


def esperar(segundos):
    # em vez de so dar sleep, aproveita o intervalo para ler o que chegou nos canais
    fim = time() + segundos
    while time() < fim:
        if sub.poll(100):
            canal, dados = sub.recv_multipart()
            msg = msgpack.unpackb(dados)
            print(
                f"[{NOME}] CANAL {canal.decode()}: {msg['usuario']} disse \"{msg['mensagem']}\""
                f" | enviado em {msg['timestamp']} | recebido em {time()} | {msg}",
                flush=True,
            )


# login: fica tentando ate dar certo
while True:
    resposta = enviar({"tipo": "login", "usuario": NOME})
    if resposta["status"] == "ok":
        break
    sleep(1)

while True:
    canais = enviar({"tipo": "listar"})["canais"]

    # menos de 5 canais: cria mais um
    if len(canais) < 5:
        opcoes = [c for c in NOMES_CANAIS if c not in canais]
        enviar({"tipo": "canal", "canal": random.choice(opcoes)})
        canais = enviar({"tipo": "listar"})["canais"]

    # inscrito em menos de 3: se inscreve em mais um
    if len(inscritos) < 3:
        opcoes = [c for c in canais if c not in inscritos]
        if len(opcoes) > 0:
            canal = random.choice(opcoes)
            sub.setsockopt_string(zmq.SUBSCRIBE, canal)
            inscritos.append(canal)
            print(f"[{NOME}] inscrito no canal {canal} (inscritos: {inscritos})", flush=True)

    if len(canais) == 0:
        esperar(1)
        continue

    canal = random.choice(canais)
    for i in range(10):
        texto = random.choice(FRASES) + " " + str(random.randint(0, 999))
        enviar({"tipo": "publicar", "canal": canal, "usuario": NOME, "mensagem": texto})
        esperar(1)
