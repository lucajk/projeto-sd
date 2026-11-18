import zmq
import msgpack
import os
import random
from time import time, sleep

NOME = os.environ.get("NOME", "bot")
NOMES_CANAIS = ["geral", "jogos", "musica", "filmes", "estudos", "futebol", "memes", "series"]

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")


def enviar(msg):
    msg["timestamp"] = time()
    print(f"[{NOME}] ENVIADO:  {msg}", flush=True)
    socket.send(msgpack.packb(msg))
    resposta = msgpack.unpackb(socket.recv())
    print(f"[{NOME}] RECEBIDO: {resposta}", flush=True)
    return resposta


# login: fica tentando ate dar certo
while True:
    resposta = enviar({"tipo": "login", "usuario": NOME})
    if resposta["status"] == "ok":
        break
    sleep(1)

while True:
    resposta = enviar({"tipo": "listar"})
    canais = resposta["canais"]

    # se o canal sorteado ainda nao existe o bot cria
    canal = random.choice(NOMES_CANAIS)
    if canal not in canais:
        enviar({"tipo": "canal", "canal": canal})

    sleep(3)
