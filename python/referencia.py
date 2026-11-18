import zmq
import msgpack
from time import time

TEMPO_LIMITE = 30  # segundos sem heartbeat para tirar o servidor da lista

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

relogio = 0
ranks = {}   # nome -> rank
enderecos = {}  # nome -> ip do servidor
ultimo = {}  # nome -> hora do ultimo heartbeat (so quem esta disponivel)

print("referencia rodando", flush=True)

while True:
    if socket.poll(1000):
        msg = msgpack.unpackb(socket.recv())
        relogio = max(relogio, msg["relogio"])
        print(f"[referencia] RECEBIDO: {msg}", flush=True)

        tipo = msg.get("tipo")
        nome = msg.get("nome")

        if tipo == "rank":
            # o rank e a ordem de chegada, nome repetido fica com o mesmo rank
            if nome not in ranks:
                ranks[nome] = len(ranks) + 1
            ultimo[nome] = time()
            enderecos[nome] = msg.get("endereco")
            resposta = {"tipo": "rank", "rank": ranks[nome]}
        elif tipo == "lista":
            lista = []
            for n in ultimo:
                lista.append({"nome": n, "rank": ranks[n], "endereco": enderecos[n]})
            resposta = {"tipo": "lista", "servidores": lista}
        elif tipo == "heartbeat":
            if nome not in ranks:
                ranks[nome] = len(ranks) + 1
            ultimo[nome] = time()
            enderecos[nome] = msg.get("endereco")
            resposta = {"tipo": "heartbeat", "status": "ok"}
        else:
            resposta = {"tipo": tipo, "status": "erro", "erro": "tipo de mensagem desconhecido"}

        relogio += 1
        resposta["relogio"] = relogio
        resposta["timestamp"] = time()
        socket.send(msgpack.packb(resposta))
        print(f"[referencia] ENVIADO:  {resposta}", flush=True)

    # tira da lista quem parou de mandar heartbeat
    for nome in list(ultimo):
        if time() - ultimo[nome] > TEMPO_LIMITE:
            del ultimo[nome]
            print(f"[referencia] {nome} removido da lista (sem heartbeat)", flush=True)
