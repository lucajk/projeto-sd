const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const NOME = process.env.NOME || "bot";
const NOMES_CANAIS = ["geral", "jogos", "musica", "filmes", "estudos", "futebol", "memes", "series"];

const socket = new zmq.Request();
socket.connect("tcp://broker:5555");

function dormir(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function enviar(msg) {
  msg.timestamp = Date.now() / 1000;
  console.log(`[${NOME}] ENVIADO: `, JSON.stringify(msg));
  await socket.send(encode(msg));
  const [dados] = await socket.receive();
  const resposta = decode(dados);
  console.log(`[${NOME}] RECEBIDO:`, JSON.stringify(resposta));
  return resposta;
}

async function main() {
  // login: fica tentando ate dar certo
  while (true) {
    const resposta = await enviar({ tipo: "login", usuario: NOME });
    if (resposta.status == "ok") break;
    await dormir(1000);
  }

  while (true) {
    const resposta = await enviar({ tipo: "listar" });
    const canais = resposta.canais;

    // se o canal sorteado ainda nao existe o bot cria
    const canal = NOMES_CANAIS[Math.floor(Math.random() * NOMES_CANAIS.length)];
    if (!canais.includes(canal)) {
      await enviar({ tipo: "canal", canal: canal });
    }

    await dormir(3000);
  }
}

main();
