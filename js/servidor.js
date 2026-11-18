const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const fs = require("fs");

const NOME = process.env.NOME || "servidor";
const PASTA = "dados";
const ARQ_LOGINS = PASTA + "/logins.txt";
const ARQ_CANAIS = PASTA + "/canais.json";

fs.mkdirSync(PASTA, { recursive: true });

// carrega os canais que ja estavam salvos
let canais = [];
if (fs.existsSync(ARQ_CANAIS)) {
  canais = JSON.parse(fs.readFileSync(ARQ_CANAIS));
}

function agora() {
  return Date.now() / 1000;
}

function salvarLogin(usuario, timestamp) {
  const linha = JSON.stringify({ usuario: usuario, timestamp: timestamp });
  fs.appendFileSync(ARQ_LOGINS, linha + "\n");
}

function salvarCanais() {
  fs.writeFileSync(ARQ_CANAIS, JSON.stringify(canais));
}

function nomeValido(nome) {
  if (typeof nome !== "string") return false;
  return /^[a-z0-9_-]{1,20}$/.test(nome);
}

function login(msg) {
  if (!nomeValido(msg.usuario)) {
    return { tipo: "login", status: "erro", erro: "nome de usuario invalido" };
  }
  salvarLogin(msg.usuario, msg.timestamp);
  return { tipo: "login", status: "ok" };
}

function criarCanal(msg) {
  if (!nomeValido(msg.canal)) {
    return { tipo: "canal", status: "erro", erro: "nome de canal invalido" };
  }
  if (canais.includes(msg.canal)) {
    return { tipo: "canal", status: "erro", erro: "canal ja existe" };
  }
  canais.push(msg.canal);
  salvarCanais();
  return { tipo: "canal", status: "ok" };
}

async function main() {
  const socket = new zmq.Reply();
  socket.connect("tcp://broker:5556");
  console.log(`[${NOME}] iniciado, ${canais.length} canais carregados`);

  for await (const [dados] of socket) {
    const msg = decode(dados);
    console.log(`[${NOME}] RECEBIDO:`, JSON.stringify(msg));

    let resposta;
    if (msg.tipo == "login") {
      resposta = login(msg);
    } else if (msg.tipo == "canal") {
      resposta = criarCanal(msg);
    } else if (msg.tipo == "listar") {
      resposta = { tipo: "listar", status: "ok", canais: canais };
    } else {
      resposta = { tipo: msg.tipo, status: "erro", erro: "tipo de mensagem desconhecido" };
    }

    resposta.servidor = NOME;
    resposta.timestamp = agora();
    await socket.send(encode(resposta));
    console.log(`[${NOME}] ENVIADO: `, JSON.stringify(resposta));
  }
}

main();
