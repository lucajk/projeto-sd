const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const fs = require("fs");
const os = require("os");

const NOME = process.env.NOME || "servidor";

// ip desse container na rede do docker
function meuEndereco() {
  const redes = os.networkInterfaces();
  for (const nome in redes) {
    for (const r of redes[nome]) {
      if (r.family == "IPv4" && !r.internal) return r.address;
    }
  }
  return os.hostname();
}
const ENDERECO = meuEndereco();

const PASTA = "dados";
const ARQ_LOGINS = PASTA + "/logins.txt";
const ARQ_CANAIS = PASTA + "/canais.json";
const ARQ_MENSAGENS = PASTA + "/mensagens.txt";

fs.mkdirSync(PASTA, { recursive: true });

// carrega os canais que ja estavam salvos
let canais = [];
if (fs.existsSync(ARQ_CANAIS)) {
  canais = JSON.parse(fs.readFileSync(ARQ_CANAIS));
}

let relogio = 0; // relogio logico
let ajuste = 0; // correcao do relogio fisico
let qtdMensagens = 0; // mensagens de clientes recebidas
let rank = 0;

// socket para publicar nos canais (passa pelo proxy)
const pub = new zmq.Publisher();
pub.connect("tcp://proxy:5557");

// socket para falar com o servico de referencia
const ref = new zmq.Request();
ref.connect("tcp://referencia:5559");

function agora() {
  // hora do container corrigida pela referencia
  return Date.now() / 1000 + ajuste;
}

function salvarLogin(usuario, timestamp) {
  const linha = JSON.stringify({ usuario: usuario, timestamp: timestamp });
  fs.appendFileSync(ARQ_LOGINS, linha + "\n");
}

function salvarCanais() {
  fs.writeFileSync(ARQ_CANAIS, JSON.stringify(canais));
}

function salvarMensagem(publicacao) {
  fs.appendFileSync(ARQ_MENSAGENS, JSON.stringify(publicacao) + "\n");
}

function nomeValido(nome) {
  if (typeof nome !== "string") return false;
  return /^[a-z0-9_-]{1,20}$/.test(nome);
}

async function pedirReferencia(msg) {
  relogio += 1;
  msg.relogio = relogio;
  msg.timestamp = agora();
  console.log(`[${NOME}] ENVIADO para referencia: `, JSON.stringify(msg));
  await ref.send(encode(msg));
  const [dados] = await ref.receive();
  const resposta = decode(dados);
  relogio = Math.max(relogio, resposta.relogio);
  console.log(`[${NOME}] RECEBIDO da referencia:`, JSON.stringify(resposta));
  return resposta;
}

async function heartbeat() {
  const resposta = await pedirReferencia({ tipo: "heartbeat", nome: NOME, endereco: ENDERECO });
  // aproveita a resposta para acertar o relogio
  ajuste = resposta.hora - Date.now() / 1000;
  console.log(`[${NOME}] relogio ajustado em ${ajuste.toFixed(6)}s`);
  await pedirReferencia({ tipo: "lista" });
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

async function publicar(msg) {
  if (!nomeValido(msg.canal)) {
    return { tipo: "publicar", status: "erro", erro: "nome de canal invalido" };
  }
  if (typeof msg.mensagem !== "string" || msg.mensagem == "") {
    return { tipo: "publicar", status: "erro", erro: "mensagem vazia" };
  }
  // nao da pra conferir se o canal existe porque ele pode ter sido
  // criado em outro servidor (o broker distribui as requisicoes)

  relogio += 1;
  const publicacao = {
    tipo: "publicacao",
    canal: msg.canal,
    usuario: msg.usuario,
    mensagem: msg.mensagem,
    servidor: NOME,
    timestamp: agora(),
    relogio: relogio,
  };
  // o topico e o nome do canal
  await pub.send([msg.canal, encode(publicacao)]);
  console.log(`[${NOME}] PUBLICADO em ${msg.canal}:`, JSON.stringify(publicacao));
  salvarMensagem(publicacao);
  return { tipo: "publicar", status: "ok" };
}

async function main() {
  const socket = new zmq.Reply();
  socket.connect("tcp://broker:5556");

  rank = (await pedirReferencia({ tipo: "rank", nome: NOME, endereco: ENDERECO })).rank;
  console.log(`[${NOME}] iniciado com rank ${rank}, ${canais.length} canais carregados`);

  for await (const [dados] of socket) {
    const msg = decode(dados);
    relogio = Math.max(relogio, msg.relogio);
    console.log(`[${NOME}] RECEBIDO:`, JSON.stringify(msg));

    let resposta;
    if (msg.tipo == "login") {
      resposta = login(msg);
    } else if (msg.tipo == "canal") {
      resposta = criarCanal(msg);
    } else if (msg.tipo == "listar") {
      resposta = { tipo: "listar", status: "ok", canais: canais };
    } else if (msg.tipo == "publicar") {
      resposta = await publicar(msg);
    } else {
      resposta = { tipo: msg.tipo, status: "erro", erro: "tipo de mensagem desconhecido" };
    }

    relogio += 1;
    resposta.servidor = NOME;
    resposta.timestamp = agora();
    resposta.relogio = relogio;
    await socket.send(encode(resposta));
    console.log(`[${NOME}] ENVIADO: `, JSON.stringify(resposta));

    // a cada 10 mensagens de clientes avisa a referencia que esta vivo
    qtdMensagens += 1;
    if (qtdMensagens % 10 == 0) {
      await heartbeat();
    }
  }
}

main();
