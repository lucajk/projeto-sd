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
const PORTA_SERVIDORES = 5560; // porta onde um servidor atende os outros

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

let coordenador = null; // nome do servidor eleito
let rankCoordenador = 0;
let enderecoCoordenador = null;
let emEleicao = false;
let diferencas = {}; // usado so pelo coordenador (berkeley): nome -> diferenca de relogio

// socket para publicar nos canais (passa pelo proxy)
const pub = new zmq.Publisher();
pub.connect("tcp://proxy:5557");

function agora() {
  // hora do container corrigida pelo coordenador
  return Date.now() / 1000 + ajuste;
}

function dormir(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

// o socket PUB nao aceita dois send ao mesmo tempo, entao um espera o outro terminar
let filaPub = Promise.resolve();
function enviarPub(topico, msg) {
  filaPub = filaPub.then(() => pub.send([topico, encode(msg)]));
  return filaPub;
}

// faz uma requisicao e devolve a resposta, ou null se ninguem responder em 1.5s
// abre um socket por requisicao porque o REQ nao deixa mandar duas ao mesmo tempo
// (o heartbeat e a eleicao podem acontecer juntos)
// para outro servidor conecta pelo ip (que vem da referencia) e nao pelo nome do container:
// quando um container para o nome dele some da rede, o zmq ficava uns 5s procurando
// o nome e enquanto isso o servidor nao respondia mais ninguem
async function pedir(destino, endereco, porta, msg) {
  const s = new zmq.Request({ receiveTimeout: 1500, linger: 0 });
  s.connect(`tcp://${endereco}:${porta}`);
  relogio += 1;
  msg.relogio = relogio;
  msg.timestamp = agora();
  console.log(`[${NOME}] ENVIADO para ${destino}: `, JSON.stringify(msg));
  let resposta = null;
  try {
    await s.send(encode(msg));
    const [dados] = await s.receive();
    resposta = decode(dados);
    relogio = Math.max(relogio, resposta.relogio);
    console.log(`[${NOME}] RECEBIDO de ${destino}:`, JSON.stringify(resposta));
  } catch (erro) {
    console.log(`[${NOME}] ${destino} nao respondeu`);
  }
  s.close();
  return resposta;
}

async function pedirReferencia(msg) {
  // a referencia sempre esta no ar, entao insiste ate responder
  while (true) {
    const resposta = await pedir("referencia", "referencia", 5559, msg);
    if (resposta != null) return resposta;
  }
}

async function heartbeat() {
  await pedirReferencia({ tipo: "heartbeat", nome: NOME, endereco: ENDERECO });
}

async function anunciarCoordenador() {
  relogio += 1;
  const msg = {
    tipo: "coordenador",
    coordenador: NOME,
    rank: rank,
    endereco: ENDERECO,
    relogio: relogio,
    timestamp: agora(),
  };
  await enviarPub("servers", msg);
  console.log(`[${NOME}] PUBLICADO em servers:`, JSON.stringify(msg));
}

// algoritmo do valentao: pergunta para todo mundo que tem rank maior
// se ninguem responder eu sou o coordenador
async function eleicao() {
  if (emEleicao) return;
  emEleicao = true;
  console.log(`[${NOME}] iniciando eleicao`);
  const servidores = (await pedirReferencia({ tipo: "lista" })).servidores;

  let alguemRespondeu = false;
  for (const s of servidores) {
    if (s.rank > rank) {
      const resposta = await pedir(s.nome, s.endereco, PORTA_SERVIDORES, { tipo: "eleicao", nome: NOME });
      if (resposta != null) alguemRespondeu = true;
    }
  }

  if (alguemRespondeu) {
    // tem servidor com rank maior vivo, ele que vai avisar no topico servers
    coordenador = null;
  } else {
    coordenador = NOME;
    rankCoordenador = rank;
    enderecoCoordenador = ENDERECO;
    diferencas = {};
    await anunciarCoordenador();
  }
  emEleicao = false;
}

async function sincronizarRelogio() {
  if (coordenador == null) {
    await eleicao();
    return;
  }
  if (coordenador == NOME) return;

  const inicio = Date.now() / 1000;
  const msg = { tipo: "relogio", nome: NOME, hora: agora() };
  const resposta = await pedir(coordenador, enderecoCoordenador, PORTA_SERVIDORES, msg);
  if (resposta == null) {
    // coordenador caiu
    await eleicao();
    return;
  }
  if (resposta.status != "ok") return;

  // soma metade do tempo de ida e volta para compensar a latencia
  const fim = Date.now() / 1000;
  const latencia = (fim - inicio) / 2;
  ajuste = resposta.hora + latencia - fim;
  console.log(`[${NOME}] relogio sincronizado com ${coordenador}, ajuste de ${ajuste.toFixed(6)}s`);
}

// roda no coordenador. guarda a diferenca do relogio de cada servidor que pediu,
// tira a media (contando o proprio coordenador com diferenca 0) e todo mundo
// anda para essa media
function berkeley(msg) {
  diferencas[msg.nome] = msg.hora - agora();
  let soma = 0;
  for (const n in diferencas) soma += diferencas[n];
  const media = soma / (Object.keys(diferencas).length + 1);
  // diferenca menor que 10ms e so o atraso da rede, nao vale mexer no relogio
  if (Math.abs(media) > 0.01) {
    ajuste += media;
    for (const n in diferencas) diferencas[n] -= media;
  }
  // quem pediu vai acertar o relogio com a resposta
  diferencas[msg.nome] = 0;
  return { tipo: "relogio", status: "ok", hora: agora() };
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
  await enviarPub(msg.canal, publicacao);
  console.log(`[${NOME}] PUBLICADO em ${msg.canal}:`, JSON.stringify(publicacao));
  salvarMensagem(publicacao);
  return { tipo: "publicar", status: "ok" };
}

async function atenderClientes() {
  const socket = new zmq.Reply();
  socket.connect("tcp://broker:5556");

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
    resposta.relogio = relogio;
    resposta.timestamp = agora();
    await socket.send(encode(resposta));
    console.log(`[${NOME}] ENVIADO: `, JSON.stringify(resposta));

    qtdMensagens += 1;
    // a cada 10 mensagens de clientes avisa a referencia que esta vivo
    if (qtdMensagens % 10 == 0) {
      await heartbeat();
    }
    // a cada 15 acerta o relogio com o coordenador
    if (qtdMensagens % 15 == 0) {
      await sincronizarRelogio();
    }
  }
}

// responde as requisicoes que vem dos outros servidores
async function atenderServidores() {
  const rep = new zmq.Reply();
  await rep.bind(`tcp://*:${PORTA_SERVIDORES}`);

  for await (const [dados] of rep) {
    const msg = decode(dados);
    relogio = Math.max(relogio, msg.relogio);
    console.log(`[${NOME}] RECEBIDO de ${msg.nome}:`, JSON.stringify(msg));

    let resposta;
    let fazerEleicao = false;
    if (msg.tipo == "eleicao") {
      // responde OK e faz a propria eleicao
      resposta = { tipo: "eleicao", status: "ok" };
      fazerEleicao = true;
    } else if (msg.tipo == "relogio") {
      if (coordenador == NOME) {
        resposta = berkeley(msg);
      } else {
        resposta = { tipo: "relogio", status: "erro", erro: "nao sou o coordenador" };
      }
    } else {
      resposta = { tipo: msg.tipo, status: "erro", erro: "tipo de mensagem desconhecido" };
    }

    relogio += 1;
    resposta.servidor = NOME;
    resposta.relogio = relogio;
    resposta.timestamp = agora();
    await rep.send(encode(resposta));
    console.log(`[${NOME}] ENVIADO para ${msg.nome}: `, JSON.stringify(resposta));

    // sem await para continuar respondendo os outros servidores durante a eleicao
    if (fazerEleicao) eleicao();
  }
}

// fica ouvindo o topico servers para saber quem e o coordenador
async function ouvirCoordenador() {
  const sub = new zmq.Subscriber();
  sub.connect("tcp://proxy:5558");
  sub.subscribe("servers");

  for await (const [topico, dados] of sub) {
    const msg = decode(dados);
    relogio = Math.max(relogio, msg.relogio);
    console.log(`[${NOME}] RECEBIDO do topico servers:`, JSON.stringify(msg));
    coordenador = msg.coordenador;
    enderecoCoordenador = msg.endereco;
    rankCoordenador = msg.rank;
    console.log(`[${NOME}] coordenador agora e ${coordenador}`);
    if (rankCoordenador < rank) {
      // meu rank e maior que o do coordenador, entao eu que deveria ser
      eleicao();
    }
  }
}

async function main() {
  atenderServidores();
  ouvirCoordenador();

  rank = (await pedirReferencia({ tipo: "rank", nome: NOME, endereco: ENDERECO })).rank;
  console.log(`[${NOME}] iniciado com rank ${rank}, ${canais.length} canais carregados`);

  // espera os sockets do pub/sub conectarem, senao o primeiro aviso de coordenador se perde
  await dormir(2000);

  atenderClientes();
  // comeca sem coordenador, entao ja pede eleicao
  eleicao();
}

main();
