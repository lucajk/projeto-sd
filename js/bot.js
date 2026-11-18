const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const NOME = process.env.NOME || "bot";
const NOMES_CANAIS = ["geral", "jogos", "musica", "filmes", "estudos", "futebol", "memes", "series"];
const FRASES = ["oi pessoal", "alguem ai?", "bom dia", "kkkkk", "concordo", "nao sei", "que legal", "to com sono", "bora", "vou almocar"];

function conectar() {
  const s = new zmq.Request({ receiveTimeout: 5000, linger: 0 });
  s.connect("tcp://broker:5555");
  return s;
}

let socket = conectar();

// socket que recebe as publicacoes dos canais
const sub = new zmq.Subscriber();
sub.connect("tcp://proxy:5558");
const inscritos = [];

let relogio = 0; // relogio logico

function dormir(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function sortear(lista) {
  return lista[Math.floor(Math.random() * lista.length)];
}

async function enviar(msg) {
  let resposta;
  while (true) {
    relogio += 1;
    msg.relogio = relogio;
    msg.timestamp = Date.now() / 1000;
    console.log(`[${NOME}] ENVIADO: `, JSON.stringify(msg));
    await socket.send(encode(msg));
    try {
      const [dados] = await socket.receive();
      resposta = decode(dados);
      break;
    } catch (erro) {
      // ninguem respondeu em 5s (o servidor pode ter caido no meio)
      // o REQ fica travado esperando, entao abre outro e manda de novo
      console.log(`[${NOME}] sem resposta, tentando de novo`);
      socket.close();
      socket = conectar();
    }
  }
  relogio = Math.max(relogio, resposta.relogio);
  console.log(`[${NOME}] RECEBIDO:`, JSON.stringify(resposta));
  return resposta;
}

// fica rodando junto com o resto, mostrando o que chega nos canais
async function receberPublicacoes() {
  for await (const [canal, dados] of sub) {
    const msg = decode(dados);
    relogio = Math.max(relogio, msg.relogio);
    const recebido = Date.now() / 1000;
    console.log(
      `[${NOME}] CANAL ${canal.toString()}: ${msg.usuario} disse "${msg.mensagem}"` +
        ` | enviado em ${msg.timestamp} | recebido em ${recebido} |`,
      JSON.stringify(msg)
    );
  }
}

async function main() {
  // login: fica tentando ate dar certo
  while (true) {
    const resposta = await enviar({ tipo: "login", usuario: NOME });
    if (resposta.status == "ok") break;
    await dormir(1000);
  }

  receberPublicacoes();

  while (true) {
    let canais = (await enviar({ tipo: "listar" })).canais;

    // menos de 5 canais: cria mais um
    if (canais.length < 5) {
      const opcoes = NOMES_CANAIS.filter((c) => !canais.includes(c));
      await enviar({ tipo: "canal", canal: sortear(opcoes) });
      canais = (await enviar({ tipo: "listar" })).canais;
    }

    // inscrito em menos de 3: se inscreve em mais um
    if (inscritos.length < 3) {
      const opcoes = canais.filter((c) => !inscritos.includes(c));
      if (opcoes.length > 0) {
        const canal = sortear(opcoes);
        sub.subscribe(canal);
        inscritos.push(canal);
        console.log(`[${NOME}] inscrito no canal ${canal} (inscritos: ${inscritos})`);
      }
    }

    if (canais.length == 0) {
      await dormir(1000);
      continue;
    }

    const canal = sortear(canais);
    // recupera as mensagens anteriores do canal
    await enviar({ tipo: "historico", canal: canal });
    for (let i = 0; i < 10; i++) {
      const texto = sortear(FRASES) + " " + Math.floor(Math.random() * 1000);
      await enviar({ tipo: "publicar", canal: canal, usuario: NOME, mensagem: texto });
      await dormir(1000);
    }
  }
}

main();
