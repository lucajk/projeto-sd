# Projeto de Sistemas Distribuídos (CC7261)

Sistema de troca de mensagens parecido com um BBS/IRC bem simples: bots fazem login, criam canais e publicam mensagens neles. Tudo roda em containers e a troca de mensagens é feita com ZeroMQ.

Integrantes:
- Luca Juraski - RA 521240127 (Python)
- Matheus da Silva Cruz - RA 521240135 (JavaScript)

## Como rodar

```
docker compose up
```

Sobem o broker, o proxy, a referência, 4 servidores (2 em Python e 2 em JavaScript) e 4 bots (2 de cada linguagem). Os dados de cada servidor ficam na pasta `dados/<nome do servidor>` (criada na primeira execução). Para começar do zero é só apagar a pasta `dados`.

## Escolhas

### Linguagens

Python e JavaScript (Node.js). Como somos uma dupla, cada linguagem tem o seu cliente (bot) e o seu servidor, e todos conversam entre si: um bot em Python pode ser atendido por um servidor em JavaScript e o contrário também.

O broker, o proxy e o serviço de referência foram feitos em Python, os dois primeiros a partir dos exemplos das aulas (broker: ROUTER na porta 5555 para os clientes e DEALER na 5556 para os servidores).

### Serialização

MessagePack. Escolhemos porque ele funciona quase igual ao JSON (monta um dicionário/objeto e serializa), só que o resultado é binário, e não precisa escrever e compilar um arquivo de schema como no Protobuf. Tem biblioteca para as duas linguagens (`msgpack` no Python e `@msgpack/msgpack` no Node).

Toda mensagem é um dicionário com o campo `tipo` e o campo `timestamp` (hora do envio, em segundos). As respostas do servidor têm `status` igual a `ok` ou `erro`, e quando é erro vem o campo `erro` com o motivo.

### Persistência

Cada servidor grava os seus dados em arquivos dentro da própria pasta (um volume separado para cada servidor, nenhum arquivo é compartilhado):

- `logins.txt`: uma linha por login, com o usuário e o timestamp do login
- `canais.json`: lista com os nomes dos canais
- `mensagens.txt`: uma linha por publicação

Usamos arquivo de texto com um JSON por linha porque dá para abrir e conferir na mão e para adicionar um registro é só escrever no fim do arquivo. (O JSON é usado só no disco, na rede vai MessagePack.)

## Mensagens da parte 1

| tipo | o cliente manda | o servidor responde |
|---|---|---|
| `login` | `usuario` | `status` |
| `canal` | `canal` | `status` |
| `listar` | nada | `canais` (lista com os nomes) |

Erros tratados:
- nome de usuário ou de canal inválido: só aceitamos letras minúsculas, números, `_` e `-`, com no máximo 20 caracteres
- canal que já existe
- tipo de mensagem desconhecido

Se o login der erro o bot espera 1 segundo e tenta de novo.

## Parte 2: publicação nos canais

Foi adicionado o container `proxy`, que é o intermediário do Pub/Sub (porta 5557 XSUB, onde os servidores publicam, e 5558 XPUB, onde os bots se inscrevem).

Para publicar, o bot manda uma requisição `publicar` com `canal`, `usuario` e `mensagem`. O servidor publica no proxy usando o nome do canal como tópico e responde `ok` ou `erro` para o bot. A publicação vai em duas partes (multipart): a primeira é o tópico e a segunda é a mensagem em MessagePack com canal, usuário, texto, servidor que publicou e timestamp.

O bot mostra na tela cada publicação que recebe com o canal, a mensagem, o timestamp do envio e o do recebimento.

As publicações são gravadas no arquivo `mensagens.txt` do servidor que recebeu a requisição, uma por linha, com todos os campos da publicação. Assim dá para recuperar depois quem falou o quê, em qual canal e quando.

Funcionamento dos bots (o padrão do enunciado). A cada volta do loop:
1. pede a lista de canais, e se tiver menos de 5 cria mais um
2. se estiver inscrito em menos de 3 canais, se inscreve em mais um (sorteado)
3. sorteia um canal e manda 10 mensagens com 1 segundo de intervalo

O bot em Python não usa thread: no intervalo de 1 segundo entre as mensagens ele fica lendo o socket SUB. O de JavaScript lê as publicações em uma função assíncrona separada.

Um problema que apareceu: o servidor não consegue conferir se o canal da publicação existe, porque o canal pode ter sido criado em outro servidor. Por enquanto o servidor só confere se o nome é válido e se a mensagem não está vazia.

## Parte 3: relógios e heartbeat

Relógio lógico: todos os processos (bots, servidores e referência) têm um contador. Ele é incrementado antes de cada envio e vai no campo `relogio` de toda mensagem. Quando chega uma mensagem, o processo fica com o maior valor entre o seu contador e o que recebeu.

Foi adicionado o container `referencia` (porta 5559, REQ/REP), que só conversa com os servidores:

| tipo | o servidor manda | a referência responde |
|---|---|---|
| `rank` | `nome` e `endereco` (ip do container) | `rank` |
| `lista` | nada | `servidores` (nome, rank e endereço de cada um) |
| `heartbeat` | `nome` e `endereco` | `status` e `hora` |

O rank é a ordem de chegada (o primeiro servidor que pede fica com 1, o segundo com 2...). Se o mesmo nome pedir de novo recebe o mesmo rank.

A cada 10 mensagens de clientes o servidor manda um heartbeat. A referência guarda a hora do último heartbeat de cada servidor, e quem fica mais de 30 segundos sem mandar sai da lista. Se ele voltar a mandar heartbeat entra de novo com o mesmo rank.

Relógio físico: a resposta do heartbeat traz a hora da referência, e o servidor guarda a diferença para o seu próprio relógio na variável `ajuste`. Todos os timestamps do servidor são a hora do container mais esse ajuste.

Os bots também ganharam um tempo limite de 5 segundos na requisição. Se o servidor que ia responder cair no meio, o socket REQ fica travado esperando para sempre, então o bot fecha o socket, abre outro e manda de novo.
