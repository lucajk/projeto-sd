# Projeto de Sistemas Distribuídos (CC7261)

Sistema de troca de mensagens parecido com um BBS/IRC bem simples: bots fazem login, criam canais e publicam mensagens neles. Tudo roda em containers e a troca de mensagens é feita com ZeroMQ.

Integrantes:
- Luca Juraski - RA 521240127 (Python)
- Matheus da Silva Cruz - RA 521240135 (JavaScript)

## Como rodar

```
docker compose up
```

Sobem o broker, o proxy, 4 servidores (2 em Python e 2 em JavaScript) e 4 bots (2 de cada linguagem). Os dados de cada servidor ficam na pasta `dados/<nome do servidor>` (criada na primeira execução). Para começar do zero é só apagar a pasta `dados`.

## Escolhas

### Linguagens

Python e JavaScript (Node.js). Como somos uma dupla, cada linguagem tem o seu cliente (bot) e o seu servidor, e todos conversam entre si: um bot em Python pode ser atendido por um servidor em JavaScript e o contrário também.

O broker e o proxy foram feitos em Python, a partir dos exemplos das aulas (broker: ROUTER na porta 5555 para os clientes e DEALER na 5556 para os servidores).

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
