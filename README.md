# Projeto de Sistemas Distribuídos (CC7261)

Sistema de troca de mensagens parecido com um BBS/IRC bem simples: bots fazem login, criam canais e publicam mensagens neles. Tudo roda em containers e a troca de mensagens é feita com ZeroMQ.

Integrantes:
- Luca Juraski - RA 521240127 (Python)
- Matheus da Silva Cruz - RA 521240135 (JavaScript)

## Como rodar

```
docker compose up
```

Sobem o broker, 4 servidores (2 em Python e 2 em JavaScript) e 4 bots (2 de cada linguagem). Os dados de cada servidor ficam na pasta `dados/<nome do servidor>` (criada na primeira execução). Para começar do zero é só apagar a pasta `dados`.

## Escolhas

### Linguagens

Python e JavaScript (Node.js). Como somos uma dupla, cada linguagem tem o seu cliente (bot) e o seu servidor, e todos conversam entre si: um bot em Python pode ser atendido por um servidor em JavaScript e o contrário também.

O broker foi feito em Python, a partir do exemplo da aula de Request-Reply (ROUTER na porta 5555 para os clientes e DEALER na 5556 para os servidores).

### Serialização

MessagePack. Escolhemos porque ele funciona quase igual ao JSON (monta um dicionário/objeto e serializa), só que o resultado é binário, e não precisa escrever e compilar um arquivo de schema como no Protobuf. Tem biblioteca para as duas linguagens (`msgpack` no Python e `@msgpack/msgpack` no Node).

Toda mensagem é um dicionário com o campo `tipo` e o campo `timestamp` (hora do envio, em segundos). As respostas do servidor têm `status` igual a `ok` ou `erro`, e quando é erro vem o campo `erro` com o motivo.

### Persistência

Cada servidor grava os seus dados em arquivos dentro da própria pasta (um volume separado para cada servidor, nenhum arquivo é compartilhado):

- `logins.txt`: uma linha por login, com o usuário e o timestamp do login
- `canais.json`: lista com os nomes dos canais

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

## Como os bots funcionam

O bot faz login, e depois fica em loop: pede a lista de canais, sorteia um nome de canal e, se ele não estiver na lista, cria. Como o broker distribui as requisições entre os servidores (round-robin) e cada servidor tem os seus próprios dados, a lista muda dependendo de qual servidor respondeu, e às vezes o bot tenta criar um canal que aquele servidor já tem (aí aparece o erro de canal duplicado).
