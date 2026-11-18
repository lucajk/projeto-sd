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

Um problema que apareceu: no começo o servidor não conseguia conferir se o canal da publicação existia, porque o canal podia ter sido criado em outro servidor. Isso foi resolvido na parte 5 com a réplica dos dados.

## Parte 3: relógios e heartbeat

Relógio lógico: todos os processos (bots, servidores e referência) têm um contador. Ele é incrementado antes de cada envio e vai no campo `relogio` de toda mensagem. Quando chega uma mensagem, o processo fica com o maior valor entre o seu contador e o que recebeu.

Foi adicionado o container `referencia` (porta 5559, REQ/REP), que só conversa com os servidores:

| tipo | o servidor manda | a referência responde |
|---|---|---|
| `rank` | `nome` e `endereco` (ip do container) | `rank` |
| `lista` | nada | `servidores` (nome, rank e endereço de cada um) |
| `heartbeat` | `nome` e `endereco` | `status` |

O rank é a ordem de chegada (o primeiro servidor que pede fica com 1, o segundo com 2...). Se o mesmo nome pedir de novo recebe o mesmo rank.

A cada 10 mensagens de clientes o servidor manda um heartbeat. A referência guarda a hora do último heartbeat de cada servidor, e quem fica mais de 30 segundos sem mandar sai da lista. Se ele voltar a mandar heartbeat entra de novo com o mesmo rank.

Relógio físico: na parte 3 a resposta do heartbeat trazia a hora da referência. Na parte 4 isso foi trocado pela sincronização com o coordenador (a referência não devolve mais a hora). O servidor guarda a correção na variável `ajuste`, e todos os timestamps dele são a hora do container mais esse ajuste.

Os bots também ganharam um tempo limite de 5 segundos na requisição. Se o servidor que ia responder cair no meio, o socket REQ fica travado esperando para sempre, então o bot fecha o socket, abre outro e manda de novo.

## Parte 4: eleição e sincronização com o coordenador

Agora os servidores conversam entre si. Cada servidor abre um socket REP na porta 5560 e os outros conectam usando o endereço (ip) que está na lista da referência.

| tipo | quem manda | resposta |
|---|---|---|
| `eleicao` | servidor que percebeu que não tem coordenador | `status` ok |
| `relogio` | servidor, com a sua `hora` | `hora` correta (só o coordenador responde) |

Eleição (algoritmo do valentão): o servidor pede a lista para a referência e manda `eleicao` para todos que têm rank maior que o dele. Se alguém responder, ele espera o aviso de quem ganhou. Se ninguém responder em 1,5 segundo, ele vira o coordenador e publica o seu nome no tópico `servers`. Quem recebe uma mensagem de `eleicao` responde OK e faz a própria eleição. No fim ganha o servidor vivo com o maior rank.

Todos os servidores ficam inscritos no tópico `servers` e guardam o nome na variável `coordenador`. Se um servidor recebe o aviso de um coordenador com rank menor que o dele, ele faz uma eleição (isso acontece quando um servidor de rank maior volta).

A eleição acontece quando o servidor inicia (ainda não tem coordenador) e quando o coordenador não responde o pedido de relógio.

Sincronização (Berkeley): a cada 15 mensagens de clientes o servidor manda a sua hora para o coordenador. O coordenador guarda a diferença entre a hora de cada servidor e a dele, calcula a média dessas diferenças (contando ele mesmo com diferença zero), corrige o próprio relógio por essa média e devolve a hora já corrigida. O servidor soma metade do tempo de ida e volta da requisição para compensar a latência.

Mudança em relação ao Berkeley original: no original o coordenador é quem pergunta a hora para todos e depois manda o ajuste para cada um. Aqui quem inicia é o servidor (seguindo o diagrama do enunciado), então o coordenador calcula a média com a última hora que cada servidor informou. Também ignoramos médias menores que 10 ms, porque como os containers rodam na mesma máquina essa diferença é só o atraso da rede e o relógio ficava andando para trás um pouquinho a cada sincronização.

Um problema que apareceu nos testes: no começo um servidor conectava no outro pelo nome do container. Quando a gente parava o coordenador para testar a eleição, o nome dele sumia da rede do Docker, e o ZeroMQ ficava uns 5 segundos tentando resolver esse nome. Durante esse tempo o servidor não respondia os outros, então um servidor de rank menor achava que estava sozinho e se declarava coordenador (depois o de rank maior tomava o lugar, mas demorava). Resolvemos fazendo o servidor mandar o seu ip para a referência e conectando sempre pelo ip.

No servidor em Python as requisições dos outros servidores são atendidas em uma thread separada. Sem isso dois servidores podiam ficar travados um esperando a resposta do outro durante a eleição. No JavaScript não precisa porque as funções assíncronas já rodam intercaladas.

## Parte 5: consistência e replicação

O problema: o broker distribui as requisições entre os servidores, então cada servidor ficava só com uma parte dos logins, canais e mensagens. Se um servidor caísse essa parte do histórico sumia, e um cliente que pedisse o histórico recebia só um pedaço.

### Método escolhido

Réplica ativa com atualização por push, com consistência eventual.

Réplica ativa porque o que é enviado para as outras réplicas é a operação de escrita (o login, o canal criado ou a mensagem publicada) no momento em que ela acontece, e cada servidor aplica a operação nos seus próprios arquivos. Push porque quem inicia a propagação é o servidor que recebeu a escrita, sem os outros precisarem pedir.

Consistência eventual porque o servidor responde o cliente sem esperar os outros confirmarem. Por alguns milissegundos os servidores ficam diferentes, mas se pararem de chegar escritas todos terminam com os mesmos dados.

### Como foi implementado

1. O servidor que recebe uma escrita grava no seu disco e publica no tópico `replica` (pelo mesmo proxy do Pub/Sub) uma mensagem com o tipo do dado (`login`, `canal` ou `mensagem`) e o conteúdo.
2. Todos os servidores ficam inscritos em `replica`. Quem recebe grava o dado, se ainda não tiver.
3. Cada login e cada mensagem tem um campo `id` (quem criou + timestamp). O servidor guarda os ids que já tem e ignora os repetidos. Para canal o próprio nome serve de id.
4. Quando um servidor inicia ele manda uma requisição `sincronizar` (na porta 5560) para outro servidor da lista da referência e recebe tudo que aquele servidor tem.

Foi adicionada também a requisição `historico` (`canal`), que devolve o total de mensagens do canal e as 5 últimas. Os bots pedem o histórico antes de cada sequência de 10 mensagens. Como cada servidor grava as mensagens na ordem em que elas chegam, a resposta é ordenada pelo relógio lógico (e pelo timestamp em caso de empate) para todos os servidores responderem na mesma ordem.

Com os canais replicados o servidor voltou a conferir se o canal existe antes de publicar.

### Mudanças no método

O passo 4 não é push, é pull. Tivemos que adicionar porque o Pub/Sub não guarda mensagem: o que é publicado enquanto um servidor está fora do ar nunca chega nele. Então um servidor que cai e volta (ou um servidor novo) ficava com um buraco no histórico. Com o pull na inicialização ele copia o que perdeu e depois segue recebendo pelo push. Como o servidor se inscreve no tópico antes de pedir a cópia, o que chegar repetido é descartado pelo id.

Outro problema que apareceu nos testes: na cópia inicial o servidor gravava os registros um por um, abrindo o arquivo a cada registro. Um servidor que tinha ficado uns 15 minutos fora precisou copiar 4 mil registros e levou mais de um minuto, e nesse tempo ele não respondia a eleição (um servidor de rank menor acabou virando coordenador até ele terminar). Agora a cópia inicial grava tudo de uma vez só.

### Limitações

- Se uma mensagem de réplica se perder com o servidor no ar (por exemplo, o proxy cair), ele só recupera quando reiniciar.
- Não tem resolução de conflito porque os dados só são adicionados, nunca alterados: duas mensagens diferentes nunca brigam entre si, e se dois servidores criarem o mesmo canal ao mesmo tempo o resultado é o mesmo canal nos dois.

### Como testar

```
docker compose stop servidor_py_1
(esperar um pouco)
docker compose start servidor_py_1
wc -l dados/*/mensagens.txt
```

No log do servidor que voltou aparece `dados copiados de ...` com a quantidade de registros que ele tinha perdido, e os arquivos de todos os servidores ficam com a mesma quantidade de linhas (pode ter diferença de uma ou duas enquanto os bots estão mandando).
