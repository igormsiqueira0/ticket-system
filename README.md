# Le Mans
Projeto para a disciplina de Sistemas Distribuídos da UFABC
Consiste na simulação da corrida Le Mans para uma "equipe"

Utiliza os conceitos de Barriers, Queue, Locks e Leader Election

Para usar, basta te uma instalação do Apache ZooKeeper no computador e atualizar a váriavel ZK para o diretório de execução do ZooKeeper
O "Queue Producer" pode ser usado ao rodar run_timer.bat
Os processos da "Barrier" podem ser executados com run_racer.bat
O tamanho da barreira é definido na váriavel RACERS
O timer é responsável por gerar n elementos na fila (no código, 24) na qual os corredores (liberados após sincronização na barreira) irão consumir (no código, cada elemento da fila a cada 10 segundos, simulando 1h da corrida)
