# FIWARE Provisioner

Interface gráfica em Tkinter que provisiona, verifica, remove e simula um dispositivo IoT num servidor FIWARE com Orion Context Broker, IoT Agent UltraLight 2.0 (transporte MQTT) e STH-Comet. Ela executa a mesma sequência da collection Postman "FIWARE Descomplicado", com os mesmos endpoints, mas monta todos os JSONs a partir de quatro dados: IP do servidor, nome do dispositivo, ID e atributos.

O servidor esperado é o [Edge IoT Lab](https://github.com/lucadaugusto/edge-iot-lab) ou o [FIWARE Descomplicado](https://github.com/fabiocabrini/fiware) original.

## Requisitos

- Python 3.8 ou mais novo, com Tkinter. Não há dependência externa: HTTP e MQTT usam só a biblioteca padrão.
- `pip install -r requirements.txt` não instala nada: o arquivo só documenta que não há pacotes do PyPI. O Tkinter vem do instalador do Python ou do gerenciador de pacotes do sistema.
- Windows e macOS: o instalador do [python.org](https://www.python.org/downloads/) já inclui o Tkinter.
- Ubuntu/Debian: `sudo apt install python3-tk`.
- No servidor, portas TCP 4041 (IoT Agent), 1026 (Orion), 8666 (STH-Comet) e 1883 (MQTT) liberadas para a sua máquina.

## Execução

```
git clone https://github.com/lucadaugusto/fiware-provisioner
cd fiware-provisioner
python fiware_provisioner.py
```

No Linux e no macOS, use `python3` se `python` apontar para o Python 2.

## Identificadores gerados

O nome e o ID definem todos os identificadores, no padrão do tutorial de aula:

| Campo | Exemplo (nome `device`, ID `001`) |
|---|---|
| `device_id` (IoT Agent) | `device001` |
| `entity_type` (Orion) | `device` |
| `entity_name` (Orion) | `urn:ngsi-ld:device:001` |
| Tópico de dados | `/TEF/device001/attrs` e `/TEF/device001/attrs/<object_id>` |
| Tópico de comandos | `/TEF/device001/cmd`, payload `device001@on\|` |
| Confirmação de comandos | `/TEF/device001/cmdexe` |

O painel "Gerado automaticamente", à esquerda, mostra esses valores para copiar no firmware.

Os valores fixos da turma ficam no topo do script, na seção `CONFIGURACAO DA TURMA`: `fiware-service` = `smart`, `fiware-servicepath` = `/`, apikey `TEF` e `entity_type` do service group = `Thing`. São os mesmos da collection Postman.

## Abas

| Aba | O que faz | Itens da collection |
|---|---|---|
| Fluxo completo | Provisiona, verifica ou remove tudo de uma vez. Pode rodar mais de uma vez sem duplicar nada | 2, 3, 4 e STH 2 |
| IoT Agent :4041 | Health check, service group, provisionar, consultar, atualizar tipos e remover o device | 1.1, 2, 2.1, 3, 5, 9 |
| Orion :1026 | Entidades, leitura e escrita de atributo, registration e envio de comandos | Orion 1 a 7, 4, 6, 7, 8, 10 |
| STH-Comet :8666 | Health check, subscription e série temporal (`lastN`) | STH 1, 2, 3 |
| Simulação | Publica dados de teste por MQTT (caminho real, passa pelo IoT Agent) ou direto no Orion | — |

Só atributos `Integer`, `Float` e `Number` entram na subscription do STH-Comet. Atributos `Text` e `Boolean` chegam ao Orion, mas não geram histórico.

## Fluxo de uso em aula

1. Preencha IP, nome e ID, e ajuste atributos e comandos ao seu dispositivo. Os campos já vêm com o exemplo do tutorial (`s`/`state`/`Text` e `p`/`p`/`Integer`, comandos `on, off`).
2. Na aba Fluxo completo, clique em **Provisionar tudo**.
3. Copie para o firmware o `device_id` e os tópicos do painel "Gerado automaticamente".
4. Antes de ligar o hardware, use a aba Simulação → **Enviar 1 amostra** em modo MQTT para testar o caminho MQTT → IoT Agent → Orion → STH-Comet.
5. Clique em **Verificar**. O resumo indica o que está faltando: device, entidade, tipos, subscription ou dado no STH.
6. No fim, use **Remover dispositivo**. O service group não é removido, porque a turma inteira o compartilha.

## Formatos de publicação na simulação

| Formato | Tópico e payload |
|---|---|
| Igual ao firmware | `Text`/`Boolean` juntos em `/attrs` (`s\|on`); numéricos em `/attrs/<object_id>` (`45`) |
| Um tópico por atributo | Cada atributo em `/attrs/<object_id>`, payload com o valor puro |
| Tópico único | Tudo em `/attrs`: `s\|on\|p\|45` |

O sufixo do tópico é sempre o `object_id`, nunca o `name`. Um sufixo que não corresponde a nenhum `object_id` faz o IoT Agent criar um atributo novo, com o tipo padrão.

## Problemas comuns

| Sintoma | Causa provável |
|---|---|
| `FALHA DE REDE` | IP errado, containers parados ou portas bloqueadas no firewall/security group da VM |
| O device volta depois de removido | Autoprovisionamento: um ESP32 ou uma simulação continua publicando em `/TEF/<device_id>/...`. Desligue e remova de novo |
| `<comando>_status` fica `PENDING` | O firmware não responde em `/TEF/<device_id>/cmdexe` com `<device_id>@<comando>\|<resultado>` |
| Tipo errado no Orion | O device foi provisionado com outro tipo. Use **Atualizar meu device (tipos)**; o Orion troca o tipo no próximo dado publicado |
| Pontos duplicados no STH | Mais de uma subscription para a mesma entidade. Remova as subscriptions na aba STH-Comet e crie de novo |
| "Device já existe, mas DIFERENTE da tela" | Outro aluno usa o mesmo nome e ID, ou você mudou atributos depois de provisionar. Escolha outro ID ou atualize o device |

## Limitações

- HTTP e MQTT sem TLS e sem autenticação, como o servidor didático.
- O publicador MQTT embutido usa QoS 0, sem retransmissão. Serve para teste, não para medir perda.
- As consultas de subscriptions e registrations leem até 1000 itens. Num servidor com mais que isso, o script pode não encontrar os itens do seu dispositivo.

## Licença

MIT. Veja [LICENSE](LICENSE).
