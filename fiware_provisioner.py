#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fiware_provisioner.py
Provisiona, consulta e simula um dispositivo no FIWARE. Baseado no tutorial
"Passo a Passo para configurar dispositivo no Fiware" e na colecao Postman
"FIWARE Descomplicado" usada em aula.

O aluno informa:
  - IP do servidor (VM)
  - Nome do dispositivo (vira o entity_type, ex.: device)
  - ID de 3 digitos (ex.: 001)
  - Atributos e comandos do seu IoT (ja vem com o exemplo do tutorial)

Exemplo do tutorial: nome=device, ID=001 gera
  device_id    device001
  entity_type  device
  entity_name  urn:ngsi-ld:device:001

Abas:
  Fluxo completo  provisiona / verifica / remove tudo de uma vez
  IoT Agent       pasta "IOT Agent MQTT" da colecao (itens 1.1, 2, 2.1, 3, 5, 9)
  Orion           pasta "Orion Context Broker" + itens 4, 6, 7, 8 e 10
  STH-Comet       pasta "STH-Comet" (health check, subscribe, serie temporal)
  Simulacao       envia dados de teste por MQTT (caminho real) ou direto no Orion

Requisitos: Python 3.8+ com Tkinter (Ubuntu/Debian: sudo apt install python3-tk)
Execucao:   python fiware_provisioner.py
"""

import json
import math
import queue
import random
import re
import socket
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext


# =============================================================================
# CONFIGURACAO DA TURMA (valores da colecao FIWARE Descomplicado)
# =============================================================================

SERVICE = "smart"                 # header fiware-service
SERVICEPATH = "/"                 # header fiware-servicepath
APIKEY = "TEF"                    # apikey do service group (topico MQTT /TEF/...)
RESOURCE = ""                     # resource do service group (vazio na colecao)
GROUP_ENTITY_TYPE = "Thing"       # entity_type do service group (colecao)
PROTOCOL = "PDI-IoTA-UltraLight"
TRANSPORT = "MQTT"

# Valores iniciais da tela = exemplo do tutorial. O aluno ajusta ao seu IoT.
DEFAULT_ATTRIBUTES = [
    ("s", "state", "Text"),
    ("p", "p", "Integer"),
]
DEFAULT_COMMANDS = "on, off"

ATTR_TYPES = ["Integer", "Float", "Number", "Text", "Boolean"]
NUMERIC_TYPES = ("Integer", "Float", "Number")   # esses vao para o STH-Comet

PORT_IOTA = 4041
PORT_ORION = 1026
PORT_STH = 8666
PORT_MQTT = 1883
HTTP_TIMEOUT = 8.0
LOG_LIMIT = 4000

NAME_RULE = re.compile(r"^[a-z][a-z0-9]{1,19}$")
ID_RULE = re.compile(r"^[0-9]{3}$")
TOKEN_RULE = re.compile(r"^[A-Za-z0-9_]{1,32}$")
FORBIDDEN_TEXT = set('|@#<>"\'=;()')     # reservados no UltraLight e no Orion

EXAMPLE_VALUE = {"Integer": "45", "Float": "25.5", "Number": "25.5",
                 "Text": "on", "Boolean": "true"}

# Formatos de publicacao MQTT (UltraLight 2.0 no IoT Agent)
SIM_FORMATS = [
    ("firmware", "Igual ao firmware: Text em /attrs, numericos em /attrs/<object_id>"),
    ("per_attr", "Um topico por atributo: /attrs/<object_id> com valor puro"),
    ("single", "Topico unico: /attrs com object_id|valor|object_id|valor"),
]
SIM_ALL = "(todos)"

# Atributos que o Orion cria sozinho para comandos e timestamp
def command_side_attrs(commands):
    out = {"TimeInstant"}
    for command in commands:
        out.add(command)
        out.add(command + "_status")
        out.add(command + "_info")
    return out


# =============================================================================
# HTTP
# =============================================================================

class NetworkError(Exception):
    """Host errado, container parado ou porta bloqueada no security group."""


def http(method, url, body=None, text=None):
    headers = {
        "fiware-service": SERVICE,
        "fiware-servicepath": SERVICEPATH,
        "Accept": "application/json",
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    elif text is not None:
        data = text.encode("utf-8")
        headers["Content-Type"] = "text/plain"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError) as exc:
        raise NetworkError("%s %s -> %s" % (method, url, exc))


def is_ok(status):
    return 200 <= status < 300


def parse_json(text):
    try:
        return json.loads(text)
    except ValueError:
        return None


def pretty(text):
    data = parse_json(text)
    if data is None:
        return text.strip()
    return json.dumps(data, indent=2, ensure_ascii=False)


def indent(text):
    return "\n".join("   " + line for line in text.splitlines())


# =============================================================================
# MQTT 3.1.1 minimo (CONNECT / PUBLISH QoS 0 / DISCONNECT), so stdlib
# =============================================================================

def _mqtt_string(value):
    raw = value.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


def _mqtt_remaining_length(size):
    out = bytearray()
    while True:
        byte = size % 128
        size = size // 128
        if size > 0:
            byte = byte | 0x80
        out.append(byte)
        if size == 0:
            break
    return bytes(out)


class MqttPublisher(object):
    """Suficiente para simular o ESP32 publicando. Sem TLS e sem usuario/senha."""

    def __init__(self, host, port, client_id, keepalive):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.keepalive = keepalive
        self.sock = None

    def connect(self):
        try:
            self.sock = socket.create_connection((self.host, self.port),
                                                 timeout=HTTP_TIMEOUT)
            variable = _mqtt_string("MQTT") + bytes([4, 0x02]) \
                + struct.pack("!H", self.keepalive)
            self._send(0x10, variable + _mqtt_string(self.client_id))
            ack = self._recv_exact(4)
        except OSError as exc:
            self.close()
            raise NetworkError("MQTT %s:%d -> %s" % (self.host, self.port, exc))
        if ack[0] != 0x20 or ack[3] != 0:
            self.close()
            raise NetworkError("Broker MQTT recusou a conexao (CONNACK codigo %d)."
                               % ack[3])

    def publish(self, topic, message):
        try:
            self._send(0x30, _mqtt_string(topic) + message.encode("utf-8"))
        except OSError as exc:
            raise NetworkError("MQTT publish %s -> %s" % (topic, exc))

    def close(self):
        """
        DISCONNECT + fechamento ordenado. Fechar o socket logo apos o ultimo PUBLISH
        faz o broker descartar mensagens ainda nao lidas (medido: ~20% de perda).
        """
        if self.sock is None:
            return
        try:
            self.sock.sendall(b"\xe0\x00")
            self.sock.shutdown(socket.SHUT_WR)
            self.sock.settimeout(2.0)
            while self.sock.recv(256):
                pass
        except OSError:
            pass
        self.sock.close()
        self.sock = None

    def _send(self, header, body):
        self.sock.sendall(bytes([header]) + _mqtt_remaining_length(len(body)) + body)

    def _recv_exact(self, size):
        data = b""
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise OSError("conexao fechada pelo broker")
            data += chunk
        return data


# =============================================================================
# Dispositivo: todos os JSONs do tutorial sao derivados daqui
# =============================================================================

class Device(object):

    def __init__(self, host, name, number, attributes, commands):
        self.host = host.strip()
        self.name = name.strip().lower()
        self.number = number.strip()
        self.attributes = attributes      # lista de (object_id, name, type)
        self.commands = commands          # lista de str

    def validate(self):
        problems = []
        if not self.host:
            problems.append("Informe o IP do servidor.")
        if not NAME_RULE.match(self.name):
            problems.append("Nome: 2 a 20 caracteres, letras minusculas e numeros, "
                            "comecando por letra (ex.: device).")
        if not ID_RULE.match(self.number):
            problems.append("ID: exatamente 3 digitos (ex.: 001).")
        if not self.attributes:
            problems.append("Defina pelo menos um atributo.")

        names = set()
        object_ids = set()
        for object_id, name, _attr_type in self.attributes:
            if not TOKEN_RULE.match(object_id) or not TOKEN_RULE.match(name):
                problems.append("Atributo '%s/%s': use so letras, numeros e _."
                                % (object_id, name))
            if name in names:
                problems.append("Atributo '%s' repetido." % name)
            if object_id in object_ids:
                problems.append("object_id '%s' repetido." % object_id)
            names.add(name)
            object_ids.add(object_id)

        for command in self.commands:
            if not TOKEN_RULE.match(command):
                problems.append("Comando '%s': use so letras, numeros e _." % command)
            if command in names:
                problems.append("Comando '%s' tem o mesmo nome de um atributo." % command)
        return problems

    # --- identificadores (padrao do tutorial) ------------------------------

    @property
    def device_id(self):
        return "%s%s" % (self.name, self.number)            # device001

    @property
    def entity_type(self):
        return self.name                                     # device

    @property
    def entity_id(self):
        return "urn:ngsi-ld:%s:%s" % (self.name, self.number)   # urn:ngsi-ld:device:001

    @property
    def attr_names(self):
        return [item[1] for item in self.attributes]

    @property
    def sth_attrs(self):
        return [name for _o, name, attr_type in self.attributes
                if attr_type in NUMERIC_TYPES]

    def attr_type(self, name):
        for _o, attr_name, attr_type in self.attributes:
            if attr_name == name:
                return attr_type
        return None

    # --- MQTT (para o firmware) --------------------------------------------

    @property
    def mqtt_topic(self):
        return "/%s/%s/attrs" % (APIKEY, self.device_id)

    @property
    def mqtt_payload(self):
        parts = []
        for object_id, _name, attr_type in self.attributes:
            parts.append("%s|%s" % (object_id, EXAMPLE_VALUE.get(attr_type, "0")))
        return "|".join(parts)

    def mqtt_attr_topic(self, object_id):
        return "%s/%s" % (self.mqtt_topic, object_id)

    def firmware_example(self):
        """Mensagens no estilo do Fiware_IoT_ESP32.ino, com valores de exemplo."""
        sample = []
        for object_id, name, attr_type in self.attributes:
            sample.append((object_id, name, attr_type, EXAMPLE_VALUE.get(attr_type, "0")))
        return build_mqtt_messages(self, sample, "firmware")

    @property
    def mqtt_cmd_topic(self):
        return "/%s/%s/cmd" % (APIKEY, self.device_id)

    @property
    def mqtt_cmd_example(self):
        return "%s@%s|" % (self.device_id, self.commands[0])

    # --- URLs --------------------------------------------------------------

    def iota(self, path):
        return "http://%s:%d%s" % (self.host, PORT_IOTA, path)

    def orion(self, path):
        return "http://%s:%d%s" % (self.host, PORT_ORION, path)

    def sth(self, path):
        return "http://%s:%d%s" % (self.host, PORT_STH, path)

    def sth_path(self, attr, last_n):
        return ("/STH/v1/contextEntities/type/%s/id/%s/attributes/%s?lastN=%d"
                % (self.entity_type, self.entity_id, attr, last_n))

    # --- payloads ----------------------------------------------------------

    def payload_service_group(self):
        """Colecao, item 2: Provisioning a Service Group for MQTT."""
        return {"services": [{
            "apikey": APIKEY,
            "cbroker": "http://%s:%d" % (self.host, PORT_ORION),
            "entity_type": GROUP_ENTITY_TYPE,
            "resource": RESOURCE,
        }]}

    def payload_device(self):
        """Tutorial, 1o passo / colecao, item 3: Provisioning a Smart Lamp."""
        device = {
            "device_id": self.device_id,
            "entity_name": self.entity_id,
            "entity_type": self.entity_type,
            "protocol": PROTOCOL,
            "transport": TRANSPORT,
        }
        if self.commands:
            device["commands"] = [{"name": c, "type": "command"} for c in self.commands]
        device["attributes"] = [{"object_id": o, "name": n, "type": t}
                                for o, n, t in self.attributes]
        return {"devices": [device]}

    def payload_device_update(self):
        """Corpo do PUT /iot/devices/<device_id> (atualiza atributos e comandos)."""
        body = {
            "attributes": [{"object_id": o, "name": n, "type": t}
                           for o, n, t in self.attributes],
        }
        if self.commands:
            body["commands"] = [{"name": c, "type": "command"} for c in self.commands]
        return body

    def payload_registration(self):
        """Tutorial, 2o passo / colecao, item 4: Registering Smart Lamp Commands."""
        return {
            "description": "%s Commands" % self.entity_type,
            "dataProvided": {
                "entities": [{"id": self.entity_id, "type": self.entity_type}],
                "attrs": list(self.commands),
            },
            "provider": {
                "http": {"url": "http://%s:%d" % (self.host, PORT_IOTA)},
                "legacyForwarding": True,
            },
        }

    def payload_subscription(self):
        """Tutorial, 6o passo / colecao STH-Comet, item 2: Subscribe."""
        return {
            "description": "Notify STH-Comet of %s changes" % self.device_id,
            "subject": {
                "entities": [{"id": self.entity_id, "type": self.entity_type}],
                "condition": {"attrs": self.sth_attrs},
            },
            "notification": {
                "http": {"url": "http://%s:%d/notify" % (self.host, PORT_STH)},
                "attrs": self.sth_attrs,
                "attrsFormat": "legacy",
            },
        }


# =============================================================================
# Simulacao
# =============================================================================

def make_sample(attributes, low, high, text_values):
    """Um valor aleatorio por atributo, respeitando o tipo declarado."""
    sample = []
    for object_id, name, attr_type in attributes:
        if attr_type == "Integer":
            lo = int(math.ceil(low))
            hi = int(math.floor(high))
            if hi < lo:
                hi = lo
            value = random.randint(lo, hi)
        elif attr_type in ("Float", "Number"):
            value = round(random.uniform(low, high), 2)
        elif attr_type == "Boolean":
            value = random.choice([True, False])
        else:
            value = random.choice(text_values)
        sample.append((object_id, name, attr_type, value))
    return sample


def ul_value(attr_type, value):
    if attr_type == "Boolean":
        if value is True or str(value).lower() == "true":
            return "true"
        return "false"
    return str(value)


def sample_to_ultralight(sample):
    parts = []
    for object_id, _name, attr_type, value in sample:
        parts.append("%s|%s" % (object_id, ul_value(attr_type, value)))
    return "|".join(parts)


def build_mqtt_messages(dev, sample, fmt):
    """
    Converte uma amostra em (topico, payload) conforme o formato:
      firmware  Text/Boolean juntos em /attrs ("s|on"); numericos em /attrs/<object_id>
      per_attr  cada atributo em /attrs/<object_id>, payload = valor puro
      single    tudo em /attrs ("s|on|p|45")
    O sufixo do topico e SEMPRE o object_id: e por ele que o IoT Agent acha o atributo.
    """
    if fmt == "single":
        return [(dev.mqtt_topic, sample_to_ultralight(sample))]
    messages = []
    grouped = []
    for item in sample:
        object_id, _name, attr_type, value = item
        if fmt == "firmware" and attr_type not in NUMERIC_TYPES:
            grouped.append(item)
        else:
            messages.append((dev.mqtt_attr_topic(object_id), ul_value(attr_type, value)))
    if grouped:
        messages.insert(0, (dev.mqtt_topic, sample_to_ultralight(grouped)))
    return messages


def sample_to_orion(sample):
    body = {}
    for _object_id, name, attr_type, value in sample:
        body[name] = {"type": attr_type, "value": value}
    return body


def to_text_plain(attr_type, raw):
    """Corpo text/plain do PUT .../attrs/<attr>/value (colecao Orion, itens 5 e 6)."""
    raw = raw.strip()
    if attr_type == "Integer":
        return str(int(raw))
    if attr_type in ("Float", "Number"):
        return repr(float(raw))
    if attr_type == "Boolean":
        lowered = raw.lower()
        if lowered not in ("true", "false"):
            raise ValueError("Boolean aceita apenas true ou false.")
        return lowered
    for char in raw:
        if char in FORBIDDEN_TEXT:
            raise ValueError("Caractere '%s' nao permitido em Text." % char)
    return json.dumps(raw)


# =============================================================================
# Operacoes
# =============================================================================

class Runner(object):

    def __init__(self, log):
        self.log = log

    def call(self, method, url, body=None, text=None, label=""):
        self.log("-> %s %s" % (method, url), "req")
        if body is not None:
            self.log(indent(json.dumps(body, indent=2, ensure_ascii=False)), "dim")
        if text is not None:
            self.log(indent("text/plain: %s" % text), "dim")
        status, response = http(method, url, body=body, text=text)
        tag = "err"
        if is_ok(status):
            tag = "ok"
        self.log("<- HTTP %d  %s" % (status, label), tag)
        shown = pretty(response)
        if len(shown) > LOG_LIMIT:
            shown = shown[:LOG_LIMIT] + "\n...[truncado]"
        if shown:
            self.log(indent(shown), "dim")
        return status, response

    def header(self, title):
        self.log("=" * 70, "head")
        self.log(title, "head")
        self.log("=" * 70, "head")

    def single(self, title, method, url, body=None, text=None, label=""):
        self.header(title)
        return self.call(method, url, body=body, text=text, label=label)

    def summary(self, results):
        self.log("")
        self.log("RESUMO", "head")
        for name, ok in results:
            if ok:
                self.log("  [OK   ] %s" % name, "ok")
            else:
                self.log("  [FALHA] %s" % name, "err")

    # --- busca -------------------------------------------------------------

    def find_subscriptions(self, dev):
        _status, text = self.call("GET", dev.orion("/v2/subscriptions?limit=1000"),
                                  label="lista subscriptions")
        items = parse_json(text)
        found = []
        if not isinstance(items, list):
            return found
        for item in items:
            for entity in item.get("subject", {}).get("entities", []):
                if entity.get("id") == dev.entity_id:
                    found.append(item.get("id"))
                    break
        return found

    def find_registration(self, dev):
        _status, text = self.call("GET", dev.orion("/v2/registrations?limit=1000"),
                                  label="lista registrations")
        items = parse_json(text)
        if not isinstance(items, list):
            return None
        for item in items:
            for entity in item.get("dataProvided", {}).get("entities", []):
                if entity.get("id") == dev.entity_id:
                    return item.get("id")
        return None

    def find_registrations(self, dev):
        """Todas as registrations da entidade: a do passo 2 do tutorial e a que o
        proprio IoT Agent cria ao provisionar um device com comandos."""
        _status, text = self.call("GET", dev.orion("/v2/registrations?limit=1000"),
                                  label="lista registrations")
        items = parse_json(text)
        found = []
        if not isinstance(items, list):
            return found
        for item in items:
            for entity in item.get("dataProvided", {}).get("entities", []):
                if entity.get("id") == dev.entity_id:
                    found.append(item.get("id"))
                    break
        return found

    def delete_device_iota(self, dev):
        """
        DELETE no IoT Agent + prova de que o device sumiu de verdade.
        Retorna True so se, alguns segundos depois, o GET der 404.
        """
        url = dev.iota("/iot/devices/%s" % dev.device_id)
        status, _t = self.call("DELETE", url, label="Delete in IoT Agent")
        if status == 404:
            self.log("Device ja nao existia no IoT Agent.", "info")
            return True
        if not is_ok(status):
            self.log("O IoT Agent recusou a remocao. Ao remover um device com comandos, "
                     "ele tenta apagar no Orion a registration que ele mesmo criou; se "
                     "ela ja foi apagada a mao, a remocao pode falhar. Veja a mensagem "
                     "de erro acima.", "info")
            return False

        self.log("Aguardando 3 s para confirmar que o device nao volta...", "dim")
        time.sleep(3.0)
        status, text = self.call("GET", url, label="confirma remocao")
        if status == 404:
            self.log("Removido de verdade.", "ok")
            return True
        entity = ""
        data = parse_json(text)
        if isinstance(data, dict):
            entity = "%s (tipo %s)" % (data.get("entity_name"), data.get("entity_type"))
        self.log("O device VOLTOU: %s." % entity, "err")
        self.log("Causa: autoprovisionamento. Algo continua publicando em /%s/%s/... "
                 "(ESP32 ligado ou simulacao rodando) e o service group recria o device "
                 "sozinho a cada mensagem. Desligue o ESP32 / pare a simulacao e remova "
                 "de novo." % (APIKEY, dev.device_id), "info")
        return False

    # --- diagnostico de tipos -------------------------------------------

    def device_diff(self, dev, remote):
        """Diferencas entre o device no IoT Agent e o que esta na tela."""
        problems = []
        if not isinstance(remote, dict):
            return problems
        remote_attrs = {}
        for attr in remote.get("attributes", []):
            key = attr.get("object_id") or attr.get("name")
            remote_attrs[key] = (attr.get("name"), attr.get("type"))
        local_attrs = {}
        for object_id, name, attr_type in dev.attributes:
            local_attrs[object_id] = (name, attr_type)

        for object_id, (name, attr_type) in local_attrs.items():
            if object_id not in remote_attrs:
                problems.append("IoT Agent nao tem o atributo %s/%s (%s)."
                                % (object_id, name, attr_type))
                continue
            remote_name, remote_type = remote_attrs[object_id]
            if (remote_name, remote_type) != (name, attr_type):
                problems.append("object_id '%s': IoT Agent tem %s/%s, tela tem %s/%s."
                                % (object_id, remote_name, remote_type, name, attr_type))
        for object_id, (name, attr_type) in remote_attrs.items():
            if object_id not in local_attrs:
                problems.append("IoT Agent tem %s/%s (%s), que nao esta na tela."
                                % (object_id, name, attr_type))

        remote_cmds = set(c.get("name") for c in remote.get("commands", []))
        if remote_cmds != set(dev.commands):
            problems.append("Comandos: IoT Agent tem %s, tela tem %s."
                            % (sorted(remote_cmds), sorted(dev.commands)))
        return problems

    def entity_type_problems(self, dev, entity):
        """Confere os tipos que efetivamente chegaram no Orion."""
        problems = []
        notes = []
        if not isinstance(entity, dict):
            return problems, notes
        expected = {}
        for object_id, name, attr_type in dev.attributes:
            expected[name] = (object_id, attr_type)
        object_ids = set(item[0] for item in dev.attributes)
        ignore = command_side_attrs(dev.commands)

        for key, attr in entity.items():
            if key in ("id", "type") or key in ignore or not isinstance(attr, dict):
                continue
            got_type = attr.get("type")
            if key in expected:
                object_id, want_type = expected[key]
                if got_type != want_type:
                    problems.append("Orion: '%s' esta como %s, esperado %s. Corrige no "
                                    "proximo dado publicado depois do device atualizado."
                                    % (key, got_type, want_type))
                elif want_type in NUMERIC_TYPES and isinstance(attr.get("value"), str):
                    notes.append("Orion: '%s' e %s mas o valor chega como string (%r). "
                                 "Isso e o IoT Agent sem autocast, nao erro de tipo."
                                 % (key, want_type, attr.get("value")))
            else:
                hint = ""
                if key not in object_ids:
                    hint = (" O topico /attrs/%s nao corresponde a nenhum object_id: o "
                            "IoT Agent cria um atributo novo com tipo padrao." % key)
                problems.append("Orion tem '%s' (%s), que nao foi declarado.%s"
                                % (key, got_type, hint))
        return problems, notes

    # --- passos idempotentes ----------------------------------------------

    def ensure_service_group(self, dev):
        _status, text = self.call("GET", dev.iota("/iot/services"),
                                  label="lista service groups")
        data = parse_json(text)
        groups = []
        if isinstance(data, dict):
            groups = data.get("services", [])
        for group in groups:
            if group.get("apikey") == APIKEY:
                self.log("Service group com apikey=%s ja existe. Nada a fazer."
                         % APIKEY, "info")
                return True
        self.log("Nenhum service group com apikey=%s. Criando." % APIKEY, "info")
        status, _text = self.call("POST", dev.iota("/iot/services"),
                                  dev.payload_service_group(), label="cria service group")
        # 409 = outro aluno criou no mesmo instante: tambem serve
        return is_ok(status) or status == 409

    def ensure_device(self, dev):
        status, text = self.call("GET", dev.iota("/iot/devices/%s" % dev.device_id),
                                 label="consulta device")
        if status == 200:
            diff = self.device_diff(dev, parse_json(text))
            if not diff:
                self.log("Device %s ja existe com os mesmos atributos e tipos. "
                         "Nada a fazer." % dev.device_id, "info")
                return True
            self.log("Device %s ja existe, mas DIFERENTE da tela:" % dev.device_id, "err")
            for line in diff:
                self.log("  - " + line, "err")
            self.log("Os tipos novos NAO foram aplicados. Use 'Atualizar meu device' na "
                     "aba IoT Agent. Se esse device nao e seu, escolha outro ID.", "info")
            return False
        status, _text = self.call("POST", dev.iota("/iot/devices"),
                                  dev.payload_device(), label="Provisioning")
        return is_ok(status)

    def ensure_registration(self, dev):
        if not dev.commands:
            self.log("Sem comandos: registration nao e necessaria.", "info")
            return True
        found = self.find_registration(dev)
        if found:
            self.log("Registration ja existe (id=%s)." % found, "info")
            return True
        status, _text = self.call("POST", dev.orion("/v2/registrations"),
                                  dev.payload_registration(), label="Registering Commands")
        return is_ok(status)

    def ensure_subscription(self, dev):
        if not dev.sth_attrs:
            self.log("Nenhum atributo numerico: subscription do STH nao criada.", "info")
            return True
        found = self.find_subscriptions(dev)
        if found:
            self.log("Subscription ja existe (ids=%s). Nao crio outra para o STH "
                     "nao gravar pontos duplicados." % ", ".join(found), "info")
            return True
        status, _text = self.call("POST", dev.orion("/v2/subscriptions"),
                                  dev.payload_subscription(), label="Subscribe STH-Comet")
        return is_ok(status)

    # --- fluxo completo ----------------------------------------------------

    def provision(self, dev):
        self.header("PROVISIONAMENTO  %s  ->  %s" % (dev.device_id, dev.entity_id))
        steps = [
            ("Service group", self.ensure_service_group),
            ("Provisioning (IoT Agent)", self.ensure_device),
            ("Registering Commands (Orion)", self.ensure_registration),
            ("Subscribe (STH-Comet)", self.ensure_subscription),
        ]
        results = []
        for name, func in steps:
            self.log("")
            self.log("--- %s ---" % name, "head")
            ok = func(dev)
            results.append((name, ok))
            if not ok:
                self.log("Passo '%s' falhou. Parando aqui." % name, "err")
                break
        self.summary(results)
        self.log("")
        self.log("Firmware do ESP32 (UltraLight, mesmo padrao do Fiware_IoT_ESP32.ino):", "info")
        for topic, payload in dev.firmware_example():
            self.log("  publicar em  %-28s payload: %s" % (topic, payload), "info")
        if dev.commands:
            self.log("  assinar      %s   recebe: %s" % (dev.mqtt_cmd_topic,
                                                         dev.mqtt_cmd_example), "info")

    def check(self, dev):
        self.header("VERIFICACAO  %s" % dev.device_id)
        results = []

        status, text = self.call("GET", dev.iota("/iot/devices/%s" % dev.device_id),
                                 label="device no IoT Agent")
        results.append(("Device no IoT Agent", status == 200))
        if status == 200:
            diff = self.device_diff(dev, parse_json(text))
            results.append(("Atributos/tipos do IoT Agent = tela", not diff))
            for line in diff:
                self.log("  - " + line, "err")

        status, text = self.call("GET", dev.orion("/v2/entities/%s" % dev.entity_id),
                                 label="entidade no Orion")
        results.append(("Entidade no Orion", status == 200))
        if status == 200:
            problems, notes = self.entity_type_problems(dev, parse_json(text))
            results.append(("Tipos no Orion = tela", not problems))
            for line in problems:
                self.log("  - " + line, "err")
            for line in notes:
                self.log("  - " + line, "info")

        subs = self.find_subscriptions(dev)
        results.append(("Subscription ativa", len(subs) > 0))
        if len(subs) > 1:
            self.log("ATENCAO: %d subscriptions para a mesma entidade. Remova as "
                     "subscriptions na aba STH-Comet e crie de novo." % len(subs), "err")

        if dev.sth_attrs:
            attr = dev.sth_attrs[0]
            _status, text = self.call("GET", dev.sth(dev.sth_path(attr, 1)),
                                      label="historico no STH")
            has_data = False
            data = parse_json(text)
            if isinstance(data, dict):
                for item in data.get("contextResponses", []):
                    for block in item.get("contextElement", {}).get("attributes", []):
                        if block.get("values"):
                            has_data = True
            results.append(("Dado historico no STH (%s)" % attr, has_data))
            if not has_data:
                oid = dev.attributes[dev.attr_names.index(attr)][0]
                self.log("Sem dado no STH. Publique em %s (ESP32 ou aba Simulacao)."
                         % dev.mqtt_attr_topic(oid), "info")
        self.summary(results)

    def teardown(self, dev):
        self.header("REMOCAO  %s" % dev.device_id)
        results = []
        results.extend(self.delete_subscriptions(dev))

        # Ordem importa: o IoT Agent remove a registration dele no Orion durante o
        # DELETE. Apagar as registrations antes faz o DELETE do device falhar.
        results.append(("Device no IoT Agent", self.delete_device_iota(dev)))

        for reg_id in self.find_registrations(dev):
            status, _t = self.call("DELETE", dev.orion("/v2/registrations/%s" % reg_id),
                                   label="remove registration")
            results.append(("Registration %s" % reg_id, is_ok(status) or status == 404))

        status, _t = self.call("DELETE", dev.orion("/v2/entities/%s" % dev.entity_id),
                               label="Delete in Orion")
        results.append(("Entidade no Orion", is_ok(status) or status == 404))

        self.log("O service group NAO e removido: ele e compartilhado pela turma.", "info")
        self.summary(results)

    # --- IoT Agent (pasta "IOT Agent MQTT") -------------------------------

    def iota_health(self, dev):
        self.single("IoT AGENT  1.1 Health Check", "GET", dev.iota("/iot/about"))

    def iota_list_groups(self, dev):
        self.single("IoT AGENT  2.1 Health Check Services", "GET", dev.iota("/iot/services"))

    def iota_create_group(self, dev):
        self.header("IoT AGENT  2. Provisioning a Service Group for MQTT")
        self.ensure_service_group(dev)

    def iota_delete_group(self, dev):
        query = urllib.parse.urlencode({"resource": RESOURCE, "apikey": APIKEY})
        self.single("IoT AGENT  2.1 Delete a Service Group", "DELETE",
                    dev.iota("/iot/services/?" + query))

    def iota_create_device(self, dev):
        self.header("IoT AGENT  3. Provisioning  %s" % dev.device_id)
        self.ensure_device(dev)

    def iota_update_device(self, dev):
        self.header("IoT AGENT  atualizar %s (PUT)" % dev.device_id)
        url = dev.iota("/iot/devices/%s" % dev.device_id)
        status, text = self.call("GET", url, label="consulta device")
        if status != 200:
            self.log("Device nao existe. Use '3. Provisionar device'.", "err")
            return
        before = parse_json(text)
        diff = self.device_diff(dev, before)
        if not diff:
            self.log("IoT Agent ja esta igual a tela. Nada a fazer.", "info")
            return
        for line in diff:
            self.log("  - " + line, "info")
        status, _t = self.call("PUT", url, dev.payload_device_update(), label="atualiza device")
        if not is_ok(status):
            return
        self.log("Device atualizado. O Orion so troca o tipo quando chegar o proximo dado.",
                 "ok")
        old_numeric = set()
        if isinstance(before, dict):
            for attr in before.get("attributes", []):
                if attr.get("type") in NUMERIC_TYPES:
                    old_numeric.add(attr.get("name"))
        if old_numeric != set(dev.sth_attrs):
            self.log("Os atributos numericos mudaram: remova e recrie a subscription na "
                     "aba STH-Comet.", "info")
        old_cmds = set(c.get("name") for c in before.get("commands", []))
        if old_cmds != set(dev.commands):
            self.log("Os comandos mudaram: remova a entidade/registration e registre de novo "
                     "na aba Orion.", "info")

    def iota_list_devices(self, dev):
        self.single("IoT AGENT  5. List all Devices Provisioned", "GET",
                    dev.iota("/iot/devices"))

    def iota_get_device(self, dev):
        self.single("IoT AGENT  consulta %s" % dev.device_id, "GET",
                    dev.iota("/iot/devices/%s" % dev.device_id))

    def iota_delete_device(self, dev):
        self.header("IoT AGENT  9. Delete in IoT Agent  %s" % dev.device_id)
        self.delete_device_iota(dev)

    # --- Orion (pasta "Orion Context Broker") -----------------------------

    def orion_version(self, dev):
        self.single("ORION  1. Version", "GET", dev.orion("/version"))

    def orion_list_entities(self, dev):
        self.single("ORION  3. Get (todas as entidades do servicepath)", "GET",
                    dev.orion("/v2/entities?limit=100"))

    def orion_get_entity(self, dev):
        self.single("ORION  entidade %s" % dev.entity_id, "GET",
                    dev.orion("/v2/entities/%s" % dev.entity_id))

    def orion_read_attr(self, dev, attr):
        self.single("ORION  7/8. Result of %s" % attr, "GET",
                    dev.orion("/v2/entities/%s/attrs/%s" % (dev.entity_id, attr)))

    def orion_update_attr(self, dev, attr, plain):
        status, _t = self.single("ORION  5/6. Selective update %s" % attr, "PUT",
                                 dev.orion("/v2/entities/%s/attrs/%s/value"
                                           % (dev.entity_id, attr)), text=plain)
        if is_ok(status) and attr in dev.sth_attrs:
            self.log("Atributo com subscription: o STH-Comet tambem recebeu esse valor.",
                     "info")

    def orion_register_commands(self, dev):
        self.header("ORION  4. Registering Commands")
        self.ensure_registration(dev)

    def orion_list_registrations(self, dev):
        self.single("ORION  registrations do servicepath", "GET",
                    dev.orion("/v2/registrations?limit=100"))

    def orion_send_command(self, dev, command):
        body = {command: {"type": "command", "value": ""}}
        status, _t = self.single("ORION  6. Switching (comando '%s')" % command, "PATCH",
                                 dev.orion("/v2/entities/%s/attrs" % dev.entity_id),
                                 body=body)
        if is_ok(status):
            self.log("O IoT Agent publica %s -> %s@%s|" % (dev.mqtt_cmd_topic,
                                                          dev.device_id, command), "info")
            self.log("Se ninguem responder em /cmdexe, %s_status fica PENDING."
                     % command, "info")

    def orion_delete_entity(self, dev):
        self.single("ORION  10. Delete in Orion  %s" % dev.entity_id, "DELETE",
                    dev.orion("/v2/entities/%s" % dev.entity_id))

    # --- STH-Comet (pasta "STH-Comet") ------------------------------------

    def sth_version(self, dev):
        self.single("STH-COMET  1. Health Check", "GET", dev.sth("/version"))

    def sth_subscribe(self, dev):
        self.header("STH-COMET  2. Subscribe %s" % ", ".join(dev.sth_attrs))
        self.ensure_subscription(dev)

    def sth_list_subscriptions(self, dev):
        self.header("STH-COMET  subscriptions de %s" % dev.entity_id)
        found = self.find_subscriptions(dev)
        if found:
            self.log("Subscriptions desta entidade: %s" % ", ".join(found), "info")
        else:
            self.log("Nenhuma subscription para %s." % dev.entity_id, "info")
        if len(found) > 1:
            self.log("Mais de uma: o STH grava cada ponto %d vezes." % len(found), "err")

    def sth_delete_subscriptions_flow(self, dev):
        self.header("STH-COMET  remove subscriptions de %s" % dev.entity_id)
        self.summary(self.delete_subscriptions(dev))

    def delete_subscriptions(self, dev):
        results = []
        for sub_id in self.find_subscriptions(dev):
            status, _t = self.call("DELETE", dev.orion("/v2/subscriptions/%s" % sub_id),
                                   label="remove subscription")
            results.append(("Subscription %s" % sub_id, is_ok(status)))
        return results

    def sth_history(self, dev, attr, last_n):
        self.single("STH-COMET  3. Request %s  lastN=%d" % (attr, last_n), "GET",
                    dev.sth(dev.sth_path(attr, last_n)))

    # --- simulacao ---------------------------------------------------------

    def simulate(self, dev, cfg, stop):
        mode = cfg["mode"]
        count = cfg["count"]
        attributes = dev.attributes
        if cfg["only"] != SIM_ALL:
            attributes = [a for a in dev.attributes if a[1] == cfg["only"]]
        self.header("SIMULACAO  %s  modo=%s  %d amostra(s) a cada %.1f s"
                    % (dev.device_id, mode, count, cfg["interval"]))
        self.log("Atributos simulados: %s" % ", ".join("%s/%s (%s)" % a for a in attributes),
                 "dim")

        publisher = None
        if mode == "mqtt":
            keepalive = min(65535, int(cfg["interval"] * 3) + 30)
            publisher = MqttPublisher(dev.host, cfg["port"], "sim_%s" % dev.device_id,
                                      keepalive)
            publisher.connect()
            self.log("Conectado ao broker %s:%d. Formato: %s"
                     % (dev.host, cfg["port"], cfg["format_label"]), "ok")
            self.log("Caminho: MQTT -> IoT Agent -> Orion -> STH-Comet", "dim")
        else:
            self.log("Caminho: HTTP -> Orion -> STH-Comet (IoT Agent NAO participa)", "dim")

        sent = 0
        try:
            for index in range(count):
                if stop.is_set():
                    self.log("Simulacao interrompida pelo usuario.", "info")
                    break
                sample = make_sample(attributes, cfg["low"], cfg["high"], cfg["texts"])
                tag_index = "[%d/%d]" % (index + 1, count)
                if publisher is not None:
                    for topic, payload in build_mqtt_messages(dev, sample, cfg["format"]):
                        publisher.publish(topic, payload)
                        self.log("%s MQTT %-32s %s" % (tag_index, topic, payload), "ok")
                else:
                    body = sample_to_orion(sample)
                    url = dev.orion("/v2/entities/%s/attrs" % dev.entity_id)
                    status, response = http("POST", url, body=body)
                    compact = json.dumps({k: v["value"] for k, v in body.items()})
                    if is_ok(status):
                        self.log("%s POST Orion  HTTP %d  %s" % (tag_index, status, compact),
                                 "ok")
                    else:
                        self.log("%s POST Orion  HTTP %d" % (tag_index, status), "err")
                        self.log(indent(pretty(response)), "dim")
                        if status == 404:
                            self.log("Entidade nao existe no Orion. Provisione antes ou "
                                     "envie uma amostra por MQTT.", "info")
                        break
                sent += 1
                if index < count - 1:
                    stop.wait(cfg["interval"])
        finally:
            if publisher is not None:
                publisher.close()

        self.log("")
        self.log("%d amostra(s) enviada(s)." % sent, "head")
        if sent:
            self.log("Use 'Verificar' para conferir se os tipos chegaram certos no Orion.",
                     "info")


# =============================================================================
# Interface grafica
# =============================================================================

class App(object):

    def __init__(self, root):
        self.root = root
        self.root.title("FIWARE - Provisionamento, Componentes e Simulacao")
        self.root.geometry("1320x800")
        self.root.minsize(1100, 700)

        self.queue = queue.Queue()
        self.busy = False
        self.buttons = []
        self.stop_event = threading.Event()

        self.var_host = tk.StringVar()
        self.var_name = tk.StringVar(value="device")
        self.var_number = tk.StringVar()
        self.var_commands = tk.StringVar(value=DEFAULT_COMMANDS)
        self.var_new_object = tk.StringVar()
        self.var_new_name = tk.StringVar()
        self.var_new_type = tk.StringVar(value=ATTR_TYPES[0])

        self.var_orion_attr = tk.StringVar()
        self.var_orion_value = tk.StringVar()
        self.var_command = tk.StringVar()
        self.var_sth_attr = tk.StringVar()
        self.var_lastn = tk.StringVar(value="30")

        self.var_sim_mode = tk.StringVar(value="mqtt")
        self.var_sim_port = tk.StringVar(value=str(PORT_MQTT))
        self.var_sim_format = tk.StringVar(value=SIM_FORMATS[0][1])
        self.var_sim_only = tk.StringVar(value=SIM_ALL)
        self.var_sim_low = tk.StringVar(value="0")
        self.var_sim_high = tk.StringVar(value="100")
        self.var_sim_text = tk.StringVar(value="on, off")
        self.var_sim_interval = tk.StringVar(value="2")
        self.var_sim_count = tk.StringVar(value="30")

        self._build()
        for object_id, name, attr_type in DEFAULT_ATTRIBUTES:
            self.tree.insert("", tk.END, values=(object_id, name, attr_type))
        for var in (self.var_host, self.var_name, self.var_number, self.var_commands):
            var.trace_add("write", self._on_change)
        self._on_change()
        self.root.after(80, self._drain_queue)

        self.log_line("Preencha IP, nome e ID, ajuste os atributos e use as abas.", "info")

    # --- layout geral ------------------------------------------------------

    def _build(self):
        left = ttk.Frame(self.root, padding=8)
        left.pack(side=tk.LEFT, fill=tk.Y)
        right = ttk.Frame(self.root, padding=(0, 8, 8, 8))
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._build_device(left)

        notebook = ttk.Notebook(right)
        notebook.pack(fill=tk.X)
        self._build_tab_flow(self._tab(notebook, "Fluxo completo"))
        self._build_tab_iota(self._tab(notebook, "IoT Agent :4041"))
        self._build_tab_orion(self._tab(notebook, "Orion :1026"))
        self._build_tab_sth(self._tab(notebook, "STH-Comet :8666"))
        self._build_tab_sim(self._tab(notebook, "Simulacao"))

        self.status = ttk.Label(right, text="pronto")
        self.status.pack(fill=tk.X, pady=(6, 2))

        self.log = scrolledtext.ScrolledText(right, wrap=tk.NONE, font=("Consolas", 9),
                                             background="#111418", foreground="#d6dae0")
        self.log.pack(fill=tk.BOTH, expand=True)
        colors = {"ok": "#6fcf6f", "err": "#ff7b72", "req": "#7fb3ff",
                  "info": "#f0c674", "head": "#ffffff", "dim": "#8b949e"}
        for tag, color in colors.items():
            self.log.tag_config(tag, foreground=color)
        self.log.configure(state=tk.DISABLED)

        bottom = ttk.Frame(right)
        bottom.pack(fill=tk.X, pady=(4, 0))
        ttk.Button(bottom, text="Limpar log", command=self.clear_log).pack(side=tk.RIGHT)

    def _tab(self, notebook, title):
        frame = ttk.Frame(notebook, padding=8)
        notebook.add(frame, text=title)
        return frame

    def _build_device(self, parent):
        form = ttk.LabelFrame(parent, text="Dispositivo", padding=6)
        form.pack(fill=tk.X)
        self._field(form, "IP do servidor", self.var_host, 0, "ex.: 54.12.34.56")
        self._field(form, "Nome (entity_type)", self.var_name, 1, "ex.: device")
        self._field(form, "ID (3 digitos)", self.var_number, 2, "ex.: 001")

        attrs = ttk.LabelFrame(parent, text="Atributos do seu IoT "
                               "(numericos vao para o STH-Comet)", padding=6)
        attrs.pack(fill=tk.X, pady=(8, 0))

        self.tree = ttk.Treeview(attrs, columns=("object_id", "name", "type"),
                                 show="headings", height=5)
        for col, width in (("object_id", 90), ("name", 150), ("type", 90)):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor="w")
        self.tree.pack(fill=tk.X)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

        editor = ttk.Frame(attrs)
        editor.pack(fill=tk.X, pady=(4, 0))
        ttk.Label(editor, text="object_id").grid(row=0, column=0, sticky="w")
        ttk.Label(editor, text="name").grid(row=0, column=1, sticky="w")
        ttk.Label(editor, text="type").grid(row=0, column=2, sticky="w")
        ttk.Entry(editor, textvariable=self.var_new_object, width=9).grid(row=1, column=0, padx=2)
        ttk.Entry(editor, textvariable=self.var_new_name, width=16).grid(row=1, column=1, padx=2)
        type_box = ttk.Combobox(editor, textvariable=self.var_new_type, values=ATTR_TYPES,
                                state="readonly", width=9)
        type_box.grid(row=1, column=2, padx=2)
        type_box.bind("<<ComboboxSelected>>", self._on_type_selected)
        ttk.Button(editor, text="Salvar", command=self.add_attribute).grid(row=1, column=3, padx=2)
        ttk.Button(editor, text="Remover", command=self.remove_attribute).grid(row=1, column=4, padx=2)

        cmds = ttk.Frame(attrs)
        cmds.pack(fill=tk.X, pady=(6, 0))
        ttk.Label(cmds, text="Comandos (virgula)").pack(side=tk.LEFT)
        ttk.Entry(cmds, textvariable=self.var_commands).pack(side=tk.LEFT, fill=tk.X,
                                                             expand=True, padx=4)

        info = ttk.LabelFrame(parent, text="Gerado automaticamente (use no firmware)",
                              padding=6)
        info.pack(fill=tk.X, pady=8)
        self.preview = tk.Text(info, height=8, width=60, font=("Consolas", 9),
                               relief=tk.FLAT, background="#f4f4f4")
        self.preview.pack(fill=tk.X)

    # --- abas --------------------------------------------------------------

    def _build_tab_flow(self, tab):
        ttk.Label(tab, text="Executa os passos do tutorial em sequencia. Pode rodar mais "
                  "de uma vez: nada e duplicado.").grid(row=0, column=0, columnspan=3,
                                                        sticky="w", pady=(0, 6))
        self._button(tab, "Provisionar tudo", self.on_provision, 1, 0)
        self._button(tab, "Verificar", self.on_check, 1, 1)
        self._button(tab, "Remover dispositivo", self.on_teardown, 1, 2)

    def _build_tab_iota(self, tab):
        self._button(tab, "1.1 Health Check", lambda: self.run_simple("iota_health"), 0, 0)
        self._button(tab, "2.1 Listar service groups",
                     lambda: self.run_simple("iota_list_groups"), 0, 1)
        self._button(tab, "2. Criar service group",
                     lambda: self.run_device("iota_create_group"), 0, 2)
        self._button(tab, "3. Provisionar device",
                     lambda: self.run_device("iota_create_device"), 1, 0)
        self._button(tab, "5. Listar todos os devices",
                     lambda: self.run_simple("iota_list_devices"), 1, 1)
        self._button(tab, "Consultar meu device",
                     lambda: self.run_device("iota_get_device"), 1, 2)
        self._button(tab, "9. Remover meu device", self.on_delete_device, 2, 0)
        self._button(tab, "Atualizar meu device (tipos)",
                     lambda: self.run_device("iota_update_device"), 2, 1)
        self._button(tab, "2.1 Remover service group", self.on_delete_group, 2, 2)

    def _build_tab_orion(self, tab):
        self._button(tab, "1. Version", lambda: self.run_simple("orion_version"), 0, 0)
        self._button(tab, "3. Listar entidades",
                     lambda: self.run_simple("orion_list_entities"), 0, 1)
        self._button(tab, "Consultar minha entidade",
                     lambda: self.run_device("orion_get_entity"), 0, 2)

        row = ttk.Frame(tab)
        row.grid(row=1, column=0, columnspan=4, sticky="w", pady=(8, 0))
        ttk.Label(row, text="atributo").pack(side=tk.LEFT)
        self.box_orion_attr = ttk.Combobox(row, textvariable=self.var_orion_attr,
                                           state="readonly", width=14)
        self.box_orion_attr.pack(side=tk.LEFT, padx=4)
        self._pack_button(row, "7/8. Ler valor", self.on_orion_read)
        ttk.Label(row, text="novo valor").pack(side=tk.LEFT, padx=(12, 2))
        ttk.Entry(row, textvariable=self.var_orion_value, width=10).pack(side=tk.LEFT)
        self._pack_button(row, "5/6. Atualizar valor", self.on_orion_update)

        row = ttk.Frame(tab)
        row.grid(row=2, column=0, columnspan=4, sticky="w", pady=(8, 0))
        self._pack_button(row, "4. Registrar comandos",
                          lambda: self.run_device("orion_register_commands"))
        self._pack_button(row, "Listar registrations",
                          lambda: self.run_simple("orion_list_registrations"))
        ttk.Label(row, text="comando").pack(side=tk.LEFT, padx=(12, 2))
        self.box_command = ttk.Combobox(row, textvariable=self.var_command,
                                        state="readonly", width=10)
        self.box_command.pack(side=tk.LEFT)
        self._pack_button(row, "6. Enviar comando", self.on_send_command)

        self._button(tab, "10. Remover minha entidade", self.on_delete_entity, 3, 0)

    def _build_tab_sth(self, tab):
        self._button(tab, "1. Health Check", lambda: self.run_simple("sth_version"), 0, 0)
        self._button(tab, "2. Criar subscription",
                     lambda: self.run_device("sth_subscribe"), 0, 1)
        self._button(tab, "Listar minhas subscriptions",
                     lambda: self.run_device("sth_list_subscriptions"), 0, 2)

        row = ttk.Frame(tab)
        row.grid(row=1, column=0, columnspan=4, sticky="w", pady=(8, 0))
        ttk.Label(row, text="atributo").pack(side=tk.LEFT)
        self.box_sth_attr = ttk.Combobox(row, textvariable=self.var_sth_attr,
                                         state="readonly", width=14)
        self.box_sth_attr.pack(side=tk.LEFT, padx=4)
        ttk.Label(row, text="lastN").pack(side=tk.LEFT, padx=(8, 2))
        ttk.Spinbox(row, textvariable=self.var_lastn, from_=1, to=100,
                    width=5).pack(side=tk.LEFT)
        self._pack_button(row, "3. Serie temporal", self.on_history)

        self._button(tab, "Remover minhas subscriptions", self.on_delete_subscriptions, 2, 0)

    def _build_tab_sim(self, tab):
        modes = ttk.Frame(tab)
        modes.grid(row=0, column=0, sticky="w")
        ttk.Radiobutton(modes, text="MQTT (simula o ESP32)",
                        variable=self.var_sim_mode, value="mqtt").pack(side=tk.LEFT)
        ttk.Label(modes, text="porta").pack(side=tk.LEFT, padx=(6, 2))
        ttk.Entry(modes, textvariable=self.var_sim_port, width=6).pack(side=tk.LEFT)
        ttk.Radiobutton(modes, text="Orion direto (sem IoT Agent)",
                        variable=self.var_sim_mode, value="orion").pack(side=tk.LEFT,
                                                                       padx=(16, 0))

        fmt = ttk.Frame(tab)
        fmt.grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Label(fmt, text="topico").pack(side=tk.LEFT, padx=(0, 2))
        ttk.Combobox(fmt, textvariable=self.var_sim_format, state="readonly", width=62,
                     values=[label for _key, label in SIM_FORMATS]).pack(side=tk.LEFT)

        only = ttk.Frame(tab)
        only.grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Label(only, text="atributo").pack(side=tk.LEFT, padx=(0, 2))
        self.box_sim_only = ttk.Combobox(only, textvariable=self.var_sim_only,
                                         state="readonly", width=14)
        self.box_sim_only.pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(only, text="numericos: min").pack(side=tk.LEFT, padx=(0, 2))
        ttk.Entry(only, textvariable=self.var_sim_low, width=7).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(only, text="max").pack(side=tk.LEFT, padx=(0, 2))
        ttk.Entry(only, textvariable=self.var_sim_high, width=7).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(only, text="Text (virgula)").pack(side=tk.LEFT, padx=(0, 2))
        ttk.Entry(only, textvariable=self.var_sim_text, width=10).pack(side=tk.LEFT)

        self._sim_row(tab, 3, [("intervalo (s)", self.var_sim_interval, 6),
                               ("amostras", self.var_sim_count, 6)])

        actions = ttk.Frame(tab)
        actions.grid(row=4, column=0, sticky="w", pady=(8, 0))
        self._pack_button(actions, "Enviar 1 amostra", lambda: self.on_simulate(True))
        self._pack_button(actions, "Iniciar simulacao", lambda: self.on_simulate(False))
        ttk.Button(actions, text="Parar", command=self.on_stop).pack(side=tk.LEFT, padx=2)
        ttk.Label(actions, text="sufixo = object_id, nunca o name",
                  foreground="#888").pack(side=tk.LEFT, padx=12)

    def _sim_row(self, tab, row, pairs):
        frame = ttk.Frame(tab)
        frame.grid(row=row, column=0, sticky="w", pady=(6, 0))
        for label, var, width in pairs:
            ttk.Label(frame, text=label).pack(side=tk.LEFT, padx=(0, 2))
            ttk.Entry(frame, textvariable=var, width=width).pack(side=tk.LEFT, padx=(0, 12))

    # --- widgets auxiliares ------------------------------------------------

    def _field(self, parent, label, var, row, hint):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=4, pady=3)
        ttk.Entry(parent, textvariable=var, width=22).grid(row=row, column=1, padx=4, pady=3)
        ttk.Label(parent, text=hint, foreground="#888").grid(row=row, column=2, sticky="w")

    def _button(self, parent, text, command, row, col):
        button = ttk.Button(parent, text=text, command=command, width=26)
        button.grid(row=row, column=col, padx=2, pady=2, sticky="w")
        self.buttons.append(button)

    def _pack_button(self, parent, text, command):
        button = ttk.Button(parent, text=text, command=command)
        button.pack(side=tk.LEFT, padx=2)
        self.buttons.append(button)

    # --- atributos e comandos ----------------------------------------------

    def _find_row(self, object_id):
        for item in self.tree.get_children():
            if str(self.tree.item(item, "values")[0]) == object_id:
                return item
        return None

    def _on_tree_select(self, _event=None):
        """Clicar numa linha carrega object_id, name e type no editor."""
        selection = self.tree.selection()
        if not selection:
            return
        values = self.tree.item(selection[0], "values")
        self.var_new_object.set(str(values[0]))
        self.var_new_name.set(str(values[1]))
        self.var_new_type.set(str(values[2]))

    def _on_type_selected(self, _event=None):
        """Trocar o type com uma linha selecionada ja altera aquela linha."""
        selection = self.tree.selection()
        if not selection:
            return
        values = self.tree.item(selection[0], "values")
        if str(values[0]) != self.var_new_object.get().strip():
            return
        self.tree.item(selection[0], values=(values[0], values[1], self.var_new_type.get()))
        self._on_change()

    def add_attribute(self):
        """Salvar: atualiza a linha com o mesmo object_id ou adiciona uma nova."""
        object_id = self.var_new_object.get().strip()
        name = self.var_new_name.get().strip()
        attr_type = self.var_new_type.get()
        if not TOKEN_RULE.match(object_id) or not TOKEN_RULE.match(name):
            messagebox.showwarning("Atributo", "Preencha object_id e name "
                                   "(so letras, numeros e _).")
            return
        row = self._find_row(object_id)
        for existing in self.collect_attributes():
            if existing[1] == name and existing[0] != object_id:
                messagebox.showwarning("Atributo", "O name '%s' ja pertence ao object_id "
                                       "'%s'." % (name, existing[0]))
                return
        if row is None:
            self.tree.insert("", tk.END, values=(object_id, name, attr_type))
        else:
            self.tree.item(row, values=(object_id, name, attr_type))
        self.tree.selection_remove(self.tree.selection())
        self.var_new_object.set("")
        self.var_new_name.set("")
        self._on_change()

    def remove_attribute(self):
        for item in self.tree.selection():
            self.tree.delete(item)
        self._on_change()

    def collect_attributes(self):
        out = []
        for item in self.tree.get_children():
            values = self.tree.item(item, "values")
            out.append((str(values[0]), str(values[1]), str(values[2])))
        return out

    def collect_commands(self):
        out = []
        for part in self.var_commands.get().split(","):
            cleaned = part.strip()
            if cleaned:
                out.append(cleaned)
        return out

    def current_device(self):
        return Device(self.var_host.get(), self.var_name.get(), self.var_number.get(),
                      self.collect_attributes(), self.collect_commands())

    # --- preview e comboboxes ---------------------------------------------

    def _on_change(self, *_args):
        dev = self.current_device()
        self._sync_box(self.box_orion_attr, self.var_orion_attr, dev.attr_names, [])
        self._sync_box(self.box_sth_attr, self.var_sth_attr, dev.sth_attrs, [])
        self._sync_box(self.box_command, self.var_command, dev.commands, [])
        self._sync_box(self.box_sim_only, self.var_sim_only, [SIM_ALL] + dev.attr_names,
                       [SIM_ALL])

        lines = []
        if NAME_RULE.match(dev.name) and ID_RULE.match(dev.number):
            lines.append("device_id   : %s" % dev.device_id)
            lines.append("entity_name : %s" % dev.entity_id)
            lines.append("entity_type : %s" % dev.entity_type)
            for topic, payload in dev.firmware_example():
                lines.append("publicar    : %-26s -> %s" % (topic, payload))
            if dev.commands:
                lines.append("comandos    : %s  <-  %s" % (dev.mqtt_cmd_topic,
                                                           dev.mqtt_cmd_example))
        else:
            lines.append("Preencha nome e ID validos para ver")
            lines.append("os identificadores do dispositivo.")
        self.preview.configure(state=tk.NORMAL)
        self.preview.delete("1.0", tk.END)
        self.preview.insert("1.0", "\n".join(lines))
        self.preview.configure(state=tk.DISABLED)

    def _sync_box(self, box, var, values, preferred):
        box.configure(values=values)
        if var.get() in values:
            return
        if preferred:
            var.set(preferred[0])
        elif values:
            var.set(values[0])
        else:
            var.set("")

    # --- log ---------------------------------------------------------------

    def log_line(self, text, tag="dim"):
        self.log.configure(state=tk.NORMAL)
        self.log.insert(tk.END, text + "\n", tag)
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    def clear_log(self):
        self.log.configure(state=tk.NORMAL)
        self.log.delete("1.0", tk.END)
        self.log.configure(state=tk.DISABLED)

    def _drain_queue(self):
        while True:
            try:
                text, tag = self.queue.get_nowait()
            except queue.Empty:
                break
            self.log_line(text, tag)
        self.root.after(80, self._drain_queue)

    # --- execucao em thread (a janela nao congela durante o HTTP) ---------

    def _set_busy(self, busy, label="pronto"):
        self.busy = busy
        state = tk.NORMAL
        if busy:
            state = tk.DISABLED
        for button in self.buttons:
            button.configure(state=state)
        self.status.configure(text=label)

    def run_async(self, label, func, need_device=True):
        if self.busy:
            return
        dev = self.current_device()
        if need_device:
            problems = dev.validate()
        else:
            problems = []
            if not dev.host:
                problems.append("Informe o IP do servidor.")
        if problems:
            messagebox.showerror("Dados invalidos", "\n".join(problems))
            return
        self._set_busy(True, "executando: %s" % label)

        def enqueue(text, tag="dim"):
            self.queue.put((text, tag))

        def worker():
            try:
                func(Runner(enqueue), dev)
            except NetworkError as exc:
                enqueue("FALHA DE REDE: %s" % exc, "err")
                enqueue("Confira o IP, se os containers estao de pe e se as portas "
                        "4041/1026/8666/1883 estao liberadas.", "info")
            except Exception as exc:
                enqueue("ERRO INTERNO: %r" % exc, "err")
            finally:
                self.root.after(0, self._set_busy, False)

        threading.Thread(target=worker, daemon=True).start()

    def run_simple(self, method_name):
        """Acao que so precisa do IP (health check, listagens gerais)."""
        self.run_async(method_name, lambda r, d: getattr(r, method_name)(d),
                       need_device=False)

    def run_device(self, method_name):
        """Acao que precisa do dispositivo completo."""
        self.run_async(method_name, lambda r, d: getattr(r, method_name)(d))

    def _confirm(self, title, text):
        return messagebox.askyesno(title, text)

    # --- handlers: fluxo completo -----------------------------------------

    def on_provision(self):
        self.run_device("provision")

    def on_check(self):
        self.run_device("check")

    def on_teardown(self):
        dev = self.current_device()
        if self._confirm("Remover", "Remover device, entidade, registration e "
                         "subscription de '%s'?" % dev.device_id):
            self.run_device("teardown")

    # --- handlers: IoT Agent ----------------------------------------------

    def on_delete_group(self):
        if self._confirm("Remover service group",
                         "O service group apikey=%s e COMPARTILHADO pela turma.\n"
                         "Remover derruba a recepcao MQTT de TODOS os alunos.\n\n"
                         "Confirma?" % APIKEY):
            self.run_simple("iota_delete_group")

    def on_delete_device(self):
        dev = self.current_device()
        if self._confirm("Remover", "Remover '%s' do IoT Agent?\n(A entidade no Orion "
                         "continua; use a aba Orion para remove-la.)" % dev.device_id):
            self.run_device("iota_delete_device")

    # --- handlers: Orion --------------------------------------------------

    def on_delete_entity(self):
        dev = self.current_device()
        if self._confirm("Remover", "Remover a entidade '%s' do Orion?" % dev.entity_id):
            self.run_device("orion_delete_entity")

    def on_orion_read(self):
        attr = self.var_orion_attr.get()
        if not attr:
            messagebox.showwarning("Orion", "Escolha um atributo.")
            return
        self.run_async("ler", lambda r, d: r.orion_read_attr(d, attr))

    def on_orion_update(self):
        attr = self.var_orion_attr.get()
        if not attr:
            messagebox.showwarning("Orion", "Escolha um atributo.")
            return
        attr_type = self.current_device().attr_type(attr)
        try:
            plain = to_text_plain(attr_type, self.var_orion_value.get())
        except ValueError as exc:
            messagebox.showwarning("Orion", "Valor invalido para %s: %s" % (attr_type, exc))
            return
        self.run_async("atualizar", lambda r, d: r.orion_update_attr(d, attr, plain))

    def on_send_command(self):
        command = self.var_command.get()
        if not command:
            messagebox.showwarning("Orion", "Nenhum comando declarado.")
            return
        self.run_async("comando", lambda r, d: r.orion_send_command(d, command))

    # --- handlers: STH-Comet ----------------------------------------------

    def on_delete_subscriptions(self):
        dev = self.current_device()
        if self._confirm("Remover", "Remover as subscriptions de '%s'?\n(O STH para de "
                         "gravar novos pontos.)" % dev.entity_id):
            self.run_device("sth_delete_subscriptions_flow")

    def on_history(self):
        attr = self.var_sth_attr.get()
        raw = self.var_lastn.get().strip()
        if not attr:
            messagebox.showwarning("STH-Comet", "Nenhum atributo numerico declarado.")
            return
        if not raw.isdigit() or int(raw) < 1:
            messagebox.showwarning("STH-Comet", "lastN deve ser inteiro >= 1.")
            return
        last_n = int(raw)
        self.run_async("serie temporal", lambda r, d: r.sth_history(d, attr, last_n))

    # --- handlers: simulacao ----------------------------------------------

    def _read_sim_config(self, single):
        try:
            low = float(self.var_sim_low.get())
            high = float(self.var_sim_high.get())
            interval = float(self.var_sim_interval.get())
            count = int(self.var_sim_count.get())
            port = int(self.var_sim_port.get())
        except ValueError:
            messagebox.showwarning("Simulacao", "min, max, intervalo, amostras e porta "
                                   "devem ser numeros.")
            return None
        texts = []
        for part in self.var_sim_text.get().split(","):
            cleaned = part.strip()
            if cleaned:
                texts.append(cleaned)
        problems = []
        if low >= high:
            problems.append("min deve ser menor que max.")
        if interval < 0.2:
            problems.append("intervalo minimo: 0.2 s.")
        if count < 1 or count > 10000:
            problems.append("amostras: entre 1 e 10000.")
        if port < 1 or port > 65535:
            problems.append("porta MQTT invalida.")
        if not texts:
            problems.append("informe ao menos um valor Text (ex.: on, off).")
        for text in texts:
            bad = [c for c in text if c in FORBIDDEN_TEXT]
            if bad:
                problems.append("valor Text '%s' contem '%s'." % (text, bad[0]))
        if problems:
            messagebox.showwarning("Simulacao", "\n".join(problems))
            return None
        fmt_key = SIM_FORMATS[0][0]
        fmt_label = self.var_sim_format.get()
        for key, label in SIM_FORMATS:
            if label == fmt_label:
                fmt_key = key
        if single:
            count = 1
        return {"mode": self.var_sim_mode.get(), "port": port, "low": low, "high": high,
                "texts": texts, "interval": interval, "count": count,
                "format": fmt_key, "format_label": fmt_label,
                "only": self.var_sim_only.get()}

    def on_simulate(self, single):
        cfg = self._read_sim_config(single)
        if cfg is None:
            return
        self.stop_event = threading.Event()
        stop = self.stop_event
        self.run_async("simulacao", lambda r, d: r.simulate(d, cfg, stop))

    def on_stop(self):
        self.stop_event.set()


def main():
    root = tk.Tk()
    try:
        ttk.Style().theme_use("clam")
    except tk.TclError:
        pass
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
