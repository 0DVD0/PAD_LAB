# Agent de mesagerie - Partea 1

Aplicatie distribuita TCP/JSON cu Publisher, Broker si Subscriberi. Broker-ul
ruteaza mesaje dupa topic, pastreaza mesaje si livrari in SQLite, foloseste o
coada thread-safe si workeri concurenti, asteapta ACK si reincearca livrarile
esuate de maximum trei ori.

## Arhitectura

```text
Publisher --publish--> Broker --deliver--> Subscriber A
                           |  \-----------> Subscriber B
                           |
                           +--> Queue + SQLite
```

Fiecare conexiune TCP este unul-la-unul si full-duplex. Numarul conexiunilor
este variabil, in functie de clienti si livrari. Mesajele folosesc framing:
`[4 bytes lungime][N bytes JSON UTF-8]`.

## Instalare

Proiectul necesita Python 3.10 sau mai nou.

```bash
cd /Users/dvd/Desktop/UNIVER/PAD/LAB1
source .venv/bin/activate
pip install -r requirements.txt
```

`rich` este optional pentru functionare, dar ofera interfata colorata.

## Pornire

Toate comenzile se executa din `Lab1.1`, in terminale separate.

```bash
cd /Users/dvd/Desktop/UNIVER/PAD/LAB1/Lab1.1
python broker.py
```

Subscriberii se pot porni complet interactiv. ID-ul si topicul sunt cerute in
consola, iar sistemul de operare aloca automat un port TCP liber:

```bash
python subscriber.py
```

Subscriber-ul afiseaza topicurile existente intr-o lista numerotata. Se poate
alege un numar sau se poate scrie un topic nou. La `Ctrl+C`, Subscriber-ul se
dezaboneaza automat de la Broker.

Alternativ, toate valorile pot fi oferite prin argumente. Portul este optional:

```bash
python subscriber.py --id alice --topic news
python subscriber.py --id bob --topic news
python subscriber.py --id carol --topic orders
```

Optional, se pot filtra tipurile:

```bash
python subscriber.py --id alice --topic news --types news notification
```

Tipurile nu provin dintr-o lista fixa. Orice tip ne-gol de maximum 100 de
caractere, format din litere, cifre, `-`, `_` si `.`, este acceptat. De exemplu:
`email.sent`, `payment-created` sau `temperature_update`.

Publisher:

```bash
python publisher.py
```

Aliasurile vechi functioneaza astfel:

```bash
python sender.py
python receiver.py --id alice --topic news
```

## Protocol

Abonare:

```json
{
  "action": "subscribe",
  "subscriber_id": "alice",
  "topic": "news",
  "host": "127.0.0.1",
  "port": 6101,
  "accepted_types": ["news"]
}
```

Publicare:

```json
{
  "action": "publish",
  "message": {
    "id": "uuid",
    "topic": "news",
    "type": "news",
    "created_at": "2026-09-15T12:00:00+00:00",
    "payload": {"text": "Mesaj demonstrativ"}
  }
}
```

Livrare si confirmare:

```json
{"action": "deliver", "message": {"...": "..."}}
```

```json
{
  "action": "ack",
  "status": "success",
  "message_id": "uuid",
  "subscriber_id": "alice"
}
```

Alte actiuni: `unsubscribe`, `list_topics`, `stats`, `ping`.

## Persistenta si politica de livrare

- `queue.Queue` este stocarea tranzitorie thread-safe.
- SQLite (`database/broker.db`) este stocarea persistenta.
- Un mesaj creeaza o livrare separata pentru fiecare abonat compatibil.
- Dupa ACK, livrarea devine `delivered`.
- La eroare sau timeout, livrarea devine `retry`.
- Intarzierile sunt 2 si 4 secunde; a treia eroare marcheaza `failed`.
- La repornire, Broker-ul restaureaza abonamentele si livrarile incomplete.
- Daca un Subscriber se reconecteaza cu acelasi ID pe un port automat nou,
  endpoint-ul livrarilor `pending`/`retry` este actualizat la noul port.
- Un mesaj fara abonati este pastrat cu status `no_subscribers`, dar nu este
  livrat retroactiv abonatilor care apar ulterior.
- Un ID de mesaj repetat este respins cu `DUPLICATE_MESSAGE`.

## Scenariu demonstrativ

1. Porneste Broker-ul.
2. Porneste Alice si Bob pe topicul `news`.
3. Porneste Carol pe topicul `orders`.
4. Publica un mesaj `news`; doar Alice si Bob il primesc.
5. Opreste Bob si publica din nou; livrarea lui este reincercata.
6. Porneste Bob inainte de a treia incercare; mesajul este confirmat.
7. Foloseste meniul Publisher-ului pentru topicuri si statistici.

Broker-ul este un single point of failure. SQLite recupereaza starea dupa
repornire, dar disponibilitatea ridicata ar necesita replicarea Broker-ului,
care nu este implementata in aceasta lucrare.

## Partea 2

Implementarea echivalenta prin gRPC si Protocol Buffers se afla in `lab1.2`.
Consultati `lab1.2/README.md` pentru generarea codului protobuf, pornire,
arhitectura RPC si teste.
