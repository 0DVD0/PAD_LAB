# Partea 2 - gRPC si Protocol Buffers

Aceeasi arhitectura Publisher/Broker/Subscriber din Partea 1, implementata la
un nivel de abstractie mai inalt. Comunicarea foloseste apeluri gRPC peste
HTTP/2, iar contractul si datele sunt definite in `message_broker.proto`.

## Componente

- `message_broker.proto` - contractul RPC si schema mesajelor.
- `broker.py` - serviciul `BrokerService`, rutare, storage, workeri si retry.
- `subscriber.py` - serviciul `SubscriberService` si confirmare `DeliveryAck`.
- `publisher.py` - client gRPC interactiv.
- `storage.py` - SQLite; mesajele protobuf sunt pastrate ca BLOB.
- `subscriptions.py` - registru concurent pentru topicuri.
- `validation.py` - validare pentru mesaje si abonamente.
- `generate_proto.py` - regenereaza modulele Python din fisierul `.proto`.

## Instalare si generare

```bash
cd /Users/dvd/Desktop/UNIVER/PAD/LAB1
source .venv/bin/activate
pip install -r requirements.txt
python lab1.2/generate_proto.py
```

## Pornire

In trei terminale separate:

```bash
cd /Users/dvd/Desktop/UNIVER/PAD/LAB1/lab1.2
python broker.py
```

```bash
cd /Users/dvd/Desktop/UNIVER/PAD/LAB1/lab1.2
python subscriber.py
```

```bash
cd /Users/dvd/Desktop/UNIVER/PAD/LAB1/lab1.2
python publisher.py
```

Subscriber-ul permite alegerea interactiva a topicului si primeste automat un
port liber. Tipurile sunt dinamice si pot fi filtrate optional:

```bash
python subscriber.py --id alice --topic news --types news email.sent
```

## Concurenta si livrare

- serverele gRPC folosesc `ThreadPoolExecutor`;
- `queue.Queue` este coada tranzitorie thread-safe;
- SQLite este storage-ul persistent;
- fiecare abonat are o livrare separata;
- Broker-ul asteapta `DeliveryAck` de la Subscriber;
- erorile RPC si timeout-urile produc retry, maximum trei incercari;
- livrarile incomplete sunt recuperate la repornirea Broker-ului;
- reabonarea aceluiasi Subscriber actualizeaza portul livrarilor incomplete;
- cererile invalide folosesc statusurile gRPC standard, precum
  `INVALID_ARGUMENT` si `ALREADY_EXISTS`.

## Diferenta fata de Partea 1

In Partea 1, aplicatia gestioneaza direct socketuri, framing, JSON si bytes. In
Partea 2, gRPC gestioneaza transportul, conexiunile si serializarea, iar codul
apeleaza metode precum `Publish`, `Subscribe` si `Deliver`. Schema protobuf
este tipizata si comuna tuturor componentelor.

## Teste

```bash
cd /Users/dvd/Desktop/UNIVER/PAD/LAB1
python -m unittest discover -s lab1.2 -p "test_*.py" -v
```
