### Kafka


```bash
docker-compose build
```


```bash
docker-compose up -d
```

```bash
docker-compose ps
```
```
http://localhost:8081/#/overview

```
```bash
docker-compose down -v
```

```bash
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic itmo2023 --partitions 1 --replication-factor 1
```
```bash
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe itmo2023  
```
```bash
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --alter --topic itmo2023 --partitions 2
```

```bash
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job.py -d  
```
