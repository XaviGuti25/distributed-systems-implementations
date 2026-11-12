# Distributed Systems Implementations

Simplified versions of **HDFS**, **MapReduce** and **Kafka** as Docker microservices in Python, plus data-processing jobs with **Apache Spark RDDs**.

The course teaching team provided a starter skeleton for each system (project structure, Docker setup and API stubs); we implemented the core logic on top of it.

Team project for the *Large-Scale Distributed Systems* course at Universitat Pompeu Fabra (2026).

## Systems

| Folder | System | What it does |
|---|---|---|
| [`hdfs/`](hdfs) | Mini HDFS | Distributed file storage: a namenode keeps file metadata (with journal + checkpoint for recovery) and datanodes store replicated blocks. The client uploads and downloads files block by block, with failover between replicas. |
| [`mapreduce/`](mapreduce) | Mini MapReduce | A master schedules map and reduce tasks across 5 workers and tracks job progress. Includes word count, bigrams and link-counting apps. |
| [`spark-rdd/`](spark-rdd) | Spark RDD jobs | PySpark jobs on a Spark cluster: sums, bigram counts, people counts and an inverted index over Wikipedia articles. |
| [`kafka/`](kafka) | Mini Kafka | A 5-broker message store with topics and partitions, leader election and a KRaft-style metadata quorum, plus producer and consumer clients. |

## Tech stack

Python · FastAPI · Pydantic · httpx · asyncio · Docker / Docker Compose · PySpark

## How to run

Each system runs with Docker Compose from its own folder.

**HDFS**
```bash
cd hdfs
docker compose up --build
python3 client/upload.py test_files/cat.txt cat.txt
python3 client/download.py cat.txt downloaded-cat.txt
```

**MapReduce**
```bash
cd mapreduce
docker compose up --build
python3 client/run.py medium-text 4 10 word_count
```

**Spark RDD**
```bash
cd spark-rdd
docker compose up
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/bitnami/spark/app/spark_reverse_index.py \
  /opt/bitnami/spark/app/data/wikipedia10.json /opt/bitnami/spark/app/output
```
`data/wikipedia10.json` is a small sample of Wikipedia articles used for testing instead of the full dump.

**Kafka**
```bash
cd kafka
docker compose up --build
python3 client/create_topic.py testtopic -p 3
python3 client/produce.py testtopic
python3 client/consume.py testtopic-1
```

## My contribution

- **HDFS:** namenode (metadata, journal and checkpoint) and FastAPI setup.
- **MapReduce:** master scheduler and the bigrams, links and modified word count apps.
- **Spark:** first RDD jobs (sum and sum of even numbers).
- **Kafka:** KRaft metadata quorum, broker API and the producer and consumer clients.

## Credits

Built with [@Martiperez314](https://github.com/Martiperez314) and [@MrMartinez100](https://github.com/MrMartinez100).
Lab design and project templates by the LSDS teaching team at Universitat Pompeu Fabra.
