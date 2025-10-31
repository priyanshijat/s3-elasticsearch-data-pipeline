# 🚀 AWS S3 → Elasticsearch Incremental ETL Pipeline

## 🧭 Overview
This project builds an **end-to-end incremental data pipeline** that transfers **partitioned data** from **Amazon S3** to **Elasticsearch**, using **AWS Glue** and **Athena** for schema discovery and querying.  

It demonstrates how to **load only the latest month’s data** into Elasticsearch while keeping **Existing data** safely stored in S3 for analytics — optimizing cost, performance, and scalability.

---

## 🏗️ Architecture

    +------------+        +-----------+        +-----------+        +----------------+
    |   Raw Data | -----> |  S3 Bucket | -----> |  AWS Glue | -----> |  AWS Athena    |
    +------------+        +-----------+        +-----------+        +----------------+
                                                           |
                                                           v
                                                  +----------------+
                                                  | Elasticsearch  |
                                                  +----------------+

---

## ⚙️ Tech Stack

| Component | Description |
|------------|-------------|
| **Amazon S3** | Stores raw and partitioned data (e.g., `s3://my-incremental-data-bucket/data/date=2025-10-01/`) |
| **AWS Glue Crawler** | Automatically infers schema and updates the Glue Data Catalog |
| **AWS Athena** | Queries partitioned S3 data efficiently |
| **AWS Glue Job / Python Script** | Loads only the latest partition’s data into Elasticsearch |
| **Elasticsearch / OpenSearch** | Stores and indexes the latest data for fast search and visualization |

-----------
2️⃣ Generate and Upload Partitioned Data to S3Upload to S3:

``` 
aws s3 cp data/ s3://my-incremental-data-bucket/data/ --recursive
```
---------------------------------------------------------

3️⃣ Create AWS Glue Database and Crawler

Go to AWS Console → Glue → Crawlers → Create crawler

Source: s3://my-incremental-data-bucket/data/

Target: Select or create a new Glue Database (e.g., incremental_db)

Run the crawler
✅ A table appears in Glue Data Catalog (e.g., incremental_table)

----------------------------------------------------------

4️⃣ Verify Data in Athena

Go to AWS Athena Query Editor and connect to your database.

```
SELECT * FROM "incremental_db"."incremental_table" LIMIT 10;
```
```
SHOW PARTITIONS "incremental_db"."incremental_table";
```

If partitions are missing, repair them:

```
MSCK REPAIR TABLE incremental_db.incremental_table;
```

----------------------------------------------------------

5️⃣ Create a Glue ETL Job to Load Data into Elasticsearch

Go to AWS Glue → Jobs → Create Job

Choose a Python shell job

Add the following ETL script (example):

----------------------------------------------------------

6️⃣ Setup Elasticsearch on a Virtual Machine

On your VM:
```
sudo apt update
sudo apt install elasticsearch
sudo systemctl enable elasticsearch
sudo systemctl start elasticsearch
```
Edit the config file:
```
sudo nano /etc/elasticsearch/elasticsearch.yml
```

Uncomment and set:
```
network.host: 0.0.0.0
discovery.type: single-node
```

Restart service:
```
sudo systemctl restart elasticsearch
```

----------------------------------------------------------


7️⃣ Test the End-to-End Flow

Run Glue crawler → Schema updates in Data Catalog

Run Athena query → Verify partitioned data

Run Glue ETL job → Uploads only latest month’s partition to Elasticsearch

Test in Elasticsearch:
```
curl -X GET "http://<your-vm-public-ip>:9200/incremental_data/_search?pretty"
```
----------------------------------------------------------
