<p align="center">
  <img src="" alt="NiFi Parquet Schema Reader" width="100%" />
</p>

# 📘 NiFi Parquet Schema Reader — Custom Processor  
A custom Apache NiFi processor designed to read **Parquet files** using the embedded **Parquet schema** (without Hadoop).  
It converts rows to JSON in a **streaming, memory-efficient** manner suitable for large files and cloud deployments.

---

## ✨ Key Features

✅ Reads Parquet files **directly using Parquet schema**  
✅ Converts data to **JSON**  
✅ **Hadoop-free** (lightweight, no heavy libs)  
✅ **Streaming mode** — does not load entire file into memory  
✅ Works in **NiFi 1.x / 2.x**  
✅ Plug-and-play NAR deployment  
✅ Suitable for **Kubernetes, Docker, cloud ETL pipelines**

---
## 📌 Why This Processor?

NiFi includes a Parquet reader that depends on an Avro schema, but it does not support reading files using the native Parquet schema.
This custom processor solves that limitation by enabling:
✅ Direct Parquet reading without needing an external Avro schema
✅ Schema-aware record extraction using the embedded Parquet schema
✅ Efficient streaming-based JSON generation (no full file loading)
✅ Lightweight, cloud-friendly performance suitable for large datasets 

Ideal for **modern NiFi deployments, serverless environments, and large-file processing**.

---

# 🧩 Maven Build Commands

Below are **all the commands** you need to build and test the project.

---

## ✅ 1. **Build with tests enabled**

```sh
mvn clean install

Processor images:

<img width="752" height="377" alt="image" src="https://github.com/user-attachments/assets/c621bff7-45bd-413a-b920-ab51c4167dbb" />

<img width="878" height="446" alt="image" src="https://github.com/user-attachments/assets/028b79ec-b000-4e82-a3e0-5260a04383f0" />

