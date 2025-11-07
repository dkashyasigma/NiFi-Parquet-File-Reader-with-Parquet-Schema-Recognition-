
# ✅ **NiFi Parquet Schema Reader Processor**

<img width="752" height="377" alt="image" src="https://github.com/user-attachments/assets/c621bff7-45bd-413a-b920-ab51c4167dbb" />

<img width="878" height="446" alt="image" src="https://github.com/user-attachments/assets/028b79ec-b000-4e82-a3e0-5260a04383f0" />



# 📘 NiFi Parquet Schema Reader — Custom Processor  
A custom Apache NiFi processor designed to read **Parquet files** using the embedded **Parquet schema** .  
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

NiFi currently lacks a native, lightweight Parquet reader that works without Hadoop.  
This processor enables:

- Parquet reading without Hadoop dependencies  
- Schema-aware record extraction  
- Streaming JSON generation  
- Cloud-friendly deployment & performance  

Ideal for **modern NiFi deployments, serverless environments, and large-file processing**.

---

# 🧩 Maven Build Commands

Below are **all the commands** you need to build and test the project.

---

## ✅ 1. **Build without running tests**

```sh
mvn clean install -DskipTests
```

Speeds up build time and is useful once tests are already verified.

---
## ✅ 2. **Build NAR module only (multi-module project)**

```sh
mvn clean install -pl nifi-parquet-schema-reader-nar -am
```

Useful when you update processor code and only want to rebuild the NAR.

---

# 📦 Deployment

## ✅ 1. Build the NAR

```sh
mvn clean install -DskipTests
```

The NAR is generated at:

```
nifi-parquet-schema-reader-nar/target/*.nar
```

## ✅ 2. Deploy to NiFi

Copy the NAR to:

```
<NIFI_HOME>/extensions
```

## ✅ 3. Restart NiFi

NiFi loads the processor automatically.

---

# 🚀 Usage

1. Drag **ParquetSchemaReader** to your NiFi canvas
2. Configure:

   * Input FlowFile (your Parquet file)
   * Output format = JSON
3. Connect **success** and **failure** relationships
4. Trigger the processor

---

# 📃 Example JSON Output

```json
[
  {
    "customer_id": 00123,
    "country": "IN",
    "balance": 5599.50,
    "created_timestamp": "2025-11-07T11:45:22Z"
  }
]
```

---

# 📁 Project Structure

```
nifi-parquet-schema-reader/
   ├── nifi-parquet-schema-reader-processors/
   │      └── src/main/java/... (processor source code)
   ├── nifi-parquet-schema-reader-nar/
   │      └── target/*.nar
   ├── assets/
   │      └── parquet_reader_banner.png
   ├── README.md
   ├── LICENSE
   └── pom.xml
```

---

# 🔍 Keywords

* NiFi Parquet Processor
* Apache NiFi Custom Processor
* Read Parquet in NiFi
* NiFi Parquet Schema Reader
* Parquet to JSON NiFi
* Hadoop-free Parquet Reader
* NiFi Extension / NiFi Plugin
* Big Data Processing NiFi
* Parquet Stream Reader
* Cloud NiFi Processors

---

# ✅ License

Apache License 2.0 — free to use, modify, and distribute.

# ⭐ Support

If this project helps you, please **star the repository** to support future development.

```


