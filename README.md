# 📂 USSD Event Processor – File Loader Service

This is **Application 1** of the USSD Event Processor system. It is a Spring Boot service responsible for **monitoring a folder**, detecting new USSD event log files, **loading their content into a PostgreSQL database**, and maintaining a log of the processing in a control table.

---

## 📌 Features

- 🕐 Monitors a folder (`C:/files/input`) **every minute**
- 📥 Detects new pipe-delimited USSD event files
- 🧾 Parses and stores event records into `call_detail_records` table
- 📊 Tracks processing metadata in `cdr_logs` table
- ✅ Marks files as `.processed` once completed and moves the files to processed folder.
- ❌ Marks file as `.error` once there is an error and moves the files to error folder.

---
