# 📺 Kafka – Section 3d

In this section, we introduce **replication** into our Kafkaesque cluster. We implement a pull-based replication model where a follower broker continuously fetches partition data from the leader, allowing the system to remain operational even after the leader fails.

- **Part 1 — Replication Implementation**:  
  We update the broker entrypoint to start a background replication thread, add a `/replica_fetch` endpoint, and implement the replication loop that synchronizes partition logs from leader to follower.

- **Part 2 — Replication Validation & Failover Test**:  
  We launch both brokers with replication enabled, produce data to the leader, verify that the follower stays in sync, then simulate a leader failure and confirm the follower can take over with fully replicated state.

<div align="center">
    <img src="./section_3d_design.png" alt="System Architecture Diagram" width="1000"/>
</div>

## 🎥 Video Walkthrough

### 🔹 Part 1: Replication Implementation

**Title:** Kafka – Section 3d (Part 1)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998893#overview)

### 🔹 Part 2: Replication Validation & Failover Test

**Title:** Kafka – Section 3d (Part 2)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998899#overview)

# ⚙️ Instructions and Commands

## ✏️ Part 1 – Replication Implementation

Update `kafkaesque/__main__.py`, `broker/app.py` and `broker/_util.py`.

<br>

## ✏️ Part 2 – Replication Validation & Failover Test

From `~/Desktop/kafka_demo` (project root):

### 1. Launch Kafkaesque Brokers

Refer back to **[Section 3C → Step 1](../section_3c/README.md#1-launch-kafkaesque-brokers)** for the exact commands to launch `broker_a` and `broker_b`.

### 2. Create `broker_a` Topics with `Partitions=2` and `RF=2`

Create the `Order` and `Payment` data topics, this time with 2 partitions per topic and replication factor set to 2:

```bash
curl -X POST http://localhost:19092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"order","partitions":2,"replication_factor":2}'

curl -X POST http://localhost:19092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"payment","partitions":2,"replication_factor":2}'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  curl.exe -X POST http://localhost:19092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"order\",\"partitions\":2,\"replication_factor\":2}'

  curl.exe -X POST http://localhost:19092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"payment\",\"partitions\":2,\"replication_factor\":2}'
  ```

Create the internal `__consumer_offsets` topic, also with 2 partitions and `RF=2`.

```bash
curl -X POST http://localhost:19092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"__consumer_offsets","partitions":2,"replication_factor":2}'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe -X POST http://localhost:19092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"__consumer_offsets\",\"partitions\":2,\"replication_factor\":2}'
  ```

### 3. Create `broker_b` Topics with `Partitions=2` and `RF=2`

Create the `Order` and `Payment` data topics, this time with 2 partitions per topic and replication factor set to 2:

```bash
curl -X POST http://localhost:29092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"order","partitions":2,"replication_factor":2}'

curl -X POST http://localhost:29092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"payment","partitions":2,"replication_factor":2}'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  curl.exe -X POST http://localhost:29092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"order\",\"partitions\":2,\"replication_factor\":2}'

  curl.exe -X POST http://localhost:29092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"payment\",\"partitions\":2,\"replication_factor\":2}'
  ```

Create the internal `__consumer_offsets` topic, also with 2 partitions and `RF=2`.

```bash
curl -X POST http://localhost:29092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"__consumer_offsets","partitions":2,"replication_factor":2}'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe -X POST http://localhost:29092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"__consumer_offsets\",\"partitions\":2,\"replication_factor\":2}'
  ```

### 4. Verify Internal State on `broker_a` and `broker_b`

Hit the debug endpoint:

```bash
curl http://localhost:19092/debug
curl http://localhost:29092/debug
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe http://localhost:19092/debug
  curl.exe http://localhost:29092/debug
  ```

### 5. Launch `e_commerce_app_kafkaesque`

Refer back to **[Section 3C → Step 5](../section_3c/README.md#5-launch-e_commerce_app_kafkaesque)** for the command to launch `e_commerce_app_kafkaesque`.

### 6. Produce `order_1` + `order_2`

Refer back to **[Section 3A → Step 6](../section_3a/README.md#6-produce-order_1--order_2)** for the exact commands to produce `order_1` and `order_2`.

### 7. Verify Partition Files

```bash
for f in .var/kafkaesque/*/*/*.log; do echo "== $f =="; cat "$f"; done
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  Get-ChildItem .var\kafkaesque\*\*\*.log | ForEach-Object {
    $r=$_.FullName.Replace((Get-Location).Path + '\','')
    "== $r =="; Get-Content $_ }
  ```

### 8. Verify Internal State on `broker_a` and `broker_b`

Refer back to **[Step 4](#4-verify-internal-state-on-broker_a-and-broker_b)** for the debug commands.

### 9. Verify Partition Files

Refer back to **[Step 7](#7-verify-partition-files)** for the commands to display the partition files content.

### 10. Verify Internal State on `broker_a` and `broker_b`

Refer back to **[Step 4](#4-verify-internal-state-on-broker_a-and-broker_b)** for the debug commands.

### 11. Kill `broker_a`

In `broker_a`'s terminal window, stop the process:

```bash
Ctrl + C
```

### 12. Verify Internal State on `broker_b`

Hit the debug endpoint:

```bash
curl http://localhost:29092/debug
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe http://localhost:29092/debug
  ```

### 13. Produce `order_3` + `order_4`

Refer back to **[Section 3A → Step 8](../section_3a/README.md#8-produce-order_3--order_4)** for the exact commands to produce `order_3` and `order_4`.

### 14. Verify Outputs

Refer back to **[Section 3C → Step 9](../section_3c/#9-verify-outputs)** for the commands to verify the database, partition logs, and `broker_b`'s internal state.

### 15. Shutdown & Reset Environment

Refer back to **[Section 3A → Step 10](../section_3a/README.md#10-shutdown--reset-environment)** for the shutdown and cleanup commands.

<br>
