# 📺 Kafka – Section 3e

In this section, we introduce **In-Sync Replicas (ISR)** and **High Watermarks (HW)** to enforce durability and consistency in our Kafkaesque cluster. These guarantees ensure that writes are safely replicated and reads only return committed data.

- **Part 1 — ISR & High Watermark Implementation**:  
  We update the broker to track ISR and High Watermarks, extend topic creation with `minISR`, enforce write rejection when replicas are out of sync, and gate consumer reads using committed offsets.

- **Part 2 — ISR & High Watermark Validation**:  
  We launch both brokers, create topics with `minISR=2`, observe failed writes before replication, verify ISR + HW initialization, and confirm that consumers only process fully replicated events.

<div align="center">
    <img src="./section_3e_design.png" alt="System Architecture Diagram" width="2000"/>
</div>

## 🎥 Video Walkthrough

### 🔹 Part 1: ISR & High Watermark Implementation

**Title:** Kafka – Section 3e (Part 1)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998907#overview)

### 🔹 Part 2: ISR & High Watermark Validation

**Title:** Kafka – Section 3e (Part 2)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998909#overview)

# ⚙️ Instructions and Commands

## ✏️ Part 1 – ISR & High Watermark Implementation

Update `broker/app.py` and `broker/_util.py`.

<br>

## ✏️ Part 2 – ISR & High Watermark Validation

From `~/Desktop/kafka_demo` (project root):

### 1. Launch Kafkaesque Brokers

Refer back to **[Section 3C → Step 1](../section_3c/README.md#1-launch-kafkaesque-brokers)** for the exact commands to launch `broker_a` and `broker_b`.

### 2. Create `broker_a` Topics (`minISR=2` for Data Topics)

Create the `Order` and `Payment` data topics, this time with 2 partitions per topic, a replication factor set to 2, and a `minISR` of 2:

```bash
curl -X POST http://localhost:19092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"order","partitions":2,"replication_factor":2,"minISR":2}'

curl -X POST http://localhost:19092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"payment","partitions":2,"replication_factor":2,"minISR":2}'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  curl.exe -X POST http://localhost:19092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"order\",\"partitions\":2,\"replication_factor\":2,\"minISR\":2}'

  curl.exe -X POST http://localhost:19092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"payment\",\"partitions\":2,\"replication_factor\":2,\"minISR\":2}'
  ```

Create the internal `__consumer_offsets` topic, also with 2 partitions and `RF=2`, but without `minISR`:

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

### 3. Create `broker_b` Topics (`minISR=2` for Data Topics)

Create the `Order` and `Payment` data topics, this time with 2 partitions per topic, replication factor set to 2, and a `minISR` of 2:

```bash
curl -X POST http://localhost:29092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"order","partitions":2,"replication_factor":2,"minISR":2}'

curl -X POST http://localhost:29092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"payment","partitions":2,"replication_factor":2,"minISR":2}'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  curl.exe -X POST http://localhost:29092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"order\",\"partitions\":2,\"replication_factor\":2,\"minISR\":2}'

  curl.exe -X POST http://localhost:29092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"payment\",\"partitions\":2,\"replication_factor\":2,\"minISR\":2}'
  ```

Create the internal `__consumer_offsets` topic, also with 2 partitions and `RF=2`, but without `minISR`:

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

### 4. Launch `e_commerce_app_kafkaesque`

Refer back to **[Section 3C → Step 5](../section_3c/README.md#5-launch-e_commerce_app_kafkaesque)** for the command to launch `e_commerce_app_kafkaesque`.

### 5. Verify Internal State on `broker_a` and `broker_b`

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

### 6. Produce `order_1` before `minISR` is Satisfied

```bash
curl -X POST http://localhost:5001/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "order",
    "key": "order_1",
    "event": {
      "event_type": "OrderPlaced",
      "order_id": "order_1",
      "user_id": "user_1",
      "items": [
        { "product_id": "prod_1", "quantity": 2 },
        { "product_id": "prod_2", "quantity": 1 }
      ],
      "total_amount": 84.97,
      "timestamp": "2025-01-01T10:00:00Z"
    }
  }'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell:**
  - Use `curl.exe` instead of `curl` (to avoid the PowerShell alias)
  - Use backticks (`` ` ``) for multiline commands—**not** backslashes (`\`)
  - Any quotes inside your JSON payload must be escaped (use `\"` instead of `"`)

  ```bash
  curl.exe -X POST http://localhost:5001/produce `
    -H "Content-Type: application/json" `
    -d '{
      \"topic\": \"order\",
      \"key\": \"order_1\",
      \"event\": {
        \"event_type\": \"OrderPlaced\",
        \"order_id\": \"order_1\",
        \"user_id\": \"user_1\",
        \"items\": [
          { \"product_id\": \"prod_1\", \"quantity\": 2 },
          { \"product_id\": \"prod_2\", \"quantity\": 1 }
        ],
        \"total_amount\": 84.97,
        \"timestamp\": \"2025-01-01T10:00:00Z\"
      }
    }'
  ```

### 7. Verify Internal State on `broker_a` and `broker_b`

Refer back to **[Step 5](#5-verify-internal-state-on-broker_a-and-broker_b)** for the debug commands.

### 8. Produce All 4 Test Orders (`order_1`, `order_2`, `order_3` and `order_4`)

Refer back to **[Section 3C → Step 8](../section_3c/README.md#8-produce-all-4-test-orders-order_1-order_2-order_3-and-order_4)** for the exact commands to produce all 4 orders.

### 9. Verify Partition Files

```bash
for f in .var/kafkaesque/*/*/*.log; do echo "== $f =="; cat "$f"; done
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  Get-ChildItem .var\kafkaesque\*\*\*.log | ForEach-Object {
    $r=$_.FullName.Replace((Get-Location).Path + '\','')
    "== $r =="; Get-Content $_ }
  ```

### 10. Verify Internal State on `broker_a` and `broker_b`

Refer back to **[Step 5](#5-verify-internal-state-on-broker_a-and-broker_b)** for the debug commands.

### 11. Verify All Outputs:

Verify database records:  
&nbsp;&nbsp;&nbsp;&nbsp;_Refer back to **[Section 1D → Step 4](/chapter_1/section_1d/README.md#4-ensure-the-app_db_endpoint-environment-variable-is-set)** to set the `APP_DB_ENDPOINT` environment variable._

```bash
docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 \
  mysql -h $APP_DB_ENDPOINT -u admin \
  --table -e "USE services_db; SELECT * FROM Orders;"
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**, run the command on a single line (no line breaks):
  ```bash
  docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 mysql -h $APP_DB_ENDPOINT -u admin --table -e "USE services_db; SELECT * FROM Orders;"
  ```

Verify on disk partition log file contents:

> _Refer back to **[Step 9](#9-verify-partition-files)** for the commands to display the partition file contents._

Verify internal state on `broker_a` and `broker_b`:

> _Refer back to **[Step 5](#5-verify-internal-state-on-broker_a-and-broker_b)** for the debug commands._

### 12. Shutdown & Reset Environment

Refer back to **[Section 3A → Step 10](../section_3a/README.md#10-shutdown--reset-environment)** for the shutdown and cleanup commands.

<br>
