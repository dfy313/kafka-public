# 📺 Kafka – Section 4c

In this section, we introduce a **topic registry znode** in ZooKeeper to centralize topic configuration across the cluster. Brokers subscribe to this shared state using watchers, allowing them to automatically rebuild their local metadata, initialize partition logs, and recover state on restart without manual coordination.

<div align="center">
    <img src="./section_4c_design.png" alt="System Architecture Diagram" width="2000"/>
</div>

## 🎥 Video Walkthrough

**Title:** Kafka – Section 4c  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998921#overview)

# ⚙️ Instructions and Commands

From `~/Desktop/kafka_demo` (project root):

### 1. Start `zkServer` & `zkCli`

Refer back to **[Section 4A (Part 1) → Step 6](/chapter_4/section_4a/README.md#6-start-zkServer--zkCli)** for the commands to start ZooKeeper server and CLI.

### 2. Activate the Virtual Environment

Before continuing, ensure your virtual environment is activated:

```bash
source venv/bin/activate
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\venv\Scripts\Activate.ps1
  ```

_Additionally make sure the following dependencies are installed:_

- _Legacy dependencies from **[Section 1D → Step 2](/chapter_1/section_1d/README.md#2-set-up-a-virtual-environment-and-install-dependencies)**_
- _`requests` library from **[Section 2B (Part 3) → Step 4](/chapter_2/section_2b/README.md#4-virtual-environment-updates)**_
- _`kazoo` library from **[Section 4A (Part 2) → Step 5](/chapter_4/section_4a/README.md#5-virtual-environment-updates)**_

### 3. Launch Kafkaesque `broker_a`

```bash
BROKER_PORT=19092 BROKER_NAME=broker_a python -m kafkaesque
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:BROKER_PORT="19092"; $env:BROKER_NAME="broker_a"; python -m kafkaesque
  ```

### 4. Launch Kafkaesque `broker_b`

```bash
BROKER_PORT=29092 BROKER_NAME=broker_b python -m kafkaesque
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:BROKER_PORT="29092"; $env:BROKER_NAME="broker_b"; python -m kafkaesque
  ```

### 5. Create Topics on Controller (`broker_a`)

Create the `Order` and `Payment` data topics with `partitions=2`, `RF=2` and `minISR=2`.

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

Create the internal `__consumer_offsets` topic with `partitions=2`, `RF=2` and no `minISR` value.

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

_Verify that the correct folders and partition files have been created under the `.var` directory._

### 6. Inspect ZooKeeper State

From the ZooKeeper CLI:

```bash
ls /
get /topic_registry
```

### 7. Verify Internal State on `broker_a` and `broker_b`

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

### 8. Launch `e_commerce_app_kafkaesque`

Launch app with both `broker_a` and `broker_b` addresses passed into `KAFKA_BOOTSTRAP`:

> _Please make sure that the `APP_DB_ENDPOINT` environment variable is properly set. You can revisit **[Section 1D → Step 4](/chapter_1/section_1d/README.md#4-ensure-the-app_db_endpoint-environment-variable-is-set)** for the specific commands._

```bash
KAFKA_BOOTSTRAP=localhost:19092,localhost:29092 \
  DB_HOST=$APP_DB_ENDPOINT \
  python -m e_commerce_app_kafkaesque.launcher
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:KAFKA_BOOTSTRAP = "localhost:19092,localhost:29092"
  $env:DB_HOST = $APP_DB_ENDPOINT
  python -m e_commerce_app_kafkaesque.launcher
  ```

### 9. Produce `order_1`

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

### 10. Verify Partition Files

```bash
for f in .var/kafkaesque/*/*/*.log; do echo "== $f =="; cat "$f"; done
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  Get-ChildItem .var\kafkaesque\*\*\*.log | ForEach-Object {
    $r=$_.FullName.Replace((Get-Location).Path + '\','')
    "== $r =="; Get-Content $_ }
  ```

### 11. Verify Internal State on `broker_a` and `broker_b`

Refer back to **[Step 7](#7-verify-internal-state-on-broker_a-and-broker_b)** for the debug commands.

### 12. Kill the Standby Broker (`broker_b`)

In `broker_b`'s terminal window, stop the process:

```bash
Ctrl + C
```

### 13. Bring `broker_b` back Online

Refer back to **[Step 4](#4-launch-kafkaesque-broker_b)** for the command to start `broker_b`.

### 14. Verify Internal State on `broker_a` and `broker_b`

Refer back to **[Step 7](#7-verify-internal-state-on-broker_a-and-broker_b)** for the debug commands.

### 15. Shut Down Kafkaesque Brokers and `e_commerce_app_kafkaesque`

In the terminal windows running `broker_a` and `broker_b`, stop each process:

```bash
Ctrl + C
```

Stop the `e_commerce_app_kafkaesque` process:

```bash
Ctrl + C
```

### 16. Inspect ZooKeeper State

Refer back to **[Step 6](#6-inspect-zookeeper-state)** for the commands to inspect the ZooKeeper state.

### 17. Shut Down ZooKeeper CLI & Server

In the terminal windows running `zkCli` and `zkServer`, stop each process:

```bash
Ctrl + C
```

> _Press `Y` if prompted to terminate batches_

### 18. Clean Up Kafkaesque & ZooKeeper State

```bash
rm -rf .var
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  Remove-Item .var -Recurse
  ```

### 19. Clean Up `Orders` Table

> _Refer back to **[Section 1D → Step 4](/chapter_1//section_1d/README.md#4-ensure-the-app_db_endpoint-environment-variable-is-set)** to set the `APP_DB_ENDPOINT` environment variable._

```bash
docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 \
  mysql -h $APP_DB_ENDPOINT -u admin \
  --table -e "USE services_db; TRUNCATE TABLE Orders;"
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**, run the command on a single line (no line breaks):
  ```bash
  docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 mysql -h $APP_DB_ENDPOINT -u admin --table -e "USE services_db; TRUNCATE TABLE Orders;"
  ```

<br>
