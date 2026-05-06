# 📺 Kafka – Section 2c

In this section, we introduce the **Kafkaesque Consumer API** and wire it into our existing `e_commerce_app_kafkaesque` application, completing the core producer–consumer workflow.

- **Part 1 — Kafkaesque Consumer API**:  
  We implement the `KafkaesqueConsumer` class and add consumer-specific data structures to the `kafkaesque/structs.py` file.

- **Part 2 — Broker-Side Consumer Support**:
  We extend the broker with the endpoints needed to support the newly added Consumer API.

- **Part 3 — App Integration and Validation**:  
  We migrate the `e_commerce_app_kafkaesque` to use `KafkaesqueConsumer` and validate the full end-to-end flow by producing `order_1` and `order_2`.

<div align="center">
    <img src="./section_2c_design.png" alt="System Architecture Diagram" width="1000"/>
</div>

## 🎥 Video Walkthrough

### 🔹 Part 1: Kafkaesque Consumer API

**Title:** Kafka – Section 2c (Part 1)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998857#overview)

### 🔹 Part 2: Broker-Side Consumer Support

**Title:** Kafka – Section 2c (Part 2)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998859#overview)

### 🔹 Part 3: App Integration and Validation

**Title:** Kafka – Section 2c (Part 3)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998863#overview)

# ⚙️ Instructions and Commands

## ✏️ Part 1 – Kafkaesque Consumer API

From `~/Desktop/kafka_demo` (project root):

### 1. Introduce Consumer API

```bash
touch kafkaesque/api/consumer_api.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item kafkaesque/api/consumer_api.py
  ```

_Paste in starter `consumer_api` starter code._

### 2. Update Structs File

Update the existing `kafkaesque/structs.py` file to include `TopicPartition` and `OffsetAndMetadata`.

<br>

## ✏️ Part 2 – Broker-Side Consumer Support

Add updated code to `broker/app.py`, `broker/_util.py` and `kafkaesque/__init__.py`.

<br>

## ✏️ Part 3 – App Integration and Validation

### 1. Update `e_commerce_app_kafkaesque` Code

Add updated code to `launcher.py`, `service_base.py`, `services/payment_service.py` and `services/notification_service.py`.

### 2. Launch Kafkaesque Broker

Please make sure your virtual environment is created and activated, and that the legacy dependencies are installed. You can revisit **[Section 1D → Step 2](/chapter_1/section_1d/README.md#2-set-up-a-virtual-environment-and-install-dependencies)** for the specific commands.

Additionally, make sure you have the requests library installed. You can revisit **[Section 2B (Part 3) → Step 4](../section_2b/README.md#4-virtual-environment-updates)** for the command.

```bash
python -m kafkaesque
```

### 3. Create Kafkaesque Topics

Create the `Order` and `Payment` data topics, both with 1 partition & a replication factor of 1:

```bash
curl -X POST http://localhost:19092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"order","partitions":1,"replication_factor":1}'

curl -X POST http://localhost:19092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"payment","partitions":1,"replication_factor":1}'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  curl.exe -X POST http://localhost:19092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"order\",\"partitions\":1,\"replication_factor\":1}'

  curl.exe -X POST http://localhost:19092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"payment\",\"partitions\":1,\"replication_factor\":1}'
  ```

Also create the internal `__consumer_offsets` topic with `partitions=1` and `RF=1`:

```bash
curl -X POST http://localhost:19092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"__consumer_offsets","partitions":1,"replication_factor":1}'
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  curl.exe -X POST http://localhost:19092/topics `
    -H 'content-type: application/json' `
    -d '{\"name\":\"__consumer_offsets\",\"partitions\":1,\"replication_factor\":1}'
  ```

Verify correct `.var` structure

### 4. Verify Internal Broker State (Before Launching App)

Hit the debug endpoint:

```bash
curl http://localhost:19092/debug
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe http://localhost:19092/debug
  ```

_Verify that `consumer_groups_cache` is currently empty._

### 5. Launch `e_commerce_app_kafkaesque`

Refer back to **[Section 2B (Part 3) → Step 7](../section_2b/README.md#7-launch-e_commerce_app_kafkaesque)** for the exact command to launch `e_commerce_app_kafkaesque`.

### 6. Verify Internal Broker State (After App Launch)

Refer back to **[Step 4](#4-verify-internal-broker-state-before-launching-app)** for the debug command.

_This time, `consumer_groups_cache` should now be populated._

### 7. Produce a Test Order Event for `order_1`

Refer back to **[Section 2B (Part 3) → Step 8](../section_2b/README.md#8-produce-a-test-order-event-for-order_1)** for the exact command to produce `order_1`.

### 8. Verify `order_1` Outputs

At this point, we should be able to confirm that the `order_1` event was processed correctly across all layers of the system.

- **Database:** Verify that the order was written to MySQL by referring back to **[Section 2B (Part 3) → Step 9](../section_2b/README.md#9-verify-order-in-the-database)**
- **Partition Files:** Inspect the on-disk Kafkaesque logs by referring back to **[Section 2B (Part 3) → Step 10](../section_2b/README.md#10-verify-partition-files)**
- **Internal Broker State:** Confirm the broker’s in-memory state using the debug endpoint by referring back to **[Step 4](#4-verify-internal-broker-state-before-launching-app)**

### 9. Produce a Test Order Event for `order_2`

```bash
curl -X POST http://localhost:5001/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "order",
    "key": "order_2",
    "event": {
      "event_type": "OrderPlaced",
      "order_id": "order_2",
      "user_id": "user_1",
      "items": [
        { "product_id": "prod_3", "quantity": 1 }
      ],
      "total_amount": 39.99,
      "timestamp": "2025-01-01T10:00:30Z"
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
      \"key\": \"order_2\",
      \"event\": {
        \"event_type\": \"OrderPlaced\",
        \"order_id\": \"order_2\",
        \"user_id\": \"user_1\",
        \"items\": [
          { \"product_id\": \"prod_3\", \"quantity\": 1 }
        ],
      \"total_amount\": 39.99,
      \"timestamp\": \"2025-01-01T10:00:30Z\"
    }
  }'
  ```

### 10. Verify `order_2` Outputs

The following outputs should now reflect the updated behavior for `order_2`:

- **Database:** Verify that the order was written to MySQL by referring back to **[Section 2B (Part 3) → Step 9](../section_2b/README.md#9-verify-order-in-the-database)**
- **Partition Files:** Inspect the on-disk Kafkaesque logs by referring back to **[Section 2B (Part 3) → Step 10](../section_2b/README.md#10-verify-partition-files)**
- **Internal Broker State:** Confirm the broker’s in-memory state using the debug endpoint by referring back to **[Step 4](#4-verify-internal-broker-state-before-launching-app)**

### 11. Shutdown & Reset Environment

Refer back to **[Section 2B (Part 3) → Step 12](../section_2b/README.md#12-shutdown--reset-environment)** for the tear down & cleanup commands.

<br>
