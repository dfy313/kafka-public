# 📺 Kafka – Section 1e

In this section, we’ll extend our **E-Commerce App** by adding the **Payment Service**, which plays a dual role in our event-driven architecture.
It acts as a **Kafka consumer**, listening for new order events from the `order` topic, and as a **Kafka producer**, publishing corresponding payment events to the `payment` topic.
We'll learn how to subscribe to a topic, process incoming messages, emit new events, and verify that both sides of the pipeline are working correctly — from order creation, to payment confirmation, all the way to messages flowing through Kafka and persisting in the database.

<div align="center">
    <img src="./section_1e_design.png" alt="System Architecture Diagram" width="800"/>
</div>

## 🎥 Video Walkthrough

**Title:** Kafka – Section 1e  
**Link:** [Watch on Udemy](https://www.udemy.com)

# ⚙️ Instructions and Commands

### 1. Add the Payment Service

From project root (`kafka_demo`):

```bash
touch e_commerce_app/services/payment_service.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item e_commerce_app/services/payment_service.py
  ```

Then paste in starter code

### 2. Prepare a Clean, Running Environment (Kafka + DB)

Before testing this section, make sure you’re starting from a clean environment by resetting both the database and Kafka state. Additionally, ensure your Kafka cluster is running with all required topics created. For the specific commands, you can revisit:

- [Section 1D → Step 8](../section_1d/README.md#8-cleanup-reset-for-future-tests) — DB + Kafka cleanup
- [Section 1C → Steps 3-4](../section_1c/README.md#3-start-the-cluster) — Start Kafka cluster + create topics

### 3. Verify existing consumer groups (before launching E-Commerce App)

Check the current list of consumer groups — it should currently be empty, since no services are running yet.

```bash
docker exec -it kafka-kraft kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**, run the command on a single line (no line breaks):
  ```bash
  docker exec -it kafka-kraft kafka-consumer-groups --bootstrap-server localhost:9092 --list
  ```

### 4. Launch the E-Commerce App

Before running the app, make sure your virtual environment is created and activated. You can revisit **[Section 1D → Step 2](../section_1d/README.md#2-set-up-a-virtual-environment-and-install-dependencies)** for the specific commands.

Additionally, ensure that the `APP_DB_ENDPOINT` environment variable is properly set. You can revisit **[Section 1D → Step 4](../section_1d/README.md#4-ensure-the-app_db_endpoint-environment-variable-is-set)** for the specific commands.

```bash
KAFKA_BOOTSTRAP=localhost:9092 \
  DB_HOST=$APP_DB_ENDPOINT \
  python -m e_commerce_app.launcher
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:KAFKA_BOOTSTRAP = "localhost:9092"
  $env:DB_HOST = $APP_DB_ENDPOINT
  python -m e_commerce_app.launcher
  ```

### 5. Verify consumer groups (after starting)

```bash
docker exec -it kafka-kraft kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**, run the command on a single line (no line breaks):
  ```bash
  docker exec -it kafka-kraft kafka-consumer-groups --bootstrap-server localhost:9092 --list
  ```

You should now see `payment_service` in the list.

### 6. Produce a Test Order Event for `order_1`

Refer back to **[Section 1D → Step 6](../section_1d/README.md#6-produce-a-test-order-event-for-order_1)** for the exact command to produce the `order_1` test event.

### 7. Verify Order in the Database

Refer back to **[Section 1D → Step 7](../section_1d/README.md#7-verify-order-in-the-database)** for the exact command to display all records in the `Orders` table.

### 8. Read 1 Message from Each Topic (`Order` & `Payment`)

```bash
docker exec -it kafka-kraft bash -lc '
for t in order payment; do
  echo === $t ===
  kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic "$t" --from-beginning --max-messages 1
done'
```

### 9. Inspect the internal `__consumer_offsets` topic

```bash
docker exec -it kafka-kraft kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic __consumer_offsets \
  --from-beginning \
  --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter"
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**, run the command on a single line (no line breaks) and escape the `$OffsetsMessageFormatter` portion using `` `$ ``:
  ```bash
  docker exec -it kafka-kraft kafka-console-consumer --bootstrap-server localhost:9092 --topic __consumer_offsets --from-beginning --formatter "kafka.coordinator.group.GroupMetadataManager`$OffsetsMessageFormatter"
  ```

### 10. Cleanup: Reset for Future Tests

You can revisit **[Section 1D → Step 9](../section_1d/README.md#9-cleanup-reset-for-future-tests)** for the specific commands.

<br>
