# 📺 Kafka – Section 1d

In this section, we’ll create the foundation of our **E-Commerce App** by building its first microservice — the **Order Service**. This service acts solely as a **Kafka producer**, publishing order-related events into the `order` topic whenever a new order is placed. We'll set up a lightweight Flask API, connect it to our existing **MySQL database** and **Kafka cluster**, and test the complete flow — from an HTTP request, to writing the order into MySQL, and finally publishing the corresponding event to Kafka.

<div align="center">
    <img src="./section_1d_design.png" alt="System Architecture Diagram" width="800"/>
</div>

## 🎥 Video Walkthrough

**Title:** Kafka – Section 1d  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998829#overview)

# ⚙️ Instructions and Commands

From the root of your project (`~/Desktop/kafka_demo`)

### 1. Create the Project Structure

Create the `e_commerce_app` folder:

```bash
mkdir e_commerce_app
```

Create the package initializer:

```bash
touch e_commerce_app/__init__.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item e_commerce_app/__init__.py
  ```

Create the `launcher.py` file:

```bash
touch e_commerce_app/launcher.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  New-Item e_commerce_app/launcher.py
  ```

_Then paste in starter code_

Create a folder for your services and add the Order Service:

```bash
mkdir -p e_commerce_app/services
```

```bash
touch e_commerce_app/services/__init__.py
touch e_commerce_app/services/order_service.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  New-Item e_commerce_app/services/__init__.py
  New-Item e_commerce_app/services/order_service.py
  ```

_Then paste in starter code_

Add a shared service base module:

```bash
touch e_commerce_app/service_base.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  New-Item e_commerce_app/service_base.py
  ```

_Then paste in starter code_

### 2. Set Up Virtual Environment and Install Dependencies

Create virtual environment:

```bash
python3 -m venv venv
```

- Alternatively (on some systems):
  ```bash
  python -m venv venv
  ```

Activate virtual environment:

```bash
source venv/bin/activate
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  .\venv\Scripts\Activate.ps1
  ```

- 💬 **Note**: If activation fails, you may need to allow script execution first:
  ```bash
  Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned -Force
  ```

Install dependencies:

```bash
pip install pymysql flask kafka-python
```

### 3. Ensure Kafka Cluster Is Running + Topics Created

Before launching the app, make sure your Kafka cluster is up and topics are created. You can revisit [Section 1C → Steps 3-4](../section_1c/README.md#3-start-the-cluster) for the setup and commands.

### 4. Ensure the `APP_DB_ENDPOINT` Environment Variable Is Set

Navigate into the `terraform/rds` directory and pull the database endpoint directly from Terraform:

```bash
cd terraform/rds
```

Set `APP_DB_ENDPOINT` environment variable:

```bash
APP_DB_ENDPOINT=$(terraform output -raw app_db_endpoint)
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  $APP_DB_ENDPOINT = terraform output -raw app_db_endpoint
  ```

_Alternatively, you can retrieve the endpoint manually from the AWS Console:_

- Go to **AWS Console → Aurora and RDS** and select `e-commerce-app-db`
- Copy the value from the **Endpoint & port** section, and set it locally:

```bash
APP_DB_ENDPOINT=<YOUR_RDS_ENDPOINT>
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  $APP_DB_ENDPOINT="<YOUR_RDS_ENDPOINT>"
  ```

### 5. Launch the E-Commerce App

Run the Flask App with your environment variables configured:

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

### 6. Produce a Test Order Event for `order_1`

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

### 7. Verify Order in the Database

Refer back to **[Step 4](#4-ensure-the-app_db_endpoint-environment-variable-is-set)** to set the `APP_DB_ENDPOINT` environment variable.

```bash
docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 \
  mysql -h $APP_DB_ENDPOINT -u admin \
  --table -e "USE services_db; SELECT * FROM Orders;"
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 `
    mysql -h $APP_DB_ENDPOINT -u admin `
    --table -e "USE services_db; SELECT * FROM Orders;"
  ```

### 8. Verify Event in Kafka

```bash
docker exec -it kafka-kraft kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order --from-beginning --max-messages 1
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**, run the command on a single line (no line breaks):
  ```bash
  docker exec -it kafka-kraft kafka-console-consumer --bootstrap-server localhost:9092 --topic order --from-beginning --max-messages 1
  ```

### 9. Cleanup: Reset for Future Tests

In the terminal where the `e_commerce_app` is running, press:

```bash
Ctrl + C
```

Truncate the `Orders` table (_Refer back to [Step 4](#4-ensure-the-app_db_endpoint-environment-variable-is-set) to set the `APP_DB_ENDPOINT` environment variable._)

```bash
docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 \
  mysql -h $APP_DB_ENDPOINT -u admin \
  --table -e "USE services_db; TRUNCATE TABLE Orders;"
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**, run the command on a single line (no line breaks):
  ```bash
  docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 mysql -h $APP_DB_ENDPOINT -u admin --table -e "USE services_db; TRUNCATE TABLE Orders;"
  ```

Tear down Kafka container:

```bash
docker-compose down -v
```

<br>
