# 📺 Kafka – Section 5a

In this section, we **package Kafkaesque** for distribution and publish it to **Test PyPI**. We then verify that the published package works end to end by installing it into a clean environment and running the full Kafkaesque + e-commerce workflow using the released package instead of the local source code.

- **Part 1 — Test PyPI Packaging & Publish Flow**:  
  We create a Test PyPI account and API token, configure local publishing credentials, add the required packaging files, build and publish Kafkaesque, and prepare the e-commerce app to install Kafkaesque directly from Test PyPI.

- **Part 2 — Published Package Validation**:  
  We install Kafkaesque from Test PyPI into a clean environment, launch the brokers and e-commerce app from that installed package, produce all four orders, and verify broker state, log files, and database persistence end to end.

<div align="center">
    <img src="./section_5a_design.png" alt="System Architecture Diagram" width="2000"/>
</div>

## 🎥 Video Walkthrough

### 🔹 Part 1: Test PyPI Packaging & Publish Flow

**Title:** Kafka – Section 5a (Part 1)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998953#overview)

### 🔹 Part 2: Published Package Validation

**Title:** Kafka – Section 5a (Part 2)  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998955#overview)

# ⚙️ Instructions and Commands

## ✏️ Part 1 – Test PyPI Packaging & Publish Flow

From `~/Desktop/kafka_demo` (project root):

### 1. Create or Update PyPI Configuration File

Create the file (if it doesn't already exist):

```bash
touch ~/.pypirc
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item ~/.pypirc
  ```

Verify the file contents:

```bash
cat ~/.pypirc
```

Open `~/.pypirc` in the code editor and add the following **TestPyPI** configuration:

```bash
[distutils]
index-servers = testpypi

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = <YOUR_TEST_PYPI_API_TOKEN>
```

> _Replace `<YOUR_TEST_PYPI_API_TOKEN>` with the API token generated on [test.pypi.org](https://test.pypi.org)_

Verify the updated configuration:

```bash
cat ~/.pypirc
```

### 2. Create `LICENSE` File

```bash
touch LICENSE
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item LICENSE
  ```

> _Paste in `LICENSE` starter code._

### 3. Create `pyproject` Configuration File

```bash
touch pyproject.toml
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item pyproject.toml
  ```

> _Paste in `pyproject.toml` starter code._

### 4. Create `README` File

```bash
touch README.md
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item README.md
  ```

> _Paste in `README.md` starter code._

### 5. Build & Publish to TestPyPI

Make sure your virtual environment is activated:

```bash
source venv/bin/activate
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\venv\Scripts\Activate.ps1
  ```

Install or upgrade the required packaging tools:

```bash
pip install --upgrade build twine
```

Build the package:

```bash
python -m build
```

> _After running this command, a `dist/` directory should be created containing the package artifacts._

Upload the package to **TestPyPI**:

```bash
python -m twine upload -r testpypi dist/*
```

### 6. Create `e_commerce_app_kafkaesque` Requirements File

```bash
touch e_commerce_app_kafkaesque/requirements.txt
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item e_commerce_app_kafkaesque/requirements.txt
  ```

> _Paste in `requirements.txt` starter code._

<br>

## ✏️ Part 2 – Published Package Validation

### 1. Create the Release Test Directory

Create `kafkaesque_pypi_release_test`:

```bash
mkdir -p ~/Desktop/kafkaesque_pypi_release_test
```

Open the folder in VS Code:

```bash
code -n ~/Desktop/kafkaesque_pypi_release_test
```

> _Alternatively, you can also drag the `kafkaesque_pypi_release_test` folder directly into VS Code._

### 2. Set Up Test Virtual Environment

> <span style="color: gray;">📂 **Working directory:** `~/Desktop/kafkaesque_pypi_release_test`</span>

Create test virtual environment:

```bash
python3 -m venv test_venv
```

- Alternatively (on some systems):
  ```bash
  python -m venv test_venv
  ```

Activate test virtual environment:

```bash
source test_venv/bin/activate
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  .\test_venv\Scripts\Activate.ps1
  ```

- 💬 **Note**: If activation fails, you may need to allow script execution first:
  ```bash
  Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned -Force
  ```

### 3. Copy Over `e_commerce_app_kafkaesque`

```bash
rsync -a \
  --exclude='__pycache__' \
  ~/Desktop/kafka_demo/e_commerce_app_kafkaesque/ \
  ~/Desktop/kafkaesque_pypi_release_test/e_commerce_app_kafkaesque/
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell** use `robocopy`:
  ```bash
  robocopy `
    $HOME\Desktop\kafka_demo\e_commerce_app_kafkaesque `
    $HOME\Desktop\kafkaesque_pypi_release_test\e_commerce_app_kafkaesque `
    /E /XD __pycache__
  ```

### 4. Install Requirements

```bash
pip install -r e_commerce_app_kafkaesque/requirements.txt
```

> _After running this command, the `test_venv/lib` directory should contain a `kafkaesque` folder._

### 5. Start `zkServer` & `zkCli`

---

> Switch back to the `~/Desktop/kafka_demo` directory.

---

Start the ZooKeeper Server in foreground:

```bash
./apache-zookeeper-3.8.4-bin/bin/zkServer.sh start-foreground
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\apache-zookeeper-3.8.4-bin\bin\zkServer.cmd
  ```

Start ZooKeeper CLI:

```bash
./apache-zookeeper-3.8.4-bin/bin/zkCli.sh
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\apache-zookeeper-3.8.4-bin\bin\zkCli.cmd
  ```

---

> After launching both `zkServer` & `zkCli`, switch back to the `~/Desktop/kafkaesque_pypi_release_test` directory.

---

### 6. Launch Kafkaesque `broker_a`

Make sure that the test virtual environment is activated:

```bash
source test_venv/bin/activate
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\test_venv\Scripts\Activate.ps1
  ```

Launch `broker_a`:

```bash
BROKER_PORT=19092 BROKER_NAME=broker_a kafkaesque
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:BROKER_PORT="19092"; $env:BROKER_NAME="broker_a"; kafkaesque
  ```

### 7. Launch Kafkaesque `broker_b`

Make sure that the test virtual environment is activated:

```bash
source test_venv/bin/activate
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\test_venv\Scripts\Activate.ps1
  ```

Launch `broker_b`:

```bash
BROKER_PORT=29092 BROKER_NAME=broker_b kafkaesque
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:BROKER_PORT="29092"; $env:BROKER_NAME="broker_b"; kafkaesque
  ```

### 8. Create Topics on Standby Broker (`minISR=2` for Data Topics)

Create the `Order` and `Payment` data topics with `partitions=2`, `RF=2` and `minISR=2`.

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

Create the internal `__consumer_offsets` topic with `partitions=2`, `RF=2` and no `minISR` value.

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

_Verify that the correct folders and partition files have been created under the `.var` directory._

### 9. Ensure `APP_DB_ENDPOINT` Environment Variable Is Set

---

> Switch back to the `~/Desktop/kafka_demo` directory.

---

Check whether the variable is already set:

```bash
echo $APP_DB_ENDPOINT
```

If the variable is not set  
Navigate to the `terraform/rds` directory:

```bash
cd terraform/rds
```

Retrieve the database endpoint from Terraform and assign it to `APP_DB_ENDPOINT`:

```bash
APP_DB_ENDPOINT=$(terraform output -raw app_db_endpoint)
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  $APP_DB_ENDPOINT = terraform output -raw app_db_endpoint
  ```

Verify the value was set correctly:

```bash
echo $APP_DB_ENDPOINT
```

---

> Copy the endpoint value and switch back to the `~/Desktop/kafkaesque_pypi_release_test` directory.

---

Set the environment variable in the new terminal window:

```bash
APP_DB_ENDPOINT=<YOUR_RDS_ENDPOINT>
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:

  ```bash
  $APP_DB_ENDPOINT="<YOUR_RDS_ENDPOINT>"
  ```

> _Replace `<YOUR_RDS_ENDPOINT>` with the value you copied earlier._

### 10. Launch `e_commerce_app_kafkaesque`

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

### 11. Produce All 4 Test Orders (`order_1`, `order_2`, `order_3` and `order_4`)

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

curl -X POST http://localhost:5001/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "order",
    "key": "order_3",
    "event": {
      "event_type": "OrderPlaced",
      "order_id": "order_3",
      "user_id": "user_1",
      "items": [
        { "product_id": "prod_4", "quantity": 1 }
      ],
      "total_amount": 2.13,
      "timestamp": "2025-01-01T10:01:00Z"
    }
  }'

curl -X POST http://localhost:5001/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "order",
    "key": "order_4",
    "event": {
      "event_type": "OrderPlaced",
      "order_id": "order_4",
      "user_id": "user_1",
      "items": [
        { "product_id": "prod_5", "quantity": 1 }
      ],
      "total_amount": 4.11,
      "timestamp": "2025-01-01T10:01:30Z"
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

  curl.exe -X POST http://localhost:5001/produce `
    -H "Content-Type: application/json" `
    -d '{
      \"topic\": \"order\",
      \"key\": \"order_3\",
      \"event\": {
        \"event_type\": \"OrderPlaced\",
        \"order_id\": \"order_3\",
        \"user_id\": \"user_1\",
        \"items\": [
          { \"product_id\": \"prod_4\", \"quantity\": 1 }
        ],
        \"total_amount\": 2.13,
        \"timestamp\": \"2025-01-01T10:01:00Z\"
      }
    }'

  curl.exe -X POST http://localhost:5001/produce `
    -H "Content-Type: application/json" `
    -d '{
      \"topic\": \"order\",
      \"key\": \"order_4\",
      \"event\": {
        \"event_type\": \"OrderPlaced\",
        \"order_id\": \"order_4\",
        \"user_id\": \"user_1\",
        \"items\": [
          { \"product_id\": \"prod_5\", \"quantity\": 1 }
        ],
      \"total_amount\": 4.11,
      \"timestamp\": \"2025-01-01T10:01:30Z\"
    }
  }'
  ```

### 12. Verify Internal State on `broker_a` and `broker_b`

Hit the debug endpoints:

```bash
curl http://localhost:19092/debug
curl http://localhost:29092/debug
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe http://localhost:19092/debug
  curl.exe http://localhost:29092/debug
  ```

### 13. Shut Down Kafkaesque Brokers and `e_commerce_app_kafkaesque`

In the terminal windows running `broker_a` and `broker_b`, stop each process:

```bash
Ctrl + C
```

Stop the `e_commerce_app_kafkaesque` process:

```bash
Ctrl + C
```

### 14. Verify Partition Files

```bash
for f in .var/kafkaesque/*/*/*.log; do echo "== $f =="; cat "$f"; done
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  Get-ChildItem .var\kafkaesque\*\*\*.log | ForEach-Object {
    $r=$_.FullName.Replace((Get-Location).Path + '\','')
    "== $r =="; Get-Content $_ }
  ```

### 15. Verify Orders in the Database

```bash
docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 \
  mysql -h $APP_DB_ENDPOINT -u admin \
  --table -e "USE services_db; SELECT * FROM Orders;"
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**, run the command on a single line (no line breaks):
  ```bash
  docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 mysql -h $APP_DB_ENDPOINT -u admin --table -e "USE services_db; SELECT * FROM Orders;"
  ```

### 16. Close `kafkaesque_pypi_release_test` & Return to `kafka_demo`

Close the `kafkaesque_pypi_release_test` VS Code window and switch back to the `kafka_demo` VS Code window.

### 17. Shut Down ZooKeeper CLI & Server

In the terminal windows running `zkCli` and `zkServer`, stop each process:

```bash
Ctrl + C
```

> _Press `Y` if prompted to terminate batches_

### 18. Clean Up Local State

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

### 20. Delete `kafkaesque_pypi_release_test` Folder

```bash
rm -rf ~/Desktop/kafkaesque_pypi_release_test
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  Remove-Item ~/Desktop/kafkaesque_pypi_release_test -Recurse
  ```

<br>
