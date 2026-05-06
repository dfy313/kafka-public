# 📺 Kafka – Section 2a

In this section, we begin building **Kafkaesque** — our own Kafka-inspired broker implemented from scratch in Python. We scaffold the core packages, initialize on-disk state, and expose basic HTTP endpoints for creating and inspecting topics. This intentionally simplified version gives us a working broker foundation that we’ll extend with partitions, replication, consumers, and offsets in future sections.

<div align="center">
    <img src="./section_2a_design.png" alt="System Architecture Diagram" width="500"/>
</div>

## 🎥 Video Walkthrough

**Title:** Kafka – Section 2a  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998843#overview)

# ⚙️ Instructions and Commands

From `~/Desktop/kafka_demo` (project root):

### 1. Create Kafkaesque Folder Structure

Create the `kafkaesque` folder:

```bash
mkdir kafkaesque
```

Create the package initializer:

```bash
touch kafkaesque/__init__.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item kafkaesque/__init__.py
  ```

Create the entrypoint file:

```bash
touch kafkaesque/__main__.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item kafkaesque/__main__.py
  ```

_Paste in `__main__.py` starter code._

### 2. Scaffold Kafkaesque Broker Package

Create the broker folder:

```bash
mkdir kafkaesque/broker
```

Create the broker's package initializer:

```bash
touch kafkaesque/broker/_init__.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item kafkaesque/broker/__init__.py
  ```

Create the broker's `app.py` file:

```bash
touch kafkaesque/broker/app.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item kafkaesque/broker/app.py
  ```

_Paste in starter broker `app.py` starter code._

Create the broker's utility file:

```bash
touch kafkaesque/broker/_util.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item kafkaesque/broker/_util.py
  ```

_Paste in broker `_util.py` starter code._

### 3. Launch Kafkaesque Broker

Before launching the Kafkaesque broker, make sure your virtual environment is created and activated. You can revisit **[Section 1D → Step 2](/chapter_1/section_1d/README.md#2-set-up-a-virtual-environment-and-install-dependencies)** for the specific commands.

```bash
python -m kafkaesque
```

_After launch, make sure the `.var` folder is created along with the nested `kafkaesque` and `default_broker` subfolders._

Hit the health check endpoint:

```bash
curl http://localhost:19092/healthz
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe http://localhost:19092/healthz
  ```

### 4. Create Kafkaesque Topics

Create the `Order` and `Payment` topics, both with 1 partition & a replication factor of 1.

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

_Verify that the topic folders get created under `.var/kafkaesque/default_broker`, along with empty partition files._

### 5. Describe Topics Endpoint

Hit the topics describe endpoints

```bash
curl http://localhost:19092/topics/order
curl http://localhost:19092/topics/payment
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe http://localhost:19092/topics/order
  curl.exe http://localhost:19092/topics/payment
  ```

### 6. Verify Internal Broker State

Hit the debug endpoint

```bash
curl http://localhost:19092/debug
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe http://localhost:19092/debug
  ```

### 7. Shutdown & Reset Environment

Stop the Kafkaesque broker

```bash
Ctrl + C
```

Cleanup Kafkaesque broker data

```bash
rm -rf .var
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  Remove-Item .var -Recurse
  ```

<br>
