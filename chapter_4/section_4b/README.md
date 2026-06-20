# 📺 Kafka – Section 4b

In this section, we introduce a **controller znode** in ZooKeeper and add **watchers** so brokers can maintain a live, in-memory view of the active controller. We publish controller identity to the `/controller` znode, subscribe to changes using ZooKeeper watches, and verify that all brokers stay in sync during startup, failover, and recovery.

<div align="center">
    <img src="./section_4b_design.png" alt="System Architecture Diagram" width="2000"/>
</div>

## 🎥 Video Walkthrough

**Title:** Kafka – Section 4b  
**Link:** [Watch on Udemy](https://www.udemy.com/course/practical-system-design/learn/lecture/55998917#overview)

# ⚙️ Instructions and Commands

From `~/Desktop/kafka_demo` (project root):

### 1. Create ZooKeeper Watchers File

```bash
touch kafkaesque/zookeeper/watchers.py
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  New-Item kafkaesque/zookeeper/watchers.py
  ```

> _Paste in `watchers.py` starter code._

### 2. Start `zkServer` & `zkCli`

Start the ZooKeeper Server in foreground:

```bash
./apache-zookeeper-3.8.4-bin/bin/zkServer.sh start-foreground
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\apache-zookeeper-3.8.4-bin\bin\zkServer.cmd
  ```

> _Verify that the `.var/zookeeper` folder is created_

Start ZooKeeper CLI:

```bash
./apache-zookeeper-3.8.4-bin/bin/zkCli.sh
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\apache-zookeeper-3.8.4-bin\bin\zkCli.cmd
  ```

### 3. Ensure Virtual Environment is Activated

```bash
source venv/bin/activate
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  .\venv\Scripts\Activate.ps1
  ```

### 4. Launch Kafkaesque `broker_a`

```bash
BROKER_PORT=19092 BROKER_NAME=broker_a python -m kafkaesque
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:BROKER_PORT="19092"; $env:BROKER_NAME="broker_a"; python -m kafkaesque
  ```

### 5. Inspect ZooKeeper State

From the ZooKeeper CLI:

```bash
ls /
get /controller
```

### 6. Launch Kafkaesque `broker_b`

> _Please make sure your virtual environment is activated. You can refer back to **[Step 3](#3-ensure-virtual-environment-is-activated)** for the exact command._

```bash
BROKER_PORT=29092 BROKER_NAME=broker_b python -m kafkaesque
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:BROKER_PORT="29092"; $env:BROKER_NAME="broker_b"; python -m kafkaesque
  ```

### 7. Inspect ZooKeeper State

From the ZooKeeper CLI:

```bash
get /controller
```

### 8. Verify Internal State on `broker_a` and `broker_b`

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

### 9. Kill the Controller (`broker_a`)

In `broker_a`'s terminal window, stop the process:

```bash
Ctrl + C
```

### 10. Inspect ZooKeeper State

From the ZooKeeper CLI:

```bash
get /controller
```

### 11. Verify Internal State on `broker_b`

Hit the debug endpoint:

```bash
curl http://localhost:29092/debug
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  curl.exe http://localhost:29092/debug
  ```

### 12. Bring `broker_a` back Online

> _Please make sure your virtual environment is activated. You can refer back to **[Step 3](#3-ensure-virtual-environment-is-activated)** for the exact command._

```bash
BROKER_PORT=19092 BROKER_NAME=broker_a python -m kafkaesque
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  $env:BROKER_PORT="19092"; $env:BROKER_NAME="broker_a"; python -m kafkaesque
  ```

### 13. Inspect ZooKeeper State

From the ZooKeeper CLI:

```bash
get /controller
```

### 14. Verify Internal State on `broker_a` and `broker_b`

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

### 15. Shutdown & Reset Environment

Stop the Kafkaesque brokers:

```bash
Ctrl + C
```

Verify the final state from the ZooKeeper CLI:

```bash
ls /
```

In the terminal windows running `zkCli` and `zkServer`, stop each process:

```bash
Ctrl + C
```

> _Press `Y` if prompted to terminate batches_

Clean up Kafkaesque and ZooKeeper state:

```bash
rm -rf .var
```

- <img src="https://raw.githubusercontent.com/PowerShell/PowerShell/master/assets/powershell_128.svg" width="18" /> On **Windows PowerShell**:
  ```bash
  Remove-Item .var -Recurse
  ```

<br>
