provider "aws" {
  region     = "us-east-2"           # Set this to the AWS region closest to you
  access_key = "<ACCESS_KEY_ID>"     # UNSAFE, ONLY FOR TESTING
  secret_key = "<SECRET_ACCESS_KEY>" # UNSAFE, ONLY FOR TESTING
}

# Uses default VPC
data "aws_vpc" "default" {
  default = true
}

variable "inbound_rules" {
  type    = list(number)
  default = [80, 443, 22, 3306, 2181, 19092, 29092, 5000, 5001, 5002, 5003, 5102, 5103]
}

resource "aws_security_group" "app_security_group" {
  name   = "e-commerce-kafkaesque-full-stack-app-sg"
  vpc_id = data.aws_vpc.default.id
  # Inbound Rules
  dynamic "ingress" {
    iterator = port
    for_each = var.inbound_rules
    content {
      from_port   = port.value
      to_port     = port.value
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }
  # Outbound Rules: allow all protocols, all ports, to anywhere (IPv4)
  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "app_db" {
  identifier             = "e-commerce-full-stack-kafkaesque-mysql-db"
  db_name                = "services_db" # Initial DB Name
  instance_class         = "db.t4g.micro"
  engine                 = "mysql"
  engine_version         = "8.0.42"
  username               = "admin"
  password               = "Password100!"
  port                   = 3306
  allocated_storage      = 20
  publicly_accessible    = true
  skip_final_snapshot    = true
  deletion_protection    = false
  vpc_security_group_ids = [aws_security_group.app_security_group.id]
}

# If you encounter shell parsing errors, ensure this file is using LF line endings instead of CRLF
# null_resource uses local Docker — your local machine running Terraform must have Docker available
resource "null_resource" "init_orders_table" {
  depends_on = [aws_db_instance.app_db]
  triggers = {
    endpoint = aws_db_instance.app_db.address
  }

  # Indentation Matters!
  provisioner "local-exec" {
    ### ⚠️ Windows Users: Uncomment interpreter line below:
    # interpreter = ["C:/Program Files/Git/bin/bash.exe", "-lc"]
    ### ⚠️ macOS/Linux Users: Uncomment interpreter line below:
    # interpreter = ["/bin/bash", "-c"]
    command = <<EOT
ENDPOINT=${aws_db_instance.app_db.address}
for i in $(seq 1 30); do
  if docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 \
      mysql -h "$ENDPOINT" -u admin -e 'SELECT 1;' >/dev/null 2>&1; then
    echo "DB is up."
    break
  fi
  echo "Waiting for DB..."
  sleep 10
done

docker run --rm -e MYSQL_PWD='Password100!' mysql:8.0 \
  mysql -h "$ENDPOINT" -u admin \
  --batch --skip-column-names \
  -e "USE services_db;
      CREATE TABLE IF NOT EXISTS Orders (
          order_id VARCHAR(64) PRIMARY KEY,
          user_id  VARCHAR(64) NOT NULL,
          total_amount DECIMAL(10,2) NOT NULL,
          created_timestamp DATETIME NOT NULL,
          items JSON NOT NULL
      );"
EOT
  }
}

resource "aws_instance" "zookeeper_ec2" {
  ami                    = "ami-0cfde0ea8edd312d4" # Ubuntu 24.04
  instance_type          = "t2.micro"
  vpc_security_group_ids = [aws_security_group.app_security_group.id]
  key_name               = "ecommerce-app-fullstack-keypair"
  tags                   = { Name = "kafkaesque_zookeeper_ec2" }

  user_data = <<-EOT
    #!/bin/bash
    set -euxo pipefail

    # --- Install Java + tools ---
    apt-get update -y
    apt-get install -y openjdk-11-jre-headless wget

    # --- Download and unpack ZooKeeper ---
    cd /opt
    wget https://archive.apache.org/dist/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz
    tar -xzf apache-zookeeper-3.8.4-bin.tar.gz
    cp apache-zookeeper-3.8.4-bin/conf/zoo_sample.cfg apache-zookeeper-3.8.4-bin/conf/zoo.cfg
    ./apache-zookeeper-3.8.4-bin/bin/zkServer.sh start
  EOT
}

resource "aws_instance" "kafkaesque_ec2" {
  ami                    = "ami-0cfde0ea8edd312d4" # Ubuntu 24.04
  instance_type          = "t2.micro"
  vpc_security_group_ids = [aws_security_group.app_security_group.id]
  key_name               = "ecommerce-app-fullstack-keypair"
  tags                   = { Name = "kafkaesque_broker_ec2" }

  user_data = <<-EOT
    #!/bin/bash
    set -euxo pipefail

    # --- Install Docker CE + Compose plugin ---
    apt-get update -y
    apt-get install -y ca-certificates curl gnupg netcat-openbsd
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
    https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $UBUNTU_CODENAME) stable" \
    > /etc/apt/sources.list.d/docker.list
    apt-get update -y
    apt-get install -y docker.io docker-compose-plugin
    systemctl enable --now docker

    ZK_HOST="${aws_instance.zookeeper_ec2.private_dns}"
    ZK_PORT=2181
    PRIVATE_DNS="$(hostname -f 2>/dev/null || true)"
    # ⚠️ Replace <username-kafkaesque> and update the <version_number> to match your PyPI release
    KAFKAESQUE_PACKAGE="<username-kafkaesque>==<version_number>"
  
    # --- Wait for ZooKeeper ---
    for i in $(seq 1 30); do
      if nc -z -w 2 "$ZK_HOST" "$ZK_PORT"; then
        echo "ZooKeeper TCP port is up at $ZK_HOST:$ZK_PORT"
        break
      fi
      echo "Waiting for ZooKeeper at $ZK_HOST:$ZK_PORT ... ($i/30)"
      sleep 5
    done

    # --- Create shared Kafkaesque data root ---
    mkdir -p /home/ubuntu/.var

    # --- Start kafkaesque broker_a on port 19092 ---
    docker rm -f kafkaesque-broker-a 2>/dev/null || true
    docker run -d --name kafkaesque-broker-a --restart unless-stopped \
      -p 19092:19092 \
      -v /home/ubuntu/.var:/.var \
      -e BROKER_PORT=19092 \
      -e BROKER_NAME=broker_a \
      -e BROKER_HOST="$PRIVATE_DNS" \
      -e ZK_CONNECT="$ZK_HOST:$ZK_PORT" \
      -e KAFKAESQUE_PACKAGE="$KAFKAESQUE_PACKAGE" \
      python:3.11-slim bash -lc '\
        pip install -U pip && \
        pip install -i https://test.pypi.org/simple --extra-index-url https://pypi.org/simple "$${KAFKAESQUE_PACKAGE}" && \
        PYTHONUNBUFFERED=1 kafkaesque \
      '

    # --- Start kafkaesque broker_b on port 29092 ---
    docker rm -f kafkaesque-broker-b 2>/dev/null || true
    docker run -d --name kafkaesque-broker-b --restart unless-stopped \
      -p 29092:29092 \
      -v /home/ubuntu/.var:/.var \
      -e BROKER_PORT=29092 \
      -e BROKER_NAME=broker_b \
      -e BROKER_HOST="$PRIVATE_DNS" \
      -e ZK_CONNECT="$ZK_HOST:$ZK_PORT" \
      -e KAFKAESQUE_PACKAGE="$KAFKAESQUE_PACKAGE" \
      python:3.11-slim bash -lc '\
        pip install -U pip && \
        pip install -i https://test.pypi.org/simple --extra-index-url https://pypi.org/simple "$${KAFKAESQUE_PACKAGE}" && \
        PYTHONUNBUFFERED=1 kafkaesque \
      '

    # Wait for broker_a HTTP to be ready (we'll use it for topic creation)
    BROKER_A_HTTP="localhost:19092"
    for i in $(seq 1 40); do
      if curl -sf "$BROKER_A_HTTP/healthz" >/dev/null; then
        echo "kafkaesque broker_a is up."
        break
      fi
      echo "Waiting for broker_a... ($i/40)"; sleep 3
    done

    # Create Data Topics
    for t in order payment __consumer_offsets; do
      curl -sS -X POST "$BROKER_A_HTTP/topics" \
        -H 'Content-Type: application/json' \
        -d "{\"name\":\"$t\",\"partitions\":2,\"replication_factor\":2,\"minISR\":2}" \
      || true
    done

    # Create Internal Offsets Topics
    curl -sS -X POST "$BROKER_A_HTTP/topics" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"__consumer_offsets\",\"partitions\":2,\"replication_factor\":2}" \
    || true
  EOT
}

resource "aws_instance" "e_commerce_app_kafkaesque_ec2" {
  ami                    = "ami-0cfde0ea8edd312d4" # Ubuntu 24.04
  instance_type          = "t2.micro"
  vpc_security_group_ids = [aws_security_group.app_security_group.id]
  key_name               = "ecommerce-app-fullstack-keypair"
  tags                   = { Name = "e_commerce_app_kafkaesque_terraform_ec2" }

  user_data = <<-EOT
    #!/bin/bash
    set -euxo pipefail

    # --- Install Docker CE + Compose plugin ---
    apt-get update -y
    apt-get install -y ca-certificates curl gnupg
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
    https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $UBUNTU_CODENAME) stable" \
    > /etc/apt/sources.list.d/docker.list
    apt-get update -y
    apt-get install -y docker.io docker-compose-plugin
    systemctl enable --now docker

    # --- Config ---
    CONTAINER="e-commerce-app-kafkaesque"
    IMAGE="<YOUR_DOCKERHUB_USERNAME>/$CONTAINER:latest"
    BROKER_A="${aws_instance.kafkaesque_ec2.public_dns}:19092"
    BROKER_B="${aws_instance.kafkaesque_ec2.public_dns}:29092"
    DB_HOST="${aws_db_instance.app_db.address}"

    # --- Pull image (pin platform to avoid arch mismatch) ---
    docker pull --platform=linux/amd64 "$IMAGE" || true

    # --- Best-effort wait for Kafka broker to accept TCP on 19092 ---
    for i in $(seq 1 24); do
      if curl -sf "$BROKER_A/healthz" >/dev/null; then
        echo "kafkaesque broker reachable at $BROKER_A"
        break
      fi
      echo "Waiting for broker $BROKER_A ... ($i/24)"
      sleep 5
    done || true

    # --- Run container (ENTRYPOINT already runs python -m e_commerce_app_kafkaesque.launcher) ---
    docker rm -f "$CONTAINER" 2>/dev/null || true
    docker run -d --name "$CONTAINER" --restart unless-stopped \
      -e KAFKA_BOOTSTRAP="$BROKER_A,$BROKER_B" \
      -e DB_HOST="$DB_HOST" \
      -p 5001:5001 -p 5002:5002 -p 5003:5003 -p 5102:5102 -p 5103:5103 \
      "$IMAGE"
  EOT
}

output "fullstack_kafkaesque_db_endpoint" {
  description = "RDS endpoint for the services_db MySQL instance"
  value       = aws_db_instance.app_db.address
}

output "kafkaesque_bootstrap_url" {
  description = "Public bootstrap address for the Kafkaesque broker"
  value       = aws_instance.kafkaesque_ec2.public_dns
}

output "e_commerce_app_kafkaesque_url" {
  description = "Base URL for the e-commerce-app-kafkaesque application"
  value       = aws_instance.e_commerce_app_kafkaesque_ec2.public_dns
}

output "zookeeper_url" {
  description = "Url for the Zookeeper ensemble"
  value       = aws_instance.zookeeper_ec2.public_dns
}
