provider "aws" {
  region     = "us-east-2"
  access_key = "<ACCESS_KEY_ID>"     # UNSAFE, ONLY FOR TESTING
  secret_key = "<SECRET_ACCESS_KEY>" # UNSAFE, ONLY FOR TESTING
}

# Uses default VPC
data "aws_vpc" "default" {
  default = true
}

variable "inbound_rules" {
  type    = list(number)
  default = [80, 443, 22, 3306, 9092, 5000, 5001, 5002, 5003]
}

resource "aws_security_group" "app_security_group" {
  name   = "e-commerce-full-stack-app-sg"
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
  identifier             = "e-commerce-full-stack-mysql-db"
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

resource "aws_instance" "kafka_ec2" {
  ami                    = "ami-0cfde0ea8edd312d4" # Ubuntu 24.04
  instance_type          = "t2.micro"
  vpc_security_group_ids = [aws_security_group.app_security_group.id]
  key_name               = "ecommerce-app-fullstack-keypair"
  tags                   = { Name = "kafka_terraform_ec2" }

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

    mkdir -p /opt/kafka
    cd /opt/kafka

    # --- Resolve PUBLIC_DNS for advertised listeners ---
    TOKEN="$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
      -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' || true)"
    export PUBLIC_DNS="$(curl -s \
      -H "X-aws-ec2-metadata-token: $TOKEN" \
      "http://169.254.169.254/latest/meta-data/public-hostname" || true)"

    # --- Generate a fresh CLUSTER_ID ---
    CLUSTER_ID="$(docker run --rm confluentinc/cp-kafka:7.6.7 bash -lc 'kafka-storage random-uuid')"
    printf "CLUSTER_ID=%s\n" "$CLUSTER_ID" >> .env

    cat > docker-compose.yml <<'YML'
    version: "3.8"
    services:
      kafka:
        image: confluentinc/cp-kafka:7.6.7
        container_name: kafka-kraft
        restart: unless-stopped
        ports:
          - "9092:9092"
        environment:
          KAFKA_PROCESS_ROLES: "broker,controller"
          KAFKA_NODE_ID: "1"
          KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
          KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-kraft:9093"
          KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
          KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://$${PUBLIC_DNS}:9092"
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
          KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
          KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
          KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
          KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
          KAFKA_HEAP_OPTS: "-Xms256m -Xmx448m"
          KAFKA_LOG_DIRS: "/var/lib/kafka/data"
          CLUSTER_ID: "$${CLUSTER_ID}"
        volumes:
          - kafka_data:/var/lib/kafka/data
    volumes:
      kafka_data:
    YML

    # Bring up Kafka
    docker compose up -d

    # Wait for Kafka to be ready
    BROKER="$${PUBLIC_DNS}:9092"
    for i in $(seq 1 40); do
      if docker exec kafka-kraft kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
        echo "Kafka is up."
        break
      fi
      echo "Waiting for Kafka to be ready... ($i)"
      sleep 3
    done

    # Create topics
    docker exec -e BROKER="$${PUBLIC_DNS}:9092" kafka-kraft bash -lc '
    for t in order payment; do
      kafka-topics \
        --bootstrap-server "$BROKER" \
        --create --if-not-exists \
        --topic "$t" \
        --partitions 1 \
        --replication-factor 1
    done'
  EOT
}

resource "aws_instance" "e_commerce_app_ec2" {
  ami                    = "ami-0cfde0ea8edd312d4" # Ubuntu 24.04
  instance_type          = "t2.micro"
  vpc_security_group_ids = [aws_security_group.app_security_group.id]
  key_name               = "ecommerce-app-fullstack-keypair"
  tags                   = { Name = "e_commerce_app_terraform_ec2" }

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
    CONTAINER="e-commerce-app" 
    IMAGE="<YOUR_DOCKERHUB_USERNAME>/$CONTAINER:latest"
    KAFKA_BOOTSTRAP="${aws_instance.kafka_ec2.public_dns}:9092"
    DB_HOST="${aws_db_instance.app_db.address}"

    # --- Pull image (pin platform to avoid arch mismatch) ---
    docker pull --platform=linux/amd64 "$IMAGE" || true

    # --- Best-effort wait for Kafka broker to accept TCP on 9092 ---
    BROKER_HOST="$${KAFKA_BOOTSTRAP%%:*}"
    for i in $(seq 1 24); do  # ~2 minutes
      if bash -lc "exec 3<>/dev/tcp/$${BROKER_HOST}/9092" 2>/dev/null; then
        echo "Kafka broker reachable at $KAFKA_BOOTSTRAP"
        break
      fi
      echo "Waiting for Kafka broker $KAFKA_BOOTSTRAP ... ($i/24)"
      sleep 5
    done || true

    # --- Run container (ENTRYPOINT already runs python -m e_commerce_app.launcher) ---
    docker rm -f "$CONTAINER" 2>/dev/null || true
    docker run -d --name "$CONTAINER" --restart unless-stopped \
      -e KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
      -e DB_HOST="$DB_HOST" \
      -p 5001:5001 -p 5002:5002 -p 5003:5003 \
      "$IMAGE"
  EOT
}

output "fullstack_db_endpoint" {
  description = "RDS endpoint for the services_db MySQL instance"
  value       = aws_db_instance.app_db.address
}

output "kafka_bootstrap_url" {
  description = "Public bootstrap address for the Kafka broker"
  value       = aws_instance.kafka_ec2.public_dns
}

output "e_commerce_app_url" {
  description = "Base URL for the e-commerce app"
  value       = aws_instance.e_commerce_app_ec2.public_dns
}
