provider "aws" {
  region     = "us-east-2"
  access_key = "<ACCESS_KEY_ID>"     # UNSAFE, ONLY FOR TESTING
  secret_key = "<SECRET_ACCESS_KEY>" # UNSAFE, ONLY FOR TESTING
}

# Uses default VPC
data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "dbtraffic" {
  name   = "e-commerce-app-db-security-group"
  vpc_id = data.aws_vpc.default.id
  # Inbound Rules: allow tcp on port 3306
  ingress {
    protocol    = "tcp"
    from_port   = 3306
    to_port     = 3306
    cidr_blocks = ["0.0.0.0/0"]
  }
  # Outbound Rules: allow all protocols, all ports, to anywhere (IPv4)
  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "eCommerceAppDB" {
  identifier             = "e-commerce-app-db"
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
  vpc_security_group_ids = [aws_security_group.dbtraffic.id]
}

resource "null_resource" "init_orders_table" {
  depends_on = [aws_db_instance.eCommerceAppDB]
  triggers = {
    endpoint = aws_db_instance.eCommerceAppDB.address
  }

  # Indentation Matters!
  provisioner "local-exec" {
    ### ⚠️ Windows Users: Uncomment interpreter line below:
    # interpreter = ["C:/Program Files/Git/bin/bash.exe", "-lc"]
    command = <<EOT
ENDPOINT=${aws_db_instance.eCommerceAppDB.address}
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

output "app_db_endpoint" {
  description = "RDS endpoint for the services_db MySQL instance"
  value       = aws_db_instance.eCommerceAppDB.address
}
