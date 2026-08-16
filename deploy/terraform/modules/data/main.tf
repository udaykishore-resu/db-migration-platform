# Data plane: the customer-managed key, the parts bucket, Aurora and MSK.

variable "name" { type = string }
variable "region" { type = string }
variable "account_id" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "aurora_sg_id" { type = string }
variable "msk_sg_id" { type = string }
variable "aurora_engine" { type = string }
variable "aurora_instance_class" { type = string }
variable "aurora_reader_count" { type = number }
variable "msk_broker_count" { type = number }
variable "msk_instance_type" { type = string }
variable "msk_volume_size" { type = number }

# ---------------------------------------------------------------------- KMS

# One customer-managed key for everything at rest. A single key is easier to
# audit and to revoke than four, and the blast radius is identical: anything able
# to read one of these stores can read the others.
resource "aws_kms_key" "this" {
  description             = "${var.name} migration data at rest"
  enable_key_rotation     = true
  deletion_window_in_days = 30

  tags = { Name = "${var.name}-cmk" }
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name}"
  target_key_id = aws_kms_key.this.key_id
}

# ----------------------------------------------------------------- S3 parts

resource "aws_s3_bucket" "parts" {
  bucket = "${var.name}-parts-${var.account_id}"

  tags = { Name = "${var.name}-parts" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "parts" {
  bucket = aws_s3_bucket.parts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.this.arn
    }
    # Cuts KMS request cost dramatically on a bucket receiving thousands of
    # multi-gigabyte parts, without weakening the encryption.
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "parts" {
  bucket = aws_s3_bucket.parts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "parts" {
  bucket = aws_s3_bucket.parts.id
  versioning_configuration { status = "Enabled" }
}

# Parts are transient by design: once loaded and reconciled they are dead weight,
# and they hold protected production data. Expiring them is a compliance control,
# not housekeeping.
resource "aws_s3_bucket_lifecycle_configuration" "parts" {
  bucket = aws_s3_bucket.parts.id

  rule {
    id     = "expire-loaded-parts"
    status = "Enabled"

    filter { prefix = "" }

    expiration { days = 30 }

    noncurrent_version_expiration { noncurrent_days = 7 }

    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
}

# Deny any request that did not arrive over TLS. Without this, a misconfigured
# client can transfer parts in clear text and nothing will complain.
resource "aws_s3_bucket_policy" "parts" {
  bucket = aws_s3_bucket.parts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.parts.arn, "${aws_s3_bucket.parts.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# --------------------------------------------------------------------- Aurora

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-aurora"
  subnet_ids = var.private_subnet_ids
}

resource "random_password" "master" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "db" {
  name       = "${var.name}/aurora/master"
  kms_key_id = aws_kms_key.this.arn
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id

  secret_string = jsonencode({
    username = "migration"
    password = random_password.master.result
    engine   = var.aurora_engine
    dbname   = "target"
  })
}

resource "aws_rds_cluster" "this" {
  cluster_identifier = "${var.name}-aurora"
  engine             = var.aurora_engine
  engine_mode        = "provisioned"

  database_name   = "target"
  master_username = "migration"
  master_password = random_password.master.result

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [var.aurora_sg_id]

  storage_encrypted = true
  kms_key_id        = aws_kms_key.this.arn

  # A migration writes at a rate ordinary traffic never does. Backups are the
  # cheapest possible insurance against having to re-run the whole load.
  backup_retention_period      = 7
  preferred_backup_window      = "03:00-04:00"
  preferred_maintenance_window = "sun:05:00-sun:06:00"

  # The cluster is deleted only deliberately, and never without a final snapshot.
  deletion_protection       = true
  skip_final_snapshot       = false
  final_snapshot_identifier = "${var.name}-aurora-final"

  enabled_cloudwatch_logs_exports = var.aurora_engine == "aurora-postgresql" ? ["postgresql"] : ["error", "slowquery"]

  # Grants the cluster permission to read parts directly from S3, which is what
  # keeps terabytes out of the worker processes.
  iam_roles = [aws_iam_role.aurora_s3.arn]

  lifecycle {
    ignore_changes = [master_password]
  }
}

resource "aws_rds_cluster_instance" "writer" {
  identifier         = "${var.name}-writer"
  cluster_identifier = aws_rds_cluster.this.id
  instance_class     = var.aurora_instance_class
  engine             = aws_rds_cluster.this.engine

  performance_insights_enabled = true
  monitoring_interval          = 30
  monitoring_role_arn          = aws_iam_role.rds_monitoring.arn
}

resource "aws_rds_cluster_instance" "reader" {
  count = var.aurora_reader_count

  identifier         = "${var.name}-reader-${count.index}"
  cluster_identifier = aws_rds_cluster.this.id
  instance_class     = var.aurora_instance_class
  engine             = aws_rds_cluster.this.engine

  performance_insights_enabled = true
  monitoring_interval          = 30
  monitoring_role_arn          = aws_iam_role.rds_monitoring.arn
}

resource "aws_iam_role" "rds_monitoring" {
  name = "${var.name}-rds-monitoring"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "monitoring.rds.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  role       = aws_iam_role.rds_monitoring.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# The cluster reads parts from S3 itself. Scoped to the parts bucket only.
resource "aws_iam_role" "aurora_s3" {
  name = "${var.name}-aurora-s3-import"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "rds.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "aurora_s3" {
  role = aws_iam_role.aurora_s3.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:GetObjectVersion"]
        Resource = [aws_s3_bucket.parts.arn, "${aws_s3_bucket.parts.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = aws_kms_key.this.arn
      },
    ]
  })
}

# ------------------------------------------------------------------------ MSK

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.name}"
  retention_in_days = 30
}

resource "aws_msk_cluster" "this" {
  cluster_name           = var.name
  kafka_version          = "3.6.0"
  number_of_broker_nodes = var.msk_broker_count

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [var.msk_sg_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.msk_volume_size
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.this.arn

    encryption_in_transit {
      # TLS only. PLAINTEXT is not offered as an option even for the in-VPC path:
      # the change stream carries protected but still sensitive data, and an
      # allowed plaintext listener is one misconfiguration away from being used.
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl { iam = true }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }
}

# Retention must outlast the migration. If the stream ages out before the load
# finishes, the only recovery is a fresh snapshot of the affected tables.
resource "aws_msk_configuration" "this" {
  name           = "${var.name}-config"
  kafka_versions = ["3.6.0"]

  server_properties = <<-PROPS
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=12
    log.retention.hours=336
    unclean.leader.election.enable=false
    compression.type=zstd
  PROPS
}

output "aurora_writer_endpoint" { value = aws_rds_cluster.this.endpoint }
output "aurora_reader_endpoint" { value = aws_rds_cluster.this.reader_endpoint }
output "msk_bootstrap_brokers_tls" { value = aws_msk_cluster.this.bootstrap_brokers_tls }
output "msk_cluster_arn" { value = aws_msk_cluster.this.arn }
output "parts_bucket_name" { value = aws_s3_bucket.parts.id }
output "parts_bucket_arn" { value = aws_s3_bucket.parts.arn }
output "kms_key_arn" { value = aws_kms_key.this.arn }
output "db_secret_arn" { value = aws_secretsmanager_secret.db.arn }
