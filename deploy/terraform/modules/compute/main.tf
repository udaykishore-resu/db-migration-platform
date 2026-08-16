# Compute: one ECS Fargate service per platform component, with least-privilege
# task roles and the alarms that make a stalled migration visible.

variable "name" { type = string }
variable "region" { type = string }
variable "account_id" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "service_sg_id" { type = string }
variable "services" { type = list(string) }
variable "service_desired_count" { type = map(number) }
variable "image_repository" { type = string }
variable "image_tag" { type = string }
variable "parts_bucket_arn" { type = string }
variable "parts_bucket_name" { type = string }
variable "kms_key_arn" { type = string }
variable "secret_arn" { type = string }
variable "msk_cluster_arn" { type = string }
variable "lag_alarm_seconds" { type = number }

resource "aws_ecs_cluster" "this" {
  name = var.name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "services" {
  for_each = toset(var.services)

  name              = "/ecs/${var.name}/${each.value}"
  retention_in_days = 30
}

# ------------------------------------------------------------------- IAM

resource "aws_iam_role" "execution" {
  name = "${var.name}-task-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "execution_secrets" {
  role = aws_iam_role.execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.secret_arn
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = var.kms_key_arn
      },
    ]
  })
}

# A role per service rather than one shared role. The reconciler has no reason to
# write to the parts bucket, and the control plane has no reason to touch the
# broker at all. Shared roles are how a compromise of the least sensitive
# component becomes a compromise of everything.
resource "aws_iam_role" "task" {
  for_each = toset(var.services)

  name = "${var.name}-${each.value}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

locals {
  # Only the loader reads parts; only the stream consumers touch the broker.
  needs_s3    = ["snapshot-loader"]
  needs_kafka = ["cdc-applier", "repair-worker"]
}

data "aws_iam_policy_document" "task" {
  for_each = toset(var.services)

  statement {
    sid       = "Kms"
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid       = "Secrets"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.secret_arn]
  }

  dynamic "statement" {
    for_each = contains(local.needs_s3, each.value) ? [1] : []
    content {
      sid       = "PartsRead"
      effect    = "Allow"
      actions   = ["s3:GetObject", "s3:ListBucket", "s3:GetObjectVersion"]
      resources = [var.parts_bucket_arn, "${var.parts_bucket_arn}/*"]
    }
  }

  dynamic "statement" {
    for_each = contains(local.needs_kafka, each.value) ? [1] : []
    content {
      sid    = "MskConnect"
      effect = "Allow"
      actions = [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData",
        "kafka-cluster:DescribeGroup",
      ]
      resources = [
        var.msk_cluster_arn,
        replace(var.msk_cluster_arn, ":cluster/", ":topic/"),
        replace(var.msk_cluster_arn, ":cluster/", ":group/"),
      ]
    }
  }
}

resource "aws_iam_role_policy" "task" {
  for_each = toset(var.services)

  role   = aws_iam_role.task[each.value].id
  policy = data.aws_iam_policy_document.task[each.value].json
}

# ------------------------------------------------------------------ services

resource "aws_ecs_task_definition" "this" {
  for_each = toset(var.services)

  family                   = "${var.name}-${each.value}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value == "cdc-applier" ? "2048" : "1024"
  memory                   = each.value == "cdc-applier" ? "4096" : "2048"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task[each.value].arn

  container_definitions = jsonencode([{
    name      = each.value
    image     = "${var.image_repository}/${each.value}:${var.image_tag}"
    essential = true

    portMappings = [{ containerPort = 9090, protocol = "tcp" }]

    environment = [
      { name = "MIGRATION_ID", value = var.name },
      { name = "ENVIRONMENT", value = "production" },
      { name = "PARTS_BUCKET", value = var.parts_bucket_name },
      { name = "AWS_REGION", value = var.region },
      { name = "ADMIN_ADDR", value = ":9090" },
      { name = "LOG_LEVEL", value = "info" },
    ]

    secrets = [
      { name = "TARGET_DSN", valueFrom = "${var.secret_arn}:dsn::" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.services[each.value].name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "ecs"
      }
    }

    # Readiness and liveness are distinct: a task that has lost its database is
    # not ready, but restarting it will not help, so this checks liveness only.
    # Using /readyz here would crash-loop the whole fleet through a target outage.
    healthCheck = {
      command     = ["CMD-SHELL", "wget -q -O- http://localhost:9090/healthz || exit 1"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 30
    }
  }])
}

resource "aws_ecs_service" "this" {
  for_each = toset(var.services)

  name            = each.value
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this[each.value].arn
  desired_count   = lookup(var.service_desired_count, each.value, 1)
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.service_sg_id]
    assign_public_ip = false
  }

  # Replacing every applier at once would stall the stream. Half the fleet stays
  # up through a deploy.
  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  enable_execute_command = true

  lifecycle {
    ignore_changes = [desired_count]
  }
}

# -------------------------------------------------------------------- alarms

# The alarms are chosen so that each one corresponds to a runbook. An alarm with
# no documented response is noise that trains people to ignore the page.
resource "aws_cloudwatch_metric_alarm" "lag" {
  alarm_name          = "${var.name}-replication-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "migration_replication_lag_seconds"
  namespace           = "DBMigration"
  period              = 60
  statistic           = "Maximum"
  threshold           = var.lag_alarm_seconds
  treat_missing_data  = "breaching"

  alarm_description = "Replication lag above threshold. Runbook: docs/runbooks/lag-incident.md"
}

resource "aws_cloudwatch_metric_alarm" "dlq" {
  alarm_name          = "${var.name}-dead-letters-open"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "migration_dlq_open"
  namespace           = "DBMigration"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  alarm_description = "Records are unapplied. Runbook: docs/runbooks/dlq-triage.md"
}

# A raw dead-letter count hides a stalled drain: a steady trickle and a stuck
# queue look identical. The age of the oldest open record does not.
resource "aws_cloudwatch_metric_alarm" "dlq_stalled" {
  alarm_name          = "${var.name}-dead-letter-drain-stalled"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "migration_dlq_oldest_open_age_seconds"
  namespace           = "DBMigration"
  period              = 300
  statistic           = "Maximum"
  threshold           = 3600
  treat_missing_data  = "notBreaching"

  alarm_description = "The oldest unapplied record is over an hour old; the drain is not keeping up. Runbook: docs/runbooks/dlq-triage.md"
}

resource "aws_cloudwatch_metric_alarm" "recon" {
  alarm_name          = "${var.name}-reconciliation-findings"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "migration_recon_open_findings"
  namespace           = "DBMigration"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  alarm_description = "The target does not match the source. This blocks cutover."
}

output "controlplane_url" {
  value       = "http://${aws_ecs_service.this["controlplane"].name}.${var.name}.internal:9090"
  description = "Internal control-plane endpoint."
}
