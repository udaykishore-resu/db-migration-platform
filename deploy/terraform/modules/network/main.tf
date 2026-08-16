# Network: a private VPC with no internet gateway.
#
# There is no public subnet and no NAT by default. Every AWS service the platform
# needs is reached through an endpoint, so traffic between the workloads, the
# broker, object storage and the key store never leaves the AWS network. The
# gateway endpoint for S3 in particular is what makes bulk part transfer both
# free and private — routing terabytes through a NAT gateway would be neither.

variable "name" { type = string }
variable "vpc_cidr" { type = string }
variable "azs" { type = list(string) }
variable "region" { type = string }
variable "onprem_cidrs" { type = list(string) }

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.name}-vpc" }
}

resource "aws_subnet" "private" {
  count = length(var.azs)

  vpc_id            = aws_vpc.this.id
  availability_zone = var.azs[count.index]
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index)

  # Explicitly false. A migration workload has no reason to be addressable.
  map_public_ip_on_launch = false

  tags = { Name = "${var.name}-private-${var.azs[count.index]}" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-private" }
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ------------------------------------------------------------------ endpoints

# Gateway endpoint: route-table based, no hourly charge, no data processing
# charge. This is the path terabytes of .dat parts take.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = { Name = "${var.name}-s3" }
}

# Interface endpoints for the control-plane APIs the services call.
locals {
  interface_endpoints = [
    "kms",
    "secretsmanager",
    "logs",
    "monitoring",
    "ecr.api",
    "ecr.dkr",
    "sts",
  ]
}

resource "aws_vpc_endpoint" "interface" {
  for_each = toset(local.interface_endpoints)

  vpc_id              = aws_vpc.this.id
  service_name        = "com.amazonaws.${var.region}.${each.value}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.name}-${replace(each.value, ".", "-")}" }
}

# ------------------------------------------------------------ security groups

resource "aws_security_group" "endpoints" {
  name        = "${var.name}-endpoints"
  description = "Interface endpoints; reachable from the service tasks only"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "HTTPS from service tasks"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.service.id]
  }

  tags = { Name = "${var.name}-endpoints" }
}

resource "aws_security_group" "service" {
  name        = "${var.name}-service"
  description = "Platform service tasks"
  vpc_id      = aws_vpc.this.id

  # Egress is not wide open: the tasks need HTTPS to the endpoints, the database
  # port, and the broker port. Nothing else, and nothing outbound to the internet.
  egress {
    description = "HTTPS to VPC endpoints"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "Aurora"
    from_port   = 3306
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "MSK TLS"
    from_port   = 9094
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  tags = { Name = "${var.name}-service" }
}

resource "aws_security_group" "aurora" {
  name        = "${var.name}-aurora"
  description = "Aurora cluster; reachable from the service tasks only"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "Postgres from service tasks"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.service.id]
  }

  ingress {
    description     = "MySQL from service tasks"
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    security_groups = [aws_security_group.service.id]
  }

  tags = { Name = "${var.name}-aurora" }
}

resource "aws_security_group" "msk" {
  name        = "${var.name}-msk"
  description = "MSK cluster; service tasks and the on-premise CDC connector"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "TLS from service tasks"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [aws_security_group.service.id]
  }

  # The on-premise connector produces into this cluster over the private circuit.
  # If onprem_cidrs is empty this rule is not created at all, rather than
  # defaulting to something permissive.
  dynamic "ingress" {
    for_each = length(var.onprem_cidrs) > 0 ? [1] : []
    content {
      description = "TLS from the on-premise CDC connector over Direct Connect"
      from_port   = 9094
      to_port     = 9094
      protocol    = "tcp"
      cidr_blocks = var.onprem_cidrs
    }
  }

  tags = { Name = "${var.name}-msk" }
}

# ---------------------------------------------------------------- flow logs

resource "aws_cloudwatch_log_group" "flow" {
  name              = "/aws/vpc/${var.name}/flow-logs"
  retention_in_days = 90
}

resource "aws_iam_role" "flow" {
  name = "${var.name}-flow-logs"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "vpc-flow-logs.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "flow" {
  role = aws_iam_role.flow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
      Resource = "${aws_cloudwatch_log_group.flow.arn}:*"
    }]
  })
}

# Flow logs are not optional here. When a migration is later audited, "which
# hosts talked to the database holding regulated data" is a question that has to
# have an answer.
resource "aws_flow_log" "this" {
  vpc_id               = aws_vpc.this.id
  traffic_type         = "ALL"
  iam_role_arn         = aws_iam_role.flow.arn
  log_destination      = aws_cloudwatch_log_group.flow.arn
  max_aggregation_interval = 60
}

output "vpc_id" { value = aws_vpc.this.id }
output "private_subnet_ids" { value = aws_subnet.private[*].id }
output "service_sg_id" { value = aws_security_group.service.id }
output "aurora_sg_id" { value = aws_security_group.aurora.id }
output "msk_sg_id" { value = aws_security_group.msk.id }
