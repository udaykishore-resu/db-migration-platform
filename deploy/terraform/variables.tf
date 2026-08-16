variable "name" {
  description = "Name prefix for every resource. Also the migration identifier."
  type        = string
  default     = "db-migration"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR for the migration VPC."
  type        = string
  default     = "10.60.0.0/16"
}

variable "azs" {
  description = "Availability zones. Three is the minimum for a quorum-based broker."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "onprem_cidrs" {
  description = <<-EOT
    CIDRs of the source network, reached over Direct Connect or a VPN.
    Only these ranges may reach the broker; nothing else on-premise needs to.
  EOT
  type        = list(string)
  default     = []
}

variable "aurora_engine" {
  description = "aurora-postgresql or aurora-mysql."
  type        = string
  default     = "aurora-postgresql"

  validation {
    condition     = contains(["aurora-postgresql", "aurora-mysql"], var.aurora_engine)
    error_message = "The platform supports aurora-postgresql and aurora-mysql."
  }
}

variable "aurora_instance_class" {
  description = "Writer instance class. Bulk loading is write-bound, so size for the load, not the steady state."
  type        = string
  default     = "db.r6g.2xlarge"
}

variable "aurora_reader_count" {
  description = <<-EOT
    Reader instances. The reconciler's digest queries are full range scans and
    belong on a reader, not on the writer that the applier is using.
  EOT
  type        = number
  default     = 1
}

variable "msk_broker_count" {
  description = "Broker count. Must be a multiple of the AZ count."
  type        = number
  default     = 3
}

variable "msk_instance_type" {
  description = "Broker instance type."
  type        = string
  default     = "kafka.m5.large"
}

variable "msk_volume_size" {
  description = <<-EOT
    Broker storage in GB. Size for the full retention of the change stream: if the
    stream ages out before the migration finishes, the only recovery is a fresh
    snapshot of the affected tables.
  EOT
  type        = number
  default     = 1000
}

variable "service_desired_count" {
  description = "Task count per service. Appliers should not exceed the partition count."
  type        = map(number)
  default = {
    "cdc-applier"     = 3
    "snapshot-loader" = 2
    "reconciler"      = 1
    "repair-worker"   = 1
    "controlplane"    = 1
  }
}

variable "image_repository" {
  description = "ECR repository holding the service images."
  type        = string
}

variable "image_tag" {
  description = "Image tag to deploy."
  type        = string
  default     = "latest"
}

variable "lag_alarm_seconds" {
  description = "Replication lag that triggers an alarm. Should be well below cutover.max_lag."
  type        = number
  default     = 60
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}
