# Root module for the migration platform.
#
# Everything here is private by design: no public subnets, no public database
# endpoints, and object storage reached through a VPC gateway endpoint rather than
# across the internet. Regulated data crosses this infrastructure, and the cheapest
# way to fail an audit is to have a defensible-sounding reason why one component
# needed a public address.

provider "aws" {
  region = var.region

  default_tags {
    tags = merge(var.tags, {
      Project   = var.name
      ManagedBy = "terraform"
      Component = "db-migration-platform"
    })
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  services   = ["cdc-applier", "snapshot-loader", "reconciler", "repair-worker", "controlplane"]
}

module "network" {
  source = "./modules/network"

  name         = var.name
  vpc_cidr     = var.vpc_cidr
  azs          = var.azs
  region       = var.region
  onprem_cidrs = var.onprem_cidrs
}

module "data" {
  source = "./modules/data"

  name                  = var.name
  region                = var.region
  account_id            = local.account_id
  vpc_id                = module.network.vpc_id
  private_subnet_ids    = module.network.private_subnet_ids
  aurora_sg_id          = module.network.aurora_sg_id
  msk_sg_id             = module.network.msk_sg_id
  aurora_engine         = var.aurora_engine
  aurora_instance_class = var.aurora_instance_class
  aurora_reader_count   = var.aurora_reader_count
  msk_broker_count      = var.msk_broker_count
  msk_instance_type     = var.msk_instance_type
  msk_volume_size       = var.msk_volume_size
}

module "compute" {
  source = "./modules/compute"

  name               = var.name
  region             = var.region
  account_id         = local.account_id
  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids
  service_sg_id      = module.network.service_sg_id

  services              = local.services
  service_desired_count = var.service_desired_count
  image_repository      = var.image_repository
  image_tag             = var.image_tag

  parts_bucket_arn  = module.data.parts_bucket_arn
  parts_bucket_name = module.data.parts_bucket_name
  kms_key_arn       = module.data.kms_key_arn
  secret_arn        = module.data.db_secret_arn
  msk_cluster_arn   = module.data.msk_cluster_arn
  lag_alarm_seconds = var.lag_alarm_seconds
}
