output "vpc_id" {
  value       = module.network.vpc_id
  description = "Migration VPC."
}

output "aurora_writer_endpoint" {
  value       = module.data.aurora_writer_endpoint
  description = "Writer endpoint. The applier and loader use this."
}

output "aurora_reader_endpoint" {
  value       = module.data.aurora_reader_endpoint
  description = "Reader endpoint. Point the reconciler here so its range scans do not compete with the apply path."
}

output "msk_bootstrap_brokers_tls" {
  value       = module.data.msk_bootstrap_brokers_tls
  description = "TLS bootstrap servers."
  sensitive   = true
}

output "parts_bucket" {
  value       = module.data.parts_bucket_name
  description = "Bucket holding extracted .dat parts."
}

output "kms_key_arn" {
  value       = module.data.kms_key_arn
  description = "CMK protecting parts, broker storage and database storage."
}

output "controlplane_url" {
  value       = module.compute.controlplane_url
  description = "Internal control-plane endpoint. GET /v1/cutover/readiness is the gate."
}
