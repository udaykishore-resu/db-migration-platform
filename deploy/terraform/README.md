# Terraform

Infrastructure for the migration platform: a private VPC with no internet
gateway, an Aurora cluster, an MSK cluster, the parts bucket, and one ECS Fargate
service per platform component.

## Layout

```
.
├── main.tf              root: wires the three modules together
├── variables.tf         every knob, with the reasoning in its description
├── outputs.tf
├── terraform.tfvars.example
└── modules/
    ├── network/         VPC, private subnets, endpoints, security groups, flow logs
    ├── data/            KMS, S3 parts bucket, Aurora, MSK
    └── compute/         ECS cluster, per-service task roles, services, alarms
```

## Usage

```bash
cp terraform.tfvars.example terraform.tfvars   # edit
terraform init
terraform plan
terraform apply
```

## Design notes worth reading before changing anything

**There is no public subnet and no NAT gateway.** Every AWS service is reached
through a VPC endpoint. The S3 gateway endpoint in particular is what makes bulk
part transfer both private and free — routing terabytes through a NAT gateway
would be neither.

**One IAM role per service, not one shared role.** The reconciler cannot write to
the parts bucket; the control plane cannot reach the broker. Shared roles are how
a compromise of the least sensitive component becomes a compromise of everything.

**Aurora reads parts from S3 itself** through `aws_iam_role.aurora_s3`. That is
what keeps terabytes out of the worker processes.

**MSK offers TLS only.** No plaintext listener is configured even for the in-VPC
path — an available plaintext listener is one misconfiguration away from being
used.

**Broker retention is 14 days by default.** It must outlast the migration. If the
stream ages out before the load finishes, the only recovery is a fresh snapshot of
the affected tables.

**The ECS health check hits `/healthz`, not `/readyz`.** A task that has lost its
database is not ready, but restarting it will not help. Using readiness as the
container health check crash-loops the entire fleet through a target outage.

**Every alarm maps to a runbook.** An alarm with no documented response is noise
that trains people to ignore the page.

## What is deliberately not here

- **Direct Connect or the VPN.** These are usually pre-existing and owned by the
  network team. Set `onprem_cidrs` to allow the on-premise CDC connector through.
- **The CDC connector itself.** Debezium on MSK Connect, Qlik Replicate or DMS,
  depending on the source engine.
- **DNS for cutover.** Repointing the application is deliberately a separate,
  reversible change owned by whoever operates the application.
