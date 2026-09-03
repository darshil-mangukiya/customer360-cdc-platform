# Terraform Deployment Scaffold

This directory maps the Customer 360 CDC platform to deployable infrastructure. The
provider-light modules can be adapted to AWS, GCP, Azure, or a managed data stack;
example variables use placeholder credentials.

## Components Represented

- PostgreSQL warehouse
- Kafka cluster / managed streaming service
- Airflow orchestration environment
- Object storage landing/archive bucket
- dbt transformation runtime
- Activation API service
- Secrets for warehouse, Kafka, and PII hash salt
- Monitoring and lineage sinks

## Usage Pattern

```bash
terraform init
terraform plan -var-file=envs/local.tfvars
```

Local execution remains Docker-first. Terraform documents how the same components map
to an infrastructure-managed environment.

## Production Adaptation

Replace the temporary `null_resource` stubs in `main.tf` with provider resources such as:

- AWS: RDS, MSK, MWAA, S3, ECS/Fargate, Secrets Manager, CloudWatch
- GCP: Cloud SQL, Pub/Sub or Confluent Cloud, Cloud Composer, GCS, Cloud Run, Secret Manager
- Azure: Azure Database for PostgreSQL, Event Hubs, Data Factory/Airflow, Blob Storage, Container Apps, Key Vault
