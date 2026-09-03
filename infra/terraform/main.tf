terraform {
  required_version = ">= 1.6.0"
}

locals {
  common_tags = {
    project     = var.project_name
    environment = var.environment
    owner       = var.owner
  }

  platform_components = {
    warehouse_postgres = {
      description = "Customer 360 warehouse for raw, identity, mart, activation, and observability schemas"
      port        = 5432
    }
    kafka_cluster = {
      description = "CDC streaming backbone for source-domain topics and DLQ topics"
      port        = 9092
    }
    airflow = {
      description = "Pipeline orchestration for CDC, identity, dbt, validation, and reverse ETL"
      port        = 8080
    }
    activation_api = {
      description = "Activation export read API and operational dashboard"
      port        = 8000
    }
    object_storage = {
      description = "Raw CDC archive and replay/backfill landing storage"
      port        = null
    }
  }
}

resource "null_resource" "component_blueprint" {
  for_each = local.platform_components

  triggers = {
    name        = each.key
    description = each.value.description
    environment = var.environment
  }
}

resource "null_resource" "secret_contracts" {
  triggers = {
    warehouse_secret = var.warehouse_secret_name
    kafka_secret     = var.kafka_secret_name
    pii_salt_secret  = var.pii_salt_secret_name
  }
}

