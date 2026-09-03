variable "project_name" {
  description = "Project/application name."
  type        = string
  default     = "customer-360-cdc-platform"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "local"
}

variable "owner" {
  description = "Owning team."
  type        = string
  default     = "data-platform"
}

variable "warehouse_secret_name" {
  description = "Secret name containing warehouse credentials."
  type        = string
  default     = "customer360/warehouse"
}

variable "kafka_secret_name" {
  description = "Secret name containing Kafka credentials."
  type        = string
  default     = "customer360/kafka"
}

variable "pii_salt_secret_name" {
  description = "Secret name containing the PII hash salt."
  type        = string
  default     = "customer360/pii_hash_salt"
}

