variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for BigQuery datasets"
  type        = string
  default     = "europe-central2"
}
