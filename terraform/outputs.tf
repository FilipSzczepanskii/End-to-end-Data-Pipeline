output "pipeline_service_account_email" {
  description = "Service account email - use for Prefect/dbt auth"
  value       = google_service_account.pipeline.email
}

output "datasets" {
  value = {
    raw     = google_bigquery_dataset.raw.dataset_id
    staging = google_bigquery_dataset.staging.dataset_id
    marts   = google_bigquery_dataset.marts.dataset_id
  }
}
