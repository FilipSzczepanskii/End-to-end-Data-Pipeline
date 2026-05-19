# GCP variant of the warehouse. The default project target is DuckDB
# (see dbt/profiles.yml.example). This plan is here so the BigQuery path
# is a profile swap away if you want to deploy to GCP.

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# BigQuery datasets

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "air_quality_raw"
  description = "Raw landing zone. Prefect writes ingested JSON here."
  location    = var.region

  default_table_expiration_ms = 7776000000 # 90 days

  labels = {
    project = "air-quality"
    layer   = "raw"
  }
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "air_quality_staging"
  description = "dbt staging layer. Cleaned and typed views."
  location    = var.region
  labels = {
    project = "air-quality"
    layer   = "staging"
  }
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "air_quality_marts"
  description = "dbt marts layer. Analytics-ready tables for the dashboard."
  location    = var.region
  labels = {
    project = "air-quality"
    layer   = "marts"
  }
}

# Service account for the pipeline

resource "google_service_account" "pipeline" {
  account_id   = "air-quality-pipeline"
  display_name = "Air Quality Pipeline (Prefect + dbt)"
}

resource "google_project_iam_member" "pipeline_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_raw_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_staging_editor" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_marts_editor" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}
