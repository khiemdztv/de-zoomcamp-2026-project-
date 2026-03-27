terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("../epl-data-pipeline-9999-de6a9c38ce47.json")
  project     = var.project_id
  region      = var.region
}

# GCS Bucket — Data Lake
resource "google_storage_bucket" "epl_datalake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  uniform_bucket_level_access = true
}

# BigQuery Dataset — Raw layer
resource "google_bigquery_dataset" "epl_raw" {
  dataset_id                      = "epl_raw"
  location                        = "asia-southeast1"
  default_table_expiration_ms     = 5184000000
  default_partition_expiration_ms = 5184000000
}

# BigQuery Dataset — Core/Analytics layer
resource "google_bigquery_dataset" "epl_core" {
  dataset_id                      = "epl_core"
  location                        = "asia-southeast1"
  default_table_expiration_ms     = 5184000000
  default_partition_expiration_ms = 5184000000
}