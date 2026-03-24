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