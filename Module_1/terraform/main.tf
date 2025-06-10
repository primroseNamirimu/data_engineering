terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.32.0"
    }
  }
}
#  credentials = "./keys/my-cred.json"
# export GOOGLE_CREDENTIALS='/Users/primrose/Desktop/Work/DE/Module_1/terraform/keys/my-cred.json'
provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "demo_bigquery" {
  dataset_id = var.bq_dataset_name
  location = var.location
}