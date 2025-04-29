terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.32.0"
    }
  }
}

provider "google" {
  project     = "charismatic-sum-458310-j6"
  region      = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "charismatic-sum-458310-j6-terra-bucket"
  location      = "US"
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