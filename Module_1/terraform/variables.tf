variable "credentials" {
    description = "Project credentials"
    default = "./keys/my-cred.json"
}


variable "project" {
    description = "Project"
    default = "charismatic-sum-458310-j6"
}

variable "region" {
    description = "Project region"
    default = "us-central1"
}

variable "location" {
    description = "Project Location"
    default = "US"
}

variable "bq_dataset_name" {
    description = "Big Query dataset Name"
    default = "demo_bigquery"
}

variable "gcs_bucket_name" {
    description = "Google Bucket Name"
    default = "charismatic-sum-458310-j6-terra-bucket"
}

variable "gcs_storage_class" {
    description = "GCS Storage Class"
    default = "STANDARD"
}