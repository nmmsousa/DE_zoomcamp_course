provider "google" {
  credentials = file(var.cred)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "my_first_bucket" {
  name          = var.my_first_bucket
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

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.dataset_name
}