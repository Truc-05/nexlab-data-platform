terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-nexlab-lake"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 90 }
  }
}

resource "google_storage_bucket_object" "raw_prefix" {
  name    = "raw/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = ""
}

resource "google_storage_bucket_object" "curated_prefix" {
  name    = "curated/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = ""
}

output "bucket_name" {
  value = google_storage_bucket.data_lake.name
}
