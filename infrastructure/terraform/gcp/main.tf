terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}
variable "project_id" { type = string }
variable "region" {
  type    = string
  default = "europe-west1"
}
variable "zone" {
  type    = string
  default = "europe-west1-b"
}
variable "boot_disk_gb" {
  type    = number
  default = 100
}
variable "ceph_disk_gb" {
  type    = number
  default = 200
}
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
resource "google_compute_network" "fema" {
  name                    = "fema-private"
  auto_create_subnetworks = false
}
resource "google_compute_subnetwork" "fema" {
  name   = "fema-subnet"
  region = var.region
  log_config {
    aggregation_interval = "INTERVAL_5_SEC"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }
  network       = google_compute_network.fema.id
  ip_cidr_range = "10.42.0.0/24"
}
resource "google_compute_firewall" "iap" {
  name          = "fema-iap-ssh"
  network       = google_compute_network.fema.name
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}
resource "google_compute_router" "fema" {
  name    = "fema-router"
  network = google_compute_network.fema.id
  region  = var.region
}
resource "google_compute_router_nat" "fema" {
  name                               = "fema-nat"
  router                             = google_compute_router.fema.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}
resource "google_compute_disk" "ceph" {
  name = "fema-ceph-osd"
  type = "pd-balanced"
  size = var.ceph_disk_gb
  zone = var.zone
  lifecycle { prevent_destroy = true }
}
resource "google_compute_instance" "fema" {
  name         = "fema-platform"
  metadata     = { block-project-ssh-keys = "TRUE" }
  machine_type = "e2-standard-8"
  zone         = var.zone
  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = var.boot_disk_gb
    }
  }
  attached_disk {
    source      = google_compute_disk.ceph.id
    device_name = "ceph-osd"
  }
  network_interface {
    subnetwork = google_compute_subnetwork.fema.id
  }
  depends_on              = [google_compute_router_nat.fema]
  metadata_startup_script = <<-EOT
    #!/bin/bash
    set -eux
    apt-get update
    apt-get install -y docker.io docker-compose-v2 git lvm2 cephadm awscli chrony
    systemctl enable --now docker
  EOT
}
output "ssh_command" {
  value = "gcloud compute ssh fema-platform --zone ${var.zone} --project ${var.project_id} --tunnel-through-iap"
}
