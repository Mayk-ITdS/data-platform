mock_provider "google" {}
variables {
  project_id = "fema-test-project"
}
run "private_single_node" {
  command = plan
  assert {
    condition     = google_compute_instance.fema.metadata["block-project-ssh-keys"] == "TRUE" && length(google_compute_instance.fema.network_interface[0].access_config) == 0
    error_message = "The VM must reject project SSH keys and have no public IP."
  }
  assert {
    condition     = length(google_compute_subnetwork.fema.log_config) == 1 && google_compute_subnetwork.fema.log_config[0].flow_sampling > 0
    error_message = "The subnet must record flow logs."
  }
  assert {
    condition     = google_compute_firewall.iap.source_ranges == toset(["35.235.240.0/20"]) && google_compute_disk.ceph.size == var.ceph_disk_gb
    error_message = "The IAP range and persistent disk sizing must match the plan."
  }
}
