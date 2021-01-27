terraform {
  required_version = ">=0.13"
}

provider "aws" {
  region = "us-east-1"
  skip_credentials_validation = true
  skip_requesting_account_id = true
  skip_metadata_api_check = true
  s3_force_path_style = true
  endpoints {
    s3 = "http://localstack:4566"
    secretsmanager = "http://localstack:4566"
    iam = "http://localstack:4566"
  }
}

module "gcs_gfw_gee_export_secret" {
  source        = "git::https://github.com/wri/gfw-aws-core-infrastructure.git//terraform/modules/secrets?ref=feature/rds_instance_count"
  project       = "test"
  name          = var.secret_name
  secret_string = "test"
}
