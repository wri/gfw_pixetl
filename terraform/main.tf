terraform {
  required_version = ">=0.12.13"
  backend "s3" {
    key     = "gfw_pixetl.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

# Download any stable version in AWS provider of 2.36.0 or higher in 2.36 train
provider "aws" {
  region  = "us-east-1"
  version = "~> 2.36.0"
}

module "ecr_push_dockerfile" {
  source      = "git::https://github.com/wri/terraform-aws-ecr-docker-image.git?ref=feature/terraform12"
  image_name  = "${local.project}${local.name_suffix}"
  source_path = "../${path.root}"
}