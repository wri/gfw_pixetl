terraform {
  required_version = ">=0.12.19"
  backend "s3" {
    key     = "wri__gfw_pixetl.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

# Download any stable version in AWS provider of 2.36.0 or higher in 2.36 train
provider "aws" {
  region  = "us-east-1"
  version = "~> 2.45.0"
}

module "container_registry" {
  source     = "git::https://github.com/wri/gfw-terraform-modules.git//modules/container_registry?ref=v0.0.4"
  image_name = "${local.project}${local.name_suffix}"
  root_dir   = "../${path.root}"
}



module "compute_environment_ephemeral_storage" {
  source             = "git::https://github.com/wri/gfw-terraform-modules.git//modules/compute_environment_ephemeral_storage?ref=v0.0.4"
  project            = local.project
  key_pair           = data.terraform_remote_state.core.outputs.key_pair_tmaschler_gfw
  subnets            = data.terraform_remote_state.core.outputs.private_subnet_ids
  tags               = local.tags
  security_group_ids = [data.terraform_remote_state.core.outputs.default_security_group_id]
  iam_policy_arn     = [data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn]
  suffix             = local.name_suffix
  instance_types     = ["r5d"]

}