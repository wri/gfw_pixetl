terraform {

  backend "s3" {
    key     = "wri__gfw_pixetl.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

module "container_registry" {
  source     = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2.2"
  image_name = "${local.project}${local.name_suffix}"
  root_dir   = "../${path.root}"
}



module "compute_environment_ephemeral_storage" {
  source               = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/compute_environment?ref=v0.4.2.2"
  project              = local.project
  key_pair             = data.terraform_remote_state.core.outputs.key_pairs["dmannarino_gfw"].key_name
  subnets              = data.terraform_remote_state.core.outputs.private_subnet_ids
  tags                 = local.tags
  security_group_ids   = [data.terraform_remote_state.core.outputs.default_security_group_id, data.terraform_remote_state.core.outputs.postgresql_security_group_id]
  ecs_role_policy_arns = [data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn]
  suffix               = local.name_suffix
  //  instance_types     = ["r5d"]

}