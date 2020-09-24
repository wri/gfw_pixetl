terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 2.45.0, < 4.0"
    }
    local = {
      source = "hashicorp/local"
    }
    template = {
      source = "hashicorp/template"
    }
  }
  required_version = ">= 0.13"
}
