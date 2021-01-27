#!/bin/bash

set -e

pushd /usr/local/app/tests/terraform
terraform init && terraform plan -var="secret_name=${AWS_GCS_KEY_SECRET_ARN}" && terraform apply -auto-approve -var="secret_name=${AWS_GCS_KEY_SECRET_ARN}"
popd

/usr/local/app/wait_for_postgres.sh pytest -vv --cov-report term --cov-report xml:/usr/local/app/tests/cobertura.xml --cov=gfw_pixetl /usr/local/app/tests/"$@"
