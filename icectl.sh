#!/bin/bash

# icectl wrapper script with proper SSL certificate configuration
export ICECTL_CONFIG="infra/icectl.config.yaml"
export REQUESTS_CA_BUNDLE="/Users/stephen/Development/git/sjdurfey/homelab/configs/ssl/ca/ca.crt"

uv run icectl "$@"