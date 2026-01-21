# Justfile for constraints_builder

# Default recipe to display help
default:
    @just --list

# Build the constraints_builder Docker image
build:
    docker build -f docker/Dockerfile.constraints-builder --target rbuilder-runtime -t constraints-builder .
