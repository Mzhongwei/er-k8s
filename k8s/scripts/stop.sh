#!/bin/bash
# This script will stop the Kubernetes cluster and delete the services and deployments.
# Delete the services
for file in k8s/services/*.yaml; do
    service_name=$(grep -o "name: [^ ]*" $file | cut -d' ' -f2)
    kubectl delete service $service_name
    echo "Service $service_name deleted successfully."
done

# Delete the deployments
for file in k8s/deployments/*.yaml; do
    deployment_name=$(grep -o "name: [^ ]*" $file | cut -d' ' -f2)
    kubectl delete deployment $deployment_name
    echo "Deployment $deployment_name deleted successfully."
done

# Delete the ConfigMap for each python script
for file in code/python_files/*.py; do
    filename=$(basename $file)
    configmap_name=$(echo $filename | cut -d. -f1)
    kubectl delete configmap $configmap_name
    echo "ConfigMap $configmap_name deleted successfully."
done

# Stop the cluster with minikube
minikube stop
echo "Kubernetes cluster stopped and services and deployments deleted successfully."