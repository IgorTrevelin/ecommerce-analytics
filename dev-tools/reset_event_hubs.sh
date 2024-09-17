#!/bin/bash

eventhubs=(
    "connect-cluster-configs"
    "connect-cluster-offsets"
    connect-cluster-status
    "postgres.public.order_items"
    "postgres.public.orders"
    "postgres.public.products"
    "postgres.public.product_categories"
    "postgres.public.sellers"
    "postgres.public.customers"
    "postgres.public.zip_code_prefixes"
)


for eventhub in "${eventhubs[@]}"; do
  echo "Deleting Event Hub: $eventhub"
  az eventhubs eventhub delete --resource-group "$RESOURCE_GROUP" --namespace-name ${WORKSPACE_NAME} --name "$eventhub"

  if [ $? -eq 0 ]; then
    echo "Successfully deleted $eventhub"
  else
    echo "Failed to delete $eventhub"
  fi
done
