#!/bin/bash

: ${COMMENT="CI/CD"}
echo $DATABRICKS_ID
DATABRICKS_URL="https://"$(az resource show --id $DATABRICKS_ID --query properties.workspaceUrl -otsv)
TENANT_ID=$(az account show --query tenantId -otsv)

GLOBAL_DATABRICKS_APPID=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d
AZURE_MANAGEMENT=https://management.core.windows.net/

aztoken=$(az account get-access-token --resource $AZURE_MANAGEMENT --query accessToken -otsv)

# Create a folder in the workspace

az rest --method POST --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/workspace/mkdirs \
  --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" \
  --body '{"path":"/Tests/pipeline"}' \
  -otsv > /dev/null

az rest --method POST --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/workspace/import \
  --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" \
  --body '{"path":"/Tests/pipeline/pipeline","overwrite":true,"format":"JUPYTER","content":"'$(cat ./notebooks/pipeline/pipeline.ipynb | base64)'"}' \
  -otsv > /dev/null

az rest --method POST --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/workspace/import \
  --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" \
  --body '{"path":"/Tests/pipeline/driver","overwrite":true,"format":"JUPYTER","content":"'$(cat ./notebooks/pipeline/driver.ipynb | base64)'"}' \
  -otsv > /dev/null

az rest --method POST --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/workspace/import \
  --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" \
  --body '{"path":"/Tests/pipeline/tests","overwrite":true,"format":"JUPYTER","content":"'$(cat ./notebooks/pipeline/tests.ipynb | base64)'"}' \
  -otsv > /dev/null

# Create the DBFS output folder

az rest --method POST --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/dbfs/mkdirs \
  --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" \
  --body '{"path":"/tmp/pipeline/junit"}' \
  -otsv > /dev/null

# Execute the tests notebook

RUN_ID=$(az rest --method POST --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/jobs/runs/submit \
  --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" \
  --body '{"run_name":"unit tests","new_cluster":{"spark_version":"6.5.x-scala2.11","node_type_id":"Standard_DS3_v2","num_workers":1},"notebook_task":{"notebook_path":"/Tests/pipeline/tests"}}' \
  --query run_id \
  -otsv)

# Wait for the notebook to complete

while true; do
    STATE=$(az rest --method GET --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/jobs/runs/get?run_id=$RUN_ID \
      --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" \
      --query 'state.life_cycle_state' \
      -otsv)
    echo $STATE
    if [ "$STATE" == "TERMINATED" ]; then
        break
    fi
    if [ "$STATE" == "SKIPPED" ]; then
        echo "SKIPPED"
        exit 1
    fi
    if [ "$STATE" == "INTERNAL_ERROR" ]; then
        echo "ERROR"
        exit 1
    fi
    sleep 5
done

# Download test results

az rest --method GET --resource $GLOBAL_DATABRICKS_APPID --uri $DATABRICKS_URL/api/2.0/dbfs/read?path=dbfs%3A%2Ftmp%2Fpipeline%2Fjunit%2FTEST-Pipeline.xml \
  --headers X-Databricks-Azure-SP-Management-Token="$aztoken" X-Databricks-Azure-Workspace-Resource-Id="$DATABRICKS_ID" \
  --query data \
  -otsv | base64 -D > TEST-Pipeline.xml

