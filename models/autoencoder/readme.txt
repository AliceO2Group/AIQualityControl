

In one terminal: 

mlflow ui \
  --backend-store-uri sqlite:////Users/zetasourpi/cernbox/mlflow-backend/mlflow.db \
  --default-artifact-root /Users/zetasourpi/cernbox/mlflow-backend/mlruns
  --port 8080

In the other under: `AIQualityControl/models/autoencoder`
Update the params.yaml
uv run python train.py 

