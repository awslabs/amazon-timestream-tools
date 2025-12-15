## Step 1

Update `variables.tf`, adding the following:
- A
- B

## Step 2

```
terraform apply
```

## Step 3

CD into `app`.

```
docker buildx build --platform linux/arm64 -t influxdb-backup-restore:latest .
```

## Step 4

```
podman push 460629772345.dkr.ecr.us-west-2.amazonaws.com/influxdb_v2_to_v3_migration_ecr_repository:latest
```
