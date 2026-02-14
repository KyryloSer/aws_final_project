Перенос из Raw в Bronze (Sales):
aws glue start-job-run --job-name "sales_raw_to_bronze" --region eu-central-1
Перенос из Bronze в Silver 
aws glue start-job-run --job-name "sales_bronze_to_silver" --region eu-central-1
проверка готовности
```bash
aws glue get-job-run --job-name "sales_raw_to_bronze" --run-id j <ID task> --query "JobRun.JobRunState" --output text --region eu-central-1
```