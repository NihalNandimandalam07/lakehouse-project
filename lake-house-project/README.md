# Lake House Project (CDK)

A data lake in AWS, deployed as one CDK stack (`LakeHouseProjectStack`, us-east-2, account 909698979753).

```
MySQL (RDS) ─ Glue JDBC ─┐
Flat files ── DataSync ──┤
Kafka ─ Glue streaming ──┼─► landing ─► raw ─► transformed ─► curated ─► Redshift Serverless
Data Exchange ───────────┘     (Glue)   (DataBrew DQ,  (Glue)      (Glue)
                                         Glue + DataBrew)
                         orchestrated by Step Functions: lakehouse-etl-workflow-nn
                         catalogued by Glue crawlers into lakehouse_catalog_nn
```

Most resources are created by CDK. A few steps can't be, or aren't yet, and are done by hand. They're listed under [Manual steps](#manual-steps), in the order to run them.

## Deploying

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cdk synth
cdk diff      # check for "replace" before deploying
cdk deploy
```

If `cdk` fails with `No module named 'aws_cdk'`, the venv's Python isn't on `PATH`. Point CDK at it directly:

```bash
npx cdk deploy --app ".venv/bin/python app.py"
```

---

## Manual steps

All commands run as the SSO role `AWSReservedSSO_AWSAdministratorAccess_378665f42c34a44b` in **us-east-2**. That role's full ARN, which Lake Formation needs, is:

```
arn:aws:iam::909698979753:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AWSAdministratorAccess_378665f42c34a44b
```

| # | Step | When |
|---|---|---|
| 1 | Kafka cluster and credentials secret | Before deploy |
| 2 | Lake Formation admin | Before deploy |
| 3 | Glue database `lakehouse_catalog_nn` | Before deploy |
| 4 | Crawler Lake Formation grant | After deploy |
| 4b | Table access for your SSO role (Athena) | After deploy |
| 5 | MySQL sample table | After deploy |
| 6 | Flat file in the source bucket | After deploy |
| 7 | Data Exchange subscription and export | After deploy |
| 8 | Kafka messages | After deploy |
| 9 | Running the pipeline | After 4–8 |
| 10 | Redshift read access for your SSO user | After the first Redshift load |

### 1. Kafka cluster and credentials secret (before deploy)

The streaming source is a self-managed Kafka cluster, used instead of MSK. It's built by a separate Terraform project: [IAC-apache-kafka-cluster](https://github.com/NihalNandimandalam07/IAC-apache-kafka-cluster), `terraform` branch.

1. Deploy the Kafka cluster from that repo. Its VPC is `10.0.0.0/16`, and the brokers are at `10.0.0.10`, `10.0.1.10` and `10.0.2.10` on port 9092, using SASL_PLAINTEXT/PLAIN.
2. Create the Secrets Manager secret `kafka_credentials_testing` holding the SASL user:
   ```bash
   aws secretsmanager create-secret --name kafka_credentials_testing \
     --secret-string '{"username":"<kafka user>","password":"<kafka password>"}'
   ```
3. If the cluster is rebuilt, update the hard-coded IDs in `lake_house_project_stack.py`:
   - `kafka_vpc_id`, `kafka_route_table_id`, `kafka_sg_id`
   - `kafka_secret_arn`
   - `kafka_bootstrap_servers`

CDK owns the Kafka-side route back to the lakehouse and the port 9092 security group rule. These would normally belong in the Terraform repo. Running `terraform apply` there removes them, because the Terraform security group and route table use inline rules. Re-run `cdk deploy` afterwards to put them back.

### 2. Lake Formation admin (before deploy)

Lake Formation controls access to the Data Catalog, and the account's default permissions are empty. Without a Lake Formation admin, nobody can see the tables the crawlers create.

Console: open **Lake Formation**, go to **Administration → Administrative roles and tasks → Data lake administrators**, click **Add**, and choose the SSO role.

CLI. `put-data-lake-settings` replaces all the settings, so read them first and only add to the admin list:

```bash
aws lakeformation get-data-lake-settings --query DataLakeSettings > lf-settings.json
# Add to "DataLakeAdmins":
#   {"DataLakePrincipalIdentifier": "<SSO role full ARN>"}
# Remove any admin whose IAM user or role no longer exists; Lake Formation rejects the whole list otherwise.
aws lakeformation put-data-lake-settings --data-lake-settings file://lf-settings.json
```

Check:

```bash
aws lakeformation get-data-lake-settings \
  --query 'DataLakeSettings.DataLakeAdmins[].DataLakePrincipalIdentifier' --output text
```

The SSO role name ends in a generated suffix. If the AdministratorAccess permission set is ever re-created, that suffix changes and this step has to be repeated with the new ARN.

### 3. Glue database `lakehouse_catalog_nn` (before deploy)

The crawlers write their tables into this database. It's created outside CDK, by the SSO role: if the CDK deploy role created it, the SSO role couldn't see the database or grant permissions on it.

```bash
aws glue create-database \
  --database-input '{"Name":"lakehouse_catalog_nn","Description":"Lakehouse transformed and curated zone tables"}'
```

Check that the SSO role has `ALL` on it, with grant option:

```bash
aws lakeformation list-permissions --resource '{"Database":{"Name":"lakehouse_catalog_nn"}}' \
  --query 'PrincipalResourcePermissions[].[Principal.DataLakePrincipalIdentifier,join(`,`,Permissions)]' --output text
```

### 4. Crawler Lake Formation grant (after deploy)

The crawler role `lakehouse-transformed-crawler-role-nn` is created by CDK, and needs Lake Formation permission to create and update tables in the database. Without it, crawlers fail with `Insufficient Lake Formation permission(s)`.

```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::909698979753:role/lakehouse-transformed-crawler-role-nn \
  --resource '{"Database":{"Name":"lakehouse_catalog_nn"}}' \
  --permissions CREATE_TABLE ALTER DESCRIBE
```

Console: open **Lake Formation**, go to **Permissions → Data permissions → Grant**, and set:
- **Principal:** IAM role `lakehouse-transformed-crawler-role-nn`
- **Named Data Catalog resources:** database `lakehouse_catalog_nn`, no tables
- **Database permissions:** Create table, Alter and Describe

Running the same command again is harmless. If the role is deleted and re-created, run it again.

### 4b. Table access for your SSO role (after deploy)

Being a Lake Formation admin lets you manage permissions, not read data. Without this grant, Athena fails with `COLUMN_NOT_FOUND: Column '...' cannot be resolved or requester is not authorized`, even though the column exists.

Grant yourself SELECT and DESCRIBE on every table in the database. The wildcard also covers tables the crawlers create later:

```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=<SSO role full ARN> \
  --resource '{"Table":{"DatabaseName":"lakehouse_catalog_nn","TableWildcard":{}}}' \
  --permissions SELECT DESCRIBE
```

Console: open **Lake Formation**, go to **Permissions → Data permissions → Grant**, and set:
- **Principal:** the SSO role
- **Database:** `lakehouse_catalog_nn`
- **Tables:** All tables
- **Table permissions:** Select and Describe

Then query in **Athena**. Choose workgroup `lakehouse-athena-workgroup-nn` and database `lakehouse_catalog_nn`; the sample queries are under **Saved queries**.

### 5. MySQL sample table (after deploy)

The RDS instance `lakehouse-mysql-nn` (database `lakehousedb`) is in isolated subnets. Reach it through the bastion `lakehouse-ec2-nn` with SSM port forwarding. No SSH key or public IP is needed.

1. Get the endpoint and credentials:
   ```bash
   aws cloudformation describe-stacks --stack-name LakeHouseProjectStack \
     --query "Stacks[0].Outputs[?OutputKey=='LakehouseMySQLEndpoint' || OutputKey=='LakehouseEC2InstanceID'].[OutputKey,OutputValue]" --output text
   aws secretsmanager get-secret-value --secret-id lakehouse-mysql-credential-nn --query SecretString --output text
   ```
2. Open a tunnel. This needs the Session Manager plugin for the AWS CLI:
   ```bash
   aws ssm start-session --target <instance id> \
     --document-name AWS-StartPortForwardingSessionToRemoteHost \
     --parameters '{"host":["<mysql endpoint>"],"portNumber":["3306"],"localPortNumber":["3306"]}'
   ```
3. In another terminal, connect with `mysql -h 127.0.0.1 -P 3306 -u <user> -p lakehousedb` and create the table that the Glue job reads:
   ```sql
   CREATE TABLE employees (
     employee_id   INT PRIMARY KEY,
     employee_name VARCHAR(100),
     department    VARCHAR(50),
     salary        DECIMAL(10,2)
   );
   INSERT INTO employees VALUES
     (1, 'John Smith', 'engineering', 95000),
     (2, 'Jane Doe', 'marketing', 72000),
     (3, 'Raj Patel', 'finance', 48000);
   ```

### 6. Flat file in the source bucket (after deploy)

DataSync copies `s3://lakehouse-source-nn/flatfiles/` to `s3://lakehouse-landing-nn/flatfiles/`. Upload a customers CSV with this header:

```
customer_id,last_name,first_name,date_of_birth,email,creation_date,phone,address_state,customer_status,annual_income
```

```bash
aws s3 cp customers.csv s3://lakehouse-source-nn/flatfiles/customers.csv
```

The bucket uses SSE-KMS by default, so no extra encryption flags are needed.

### 7. Data Exchange subscription and export (after deploy)

The third-party source is the free **COVID-19 Activity** dataset in AWS Data Exchange.

1. Open **AWS Data Exchange → Browse catalog**, search for *COVID-19 Activity*, and subscribe. The subscription is free.
2. Go to **Entitled data**, open the dataset, choose its latest **revision**, and click **Export to Amazon S3**:
   - **Bucket:** `lakehouse-landing-nn`
   - **Key naming pattern:** `third-party-covid19/${Asset.Name}`
   - **Encryption:** SSE-KMS with `alias/lakehouse-kms-nn`
3. Check that the CSV is under the path `landing_to_raw.py` reads, `third-party-covid19/published/PUBLIC/COVID-19-Activity/*/*.csv`:
   ```bash
   aws s3 ls s3://lakehouse-landing-nn/third-party-covid19/ --recursive
   ```
   If the export landed somewhere else, move it to match, or update the path in `glue_scripts/landing_to_raw.py`.

### 8. Kafka messages (after deploy)

The streaming job `lakehouse-kafka-to-s3-nn` reads topic `lakehouse-topic` and expects JSON messages like:

```json
{"employee_id": 4, "employee_name": "Maria Garcia", "department": "sales", "salary": 61000}
```

1. Create the topic, and publish a few messages from a Kafka client in the Kafka VPC. Use SASL_PLAINTEXT/PLAIN with the `kafka_credentials_testing` user.
2. Start the streaming job:
   ```bash
   aws glue start-job-run --job-name lakehouse-kafka-to-s3-nn
   ```
   It runs continuously, writing to `s3://lakehouse-landing-nn/kafka/employees/`. Stop it when you're done to avoid charges:
   ```bash
   aws glue batch-stop-job-run --job-name lakehouse-kafka-to-s3-nn --job-run-ids <run id>
   ```

### 9. Running the pipeline (after steps 4–8)

The ingestion jobs aren't part of the Step Functions workflow. Run them first, then the workflow, then the crawlers.

```bash
# Ingestion into landing
aws glue start-job-run --job-name lakehouse-rds-to-s3-nn
aws datasync start-task-execution --task-arn <lakehouse-flatfiles-datasync-nn task ARN>

# landing -> raw -> DQ -> transformed -> curated -> Redshift
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-2:909698979753:stateMachine:lakehouse-etl-workflow-nn

# Refresh the catalog once the workflow succeeds
for c in transformed-mysql transformed-kafka transformed-flatfiles transformed-dataexchange \
         curated-employees curated-customers curated-covid; do
  aws glue start-crawler --name lakehouse-$c-crawler-nn
done
```

Check that the tables are there:

```bash
aws glue get-tables --database-name lakehouse_catalog_nn --query 'TableList[].Name' --output text
```

### 10. Redshift read access for your SSO user (after the first Redshift load)

The Glue job loads `public.employees`, `public.customers` and `public.covid` as the Redshift admin user `lakehouseadmin`. Query Editor v2 signs you in as a different Redshift user, `IAMR:AWSReservedSSO_AWSAdministratorAccess_378665f42c34a44b`, which can't see those tables until it's granted access.

1. In **Redshift Query Editor v2**, connect to workgroup `lakehouse-workgroup-nn`, database `lakehouse_dw`, using **AWS Secrets Manager** and the secret `lakehouse-redshift-credential-nn`. This signs you in as `lakehouseadmin`.
2. Run:
   ```sql
   GRANT USAGE ON SCHEMA public TO "IAMR:AWSReservedSSO_AWSAdministratorAccess_378665f42c34a44b";
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO "IAMR:AWSReservedSSO_AWSAdministratorAccess_378665f42c34a44b";
   -- tables the Glue job creates later are readable too
   ALTER DEFAULT PRIVILEGES FOR USER lakehouseadmin IN SCHEMA public
     GRANT SELECT ON TABLES TO "IAMR:AWSReservedSSO_AWSAdministratorAccess_378665f42c34a44b";
   ```
3. Reconnect with your federated IAM identity. The three tables appear under **lakehouse_dw → public → Tables**.

If the Redshift namespace is re-created, these grants have to be run again.

---

## Differences from the capstone brief

- **Single account.** Everything is deployed to one account rather than separate landing zone, lake house, governance and consumer accounts.
- **Self-managed Kafka.** It's used instead of MSK, and reuses an existing Terraform-built cluster.
- **Glue jobs are scripts defined in CDK** (`glue_scripts/`). They still appear in Glue Studio.
- **The Step Functions workflow is defined in CDK.** It can still be opened in Workflow Studio.

## Useful commands

* `cdk ls`          list all stacks in the app
* `cdk synth`       emit the synthesized CloudFormation template
* `cdk diff`        compare the deployed stack with the current code
* `cdk deploy`      deploy the stack to the default AWS account and region
