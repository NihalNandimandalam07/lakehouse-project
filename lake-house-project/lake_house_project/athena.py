from aws_cdk import (
    Stack,
    CfnOutput,
    aws_athena as athena,
    aws_s3 as s3,
    aws_kms as kms,
)


#saved queries on the catalog tables (shown under Saved queries in the Athena console)
SAMPLE_QUERIES = {
    "CuratedRowCounts": (
        "lakehouse-curated-row-counts-nn",
        "Row counts for each curated table",
        """SELECT 'curated_employees' AS table_name, COUNT(*) AS row_count FROM curated_employees
UNION ALL
SELECT 'curated_customers', COUNT(*) FROM curated_customers
UNION ALL
SELECT 'curated_covid', COUNT(*) FROM curated_covid;""",
    ),
    "EmployeesByDepartment": (
        "lakehouse-employees-by-department-nn",
        "Headcount and salary by department",
        """SELECT department,
       COUNT(*) AS headcount,
       ROUND(AVG(salary), 2) AS avg_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary
FROM curated_employees
GROUP BY department
ORDER BY headcount DESC;""",
    ),
    "CustomersByState": (
        "lakehouse-customers-by-state-nn",
        "Customers and average income by state and status",
        """SELECT address_state,
       customer_status,
       COUNT(*) AS customers,
       ROUND(AVG(annual_income), 2) AS avg_annual_income
FROM curated_customers
GROUP BY address_state, customer_status
ORDER BY customers DESC;""",
    ),
    "CovidMonthlyByCountry": (
        "lakehouse-covid-monthly-by-country-nn",
        "Monthly COVID-19 cases and deaths by country (uses year/month partitions)",
        """SELECT year,
       month,
       country_short_name,
       SUM(new_cases) AS new_cases,
       SUM(new_deaths) AS new_deaths
FROM curated_covid
WHERE year = '2020'
GROUP BY year, month, country_short_name
ORDER BY new_cases DESC
LIMIT 50;""",
    ),
    "TransformedVsCurated": (
        "lakehouse-transformed-vs-curated-nn",
        "MySQL employees in transformed compared with the merged curated employees",
        """SELECT c.employee_id,
       c.employee_name,
       c.source,
       t.salary AS transformed_salary,
       c.salary AS curated_salary
FROM curated_employees c
LEFT JOIN mysql_employees t ON t.employee_id = c.employee_id
ORDER BY c.employee_id;""",
    ),
}


class LakehouseAthena:
    """Athena workgroup and saved queries over the lakehouse catalog.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        results_bucket: s3.IBucket,
        kms_key: kms.IKey,
        database_name: str = "lakehouse_catalog_nn",
    ) -> None:

        #Athena Workgroup
        #enforced settings: every query in this workgroup writes KMS-encrypted results
        self.workgroup = athena.CfnWorkGroup(
            scope,
            "LakehouseAthenaWorkgroup",
            name="lakehouse-athena-workgroup-nn",
            description="Lakehouse queries over lakehouse_catalog_nn",
            state="ENABLED",
            #lets CloudFormation delete the workgroup even after it has query history
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                #cost guard: stop any single query after 10 GB scanned
                bytes_scanned_cutoff_per_query=10 * 1024 ** 3,
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3"
                ),
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{results_bucket.bucket_name}/results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_KMS",
                        kms_key=kms_key.key_arn,
                    ),
                ),
            ),
        )

        #Saved Queries
        for construct_id, (name, description, query) in SAMPLE_QUERIES.items():
            athena.CfnNamedQuery(
                scope,
                f"LakehouseAthenaQuery{construct_id}",
                name=name,
                description=description,
                database=database_name,
                query_string=query,
                work_group=self.workgroup.ref,
            )

        #outputs
        CfnOutput(
            scope,
            "LakehouseAthenaWorkgroupName",
            value=self.workgroup.ref
        )
