# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import inspect

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable


class S3ToRedshiftOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Redshift
    :param schema: reference to a specific schema in redshift database
    :type schema: string
    :param table: reference to a specific table in redshift database
    :type table: string
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: string
    :param s3_key: reference to a specific S3 key
    :type s3_key: string
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: string
    :param copy_options: reference to a list of COPY options
    :type copy_options: list
    """

    @apply_defaults
    def __init__(
            self,
            schema,
            table,
            s3_bucket,
            s3_key, #리스트 형식
            previous_task_id=None,
            redshift_conn_id='redshift_dev_db',
            aws_conn_id='aws_default',
            copy_options=tuple(),
            overwrite=True,
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.previous_task_id = previous_task_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters
        self.overwrite = overwrite

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(self.aws_conn_id)
        iam_role = Variable.get("iam_role_for_copy_access_token")

        credentials = self.s3.get_credentials('ap-northeast-2')

        access_key = credentials.access_key
        secret_key = credentials.secret_key

        temp_table = "temp_{table}".format(table=self.table)

        queries = ""
        if self.overwrite:
            queries = """DROP TABLE IF EXISTS {schema}.{temp_table};
                CREATE TABLE {schema}.{temp_table}(LIKE {schema}.{table});""".format(
                    table=self.table,
                    temp_table=temp_table,
                    schema=self.schema
                )
            
            self.log.info('Executing COPY command... ' + queries)
            self.hook.run(queries, self.autocommit)

        for key in self.s3_key:
            queries = """
                COPY {schema}.{table}
                FROM 's3://{s3_bucket}/{s3_key}'
                credentials 'aws_iam_role={iam_role}'
                {copy_options};
            """.format(schema=self.schema,
                       table=temp_table if self.overwrite else self.table,
                       s3_bucket=self.s3_bucket,
                       s3_key=key,
                       access_key=access_key,
                       secret_key=secret_key,
                       iam_role=iam_role,
                       copy_options=self.copy_options)

            self.log.info('Executing COPY command... ' + queries)
            self.hook.run(queries, self.autocommit)

        if self.overwrite:
            self.log.info("COPY command complete to a temp table...")
            queries = "BEGIN;"
            queries += "DROP TABLE {schema}.{table} CASCADE;".format(
                table=self.table,
                schema=self.schema
            )
            queries += 'ALTER TABLE {schema}.{temp_table} rename to "{table}";'.format(
                table=self.table,
                temp_table=temp_table,
                schema=self.schema
            )
            queries += "END;"

            self.log.info('Executing SWAP command... ' + queries)
            self.hook.run(queries, self.autocommit)
            self.log.info("COPY command complete to a temp table...")

