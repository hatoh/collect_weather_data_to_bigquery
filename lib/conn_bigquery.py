import json
import os
import datetime
from logging import getLogger
import sys
import traceback
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = getLogger(__name__)


class ConnBigQuery():
    def __init__(self, auth_key_path):
        """
        サービスアカウントのjsonをロードする
        """
        self.auth_key_path = auth_key_path
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = auth_key_path

        self.client = bigquery.Client()

    def _create_schemafield_list(self, schema_file_path):
        """
        スキーマのjsonを引数にとって、BigQueryロード用のSchemaFieldを生成する
        see: https://cloud.google.com/bigquery/docs/schemas?hl=ja#creating_a_json_schema_file
        """
        with open(schema_file_path, mode='r') as input_file:
            json_file = input_file.read()
            schema_json = json.loads(json_file)

            schema_field_list = []
            for schema in schema_json:
                # fixme NULLABLE以外のmodeも対応できるように
                schema_field = bigquery.SchemaField(
                    schema['name'], schema['type'],
                    mode="NULLABLE",
                    description=schema['description']
                )
                schema_field_list.append(schema_field)

        return schema_field_list

    def load_csv_file(self, target_file_path, dataset_id, table_id, schema_file_path=None):
        """
        ローカルに保存されているBigQueryロード形式のjsonファイルをBQロードする
        schema_file_pathの指定がなければスキーマはautodetectする
        """
        dt_now = datetime.datetime.now()

        try:
            dataset_ref = self.client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)

            job_config = bigquery.LoadJobConfig()

            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1

            if schema_file_path is None:
                job_config.autodetect = True
            else:
                schema_field_tuple = self._create_schemafield_list(schema_file_path)
                job_config.schema = schema_field_tuple

            with open(target_file_path, "rb") as source_file:
                job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)

            job.result()  # Waits for table load to complete.

            logger.info("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

        except:
            logger.error(traceback.format_exc())
            # ロードできなかったjsonファイルをリネーム
            ng_file_name = target_file_path + '_ng'+ dt_now.strftime('_%Y-%m-%d_%H:%M:%S')
            os.rename(target_file_path, ng_file_name)
            logger.error('can\'t loaded json file: ' + ng_file_name)
            sys.exit(1)

        else:
            # ロードできたらjsonファイルをリネーム
            loaded_file_name = target_file_path + '_loaded' + dt_now.strftime('_%Y-%m-%d_%H:%M:%S')
            os.rename(target_file_path, loaded_file_name)
            logger.info('loaded json file: ' + loaded_file_name)

    def query_table(self, query_string):
        """
        BigQueryテーブルをqueryし、結果をreturnする
        """
        try:
            query = query_string
            query_job = self.client.query(query)
            results = query_job.result()

            query_results_dict_list = []
            for row in results:
                query_results_dict_list.append(dict(row))

            return query_results_dict_list

        except:
            logger.error(traceback.format_exc())
            sys.exit(1)

    def exists_table(self, table_id):
        """
        テーブル存在確認
        """
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False

    def exists_dataset(self, dataset_id):
        """
        データセット存在確認
        """
        try:
            self.client.get_dataset(dataset_id)
            return True
        except NotFound:
            return False

    def create_dataset(self, dataset_id):
        """
        データセット作成
        """
        try:
            self.client.create_dataset(dataset_id)
            logger.info('created dataset: '+ dataset_id)
        except:
            logger.error(traceback.format_exc())
            sys.exit(1)