# リスト6-1. 各種Pythonモジュールのインポート
import datetime
import os

import airflow
from airflow.contrib.operators import bigquery_operator, \
    bigquery_table_delete_operator, gcs_to_bq
import pendulum


# リスト6-2. DAG内のオペレータ共通のパラメータの定義
# DAG内のオペレータ共通のパラメータを定義する。
default_args = {
    'owner': 'gcpbook',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # DAG作成日の午前2時(JST)を開始日時とする。
    'start_date': pendulum.today('Asia/Tokyo').add(hours=2)
}

# リスト6-3. DAGの定義
# DAGを定義する。
with airflow.DAG(
        'count_users',
        default_args=default_args,
        # 日次でDAGを実行する。
        schedule_interval=datetime.timedelta(days=1),
        catchup=False) as dag:

    # リスト6-4. ユーザ行動ログ取り込みタスクの定義
    # Cloud Storage上のユーザ行動ログをBigQueryの作業用テーブルへ
    # 取り込むタスクを定義する。
    # load_events = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    #     task_id='load_events',
    #     bucket='cloud-composer-handson-gcpbook-ch5',
    #     source_objects=['data/events/{{ ds_nodash }}/*.json.gz'],
    #     destination_project_dataset_table='f"{dataset}".work_events',
    #     source_format='NEWLINE_DELIMITED_JSON'
    # )

    dataset = "handson_dataset"
    bucket = "cloud-composer-handson"

    load_events = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_events',
        bucket=bucket,
        source_objects=['data/events/20181001/events.csv'],
        destination_project_dataset_table=f"{dataset}.work_events",
        source_format='CSV',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'user_pseudo_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'event_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'event_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
        ]
    )

    # usersテーブルへのデータ読み込みタスクの定義
    load_users = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_users',
        bucket=bucket,
        source_objects=['data/users/users.csv'],
        destination_project_dataset_table=f"{dataset}.users",
        source_format='CSV',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'user_pseudo_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'is_paid_user', 'type': 'BOOLEAN', 'mode': 'REQUIRED'}
        ]
    )

    # リスト6-5. f"{dataset}".dauテーブルへの書き込みタスクの定義
    # BigQueryの作業用テーブルとユーザ情報テーブルを結合し、課金ユーザと
    # 無課金ユーザそれぞれのユーザ数を算出して、結果をf"{dataset}".dau
    # テーブルへ書き込むタスクを定義する。
    insert_dau = bigquery_operator.BigQueryOperator(
        task_id='insert_dau',
        use_legacy_sql=False,
        sql="""
            insert handson_dataset.dau
            select
                date('{{ ds }}') as dt
            ,   countif(u.is_paid_user) as paid_users
            ,   countif(not u.is_paid_user) as free_to_play_users
            from
                (
                    select distinct
                        user_pseudo_id
                    from
                        handson_dataset.work_events
                ) e
                    inner join
                        handson_dataset.users u
                    on
                        u.user_pseudo_id = e.user_pseudo_id
        """
    )

    # リスト6-6. 作業用テーブルを削除するタスクの定義
    # BigQueryの作業用テーブルを削除するタスクを定義する。
    delete_work_table = \
        bigquery_table_delete_operator.BigQueryTableDeleteOperator(
            task_id='delete_work_table',
            deletion_dataset_table=f"{dataset}.work_events"
        )

    # リスト6-7. タスクの依存関係の定義
    # 各タスクの依存関係を定義する。
    load_events >> load_users >> insert_dau >> delete_work_table