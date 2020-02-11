import configparser
import datetime
import json
from logging import getLogger, basicConfig, INFO, DEBUG
import sys
import traceback
import lib.conn_bigquery
from lib.download_uri import download_uri


def _search_max_collected_date(project_id, dataset_id, table_id):
    """
    BQをクエリして、収集済みの日付を抽出する
    """
    query_bigquery = lib.conn_bigquery.ConnBigQuery(
        auth_key_path=service_account_json
    )
    collected_date = query_bigquery.query_table(
        query_string=(
            'SELECT max(date(observation_year, observation_month, observation_day)) as max_date'
            '  FROM `{project_id}.{dataset_id}.{table_id}`'
            ' LIMIT 1'
        ).format(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id
        )
    )

    for date in collected_date:
        max_collected_date = date['max_date']

    return max_collected_date


def _load_target_table_list():
    """
    処理対象テーブル設定をロードする
    """
    with open(target_table_list_file_path, mode='r') as target_table_json_file:
        target_table_file = target_table_json_file.read()
        target_table_dict_list = json.loads(target_table_file)
        return target_table_dict_list


def _calulate_sakanobori_day(max_collected_date):
    """
    何日分遡って取得すべきか計算する
    """
    # fixme: JST以外が未考慮
    will_collect_max_day = datetime.date.today() - datetime.timedelta(days=1)
    logger.debug(str(will_collect_max_day) + 'までロード対象日')

    target_days = will_collect_max_day - max_collected_date  # datetime.timedelta
    sakanobori_day = target_days.days  # to int

    if sakanobori_day > sakanobori_day_max:
        sakanobori_day = sakanobori_day_max

    # fixme: 未来までロードされている場合のメッセージが不適当
    logger.debug(str(sakanobori_day) + '日まで遡る')
    return sakanobori_day


def _etl(source_url, intermediate_csv_file_path, dataset_id, table_id, schema_file_path):
    # csvダウンロード開始
    logger.info('start downloading csv. uri is : ' + source_url)

    download_uri(
        uri=source_url,
        dest_file_path=intermediate_csv_file_path
    )

    # BigQueryロード開始
    logger.info('start loading to ' + project_id + '.' + dataset_id + '.' + table_id)

    bq = lib.conn_bigquery.ConnBigQuery(
        auth_key_path=service_account_json
    )
    bq.load_csv_file(
        target_file_path=intermediate_csv_file_path,
        dataset_id=dataset_id,
        table_id=table_id,
        schema_file_path=schema_file_path
    )


if __name__ == '__main__':
    # set config
    config_ini = configparser.ConfigParser()
    config_ini.read('./conf/config.ini', encoding='utf-8')

    settings = config_ini['settings']
    service_account_json = settings.get('service_account_json')
    project_id = settings.get('project_id')
    target_table_list_file_path = settings.get('target_table_list_file_path')
    sakanobori_day_max = int(settings.get('sakanobori_day_max'))

    log_level = settings.get('log_level')

    # load table setting
    target_table_dict_list = _load_target_table_list()

    # logger setting
    dt_now = datetime.datetime.now()
    logfile_path = './log/' + dt_now.strftime('%Y-%m-%d') + '.log'
    logger = getLogger(__name__)
    basicConfig(
        filename=logfile_path,
        level=log_level,
        # stream=sys.stdout,
        format='%(asctime)s %(name)s [%(levelname)s] %(message)s'
    )

    try:
        # 処理対象テーブル分ループ
        for target_table in target_table_dict_list:
            # fixme: 中間ファイル出力ディレクトリパスをconfigに外だし
            intermediate_csv_file_path = './data/' + target_table["table_id"] + '.csv'
            logger.info('etl target table_name is ' + target_table['table_id'])

            # データセット存在確認
            bigquery_client = lib.conn_bigquery.ConnBigQuery(
                auth_key_path=service_account_json
            )
            is_exists_dataset = bigquery_client.exists_dataset(
                dataset_id=project_id + '.' + target_table['dataset_id']
            )
            logger.debug('dataset is already exists: ' + str(is_exists_dataset))

            if not is_exists_dataset:
                bigquery_client.create_dataset(target_table['dataset_id'])

            # テーブル存在確認
            is_exists_table = bigquery_client.exists_table(
                table_id=project_id + '.' + target_table['dataset_id'] + '.' + target_table['table_id']
            )
            logger.debug('table is already exists: ' + str(is_exists_dataset))

            if is_exists_table:
                # いつまでロード済みかの日付を抽出
                max_collected_date = _search_max_collected_date(
                    project_id=project_id,
                    dataset_id=target_table['dataset_id'],
                    table_id=target_table['table_id']
                )
                logger.debug(str(max_collected_date) + 'までロード済み')

                # 何日前まで遡ってロードするかの日付を抽出
                sakanobori_day = _calulate_sakanobori_day(max_collected_date)

            else:
                # maxまで遡ってロードする。
                sakanobori_day = sakanobori_day_max
                logger.debug('テーブルが存在しないので' + str(sakanobori_day_max) + 'まで遡って取得する')

            for day_count in reversed(range(sakanobori_day)):
                # 未来の日付までロードされていても何もしない
                # 過去から順にロードしたいのでreversedしている
                target_day = day_count + 1

                target_uri = target_table['source_uri'].replace("00_rct", "0"+str(target_day))
                logger.info(str(target_day) + '日前のロード開始')
                _etl(
                    source_url=target_uri,
                    intermediate_csv_file_path=intermediate_csv_file_path,
                    dataset_id=target_table['dataset_id'],
                    table_id=target_table['table_id'],
                    schema_file_path=target_table['schema_file_path']
                )

                logger.info(str(target_day) + '日前までロード完了')
    except:
        logger.error(traceback.format_exc())
        sys.exit(1)

    else:
        logger.info('etl finished')