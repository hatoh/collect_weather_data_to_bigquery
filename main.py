import configparser
import datetime
import json
import traceback
import lib.conn_bigquery
from lib.download_uri import download_uri


# todo: logger入れる
# todo: データセットがなければまず作る
# todo: errorになったらexit(1)しないと。。
# todo: schemaファイルを書く（他の天気情報の）
# todo: readmeを書く

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
    print(str(will_collect_max_day) + 'までロード対象日')
    target_days = will_collect_max_day - max_collected_date  # datetime.timedelta
    sakanobori_day = target_days.days  # to int

    if sakanobori_day > sakanobori_day_max:
        sakanobori_day = sakanobori_day_max

    # fixme: 未来までロードされている場合のメッセージが不適当
    print(str(sakanobori_day) + '日まで遡る')
    return sakanobori_day


def _etl(source_url, intermediate_csv_file_path, dataset_id, table_id, schema_file_path):
    # csvダウンロード開始
    print('start download csv')

    download_uri(
        uri=source_url,
        dest_file_path=intermediate_csv_file_path
    )

    # bigqueryロード開始
    print('start load to bigquery')

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

    # load table setting
    target_table_dict_list = _load_target_table_list()

    try:
        # 処理対象テーブル分ループ
        for target_table in target_table_dict_list:
            # fixme: 中間ファイル出力ディレクトリパスをconfigに外だし
            intermediate_csv_file_path = './data/' + target_table["table_id"] + '.csv'
            print('target_table: ' + target_table['table_id'])

            # テーブル存在確認
            query_bigquery = lib.conn_bigquery.ConnBigQuery(
                auth_key_path=service_account_json
            )
            is_exists_table = query_bigquery.exists_table(
                table_id=project_id + '.' + target_table['dataset_id'] + '.' + target_table['table_id']
            )
            if is_exists_table:
                # いつまでロード済みかの日付を抽出
                max_collected_date = _search_max_collected_date(
                    project_id=project_id,
                    dataset_id=target_table['dataset_id'],
                    table_id=target_table['table_id']
                )
                print(str(max_collected_date) + 'までロード済み')

                # 何日前まで遡ってロードするかの日付を抽出
                sakanobori_day = _calulate_sakanobori_day(max_collected_date)

            else:
                # maxまで遡ってロードする。
                sakanobori_day = sakanobori_day_max
                print('テーブル新規作成')

            for day_count in reversed(range(sakanobori_day)):
                # 未来の日付までロードされていても何もしない
                # 過去から順にロードしたいのでreversedしている
                target_day = day_count + 1

                target_uri = target_table['source_uri'].replace("00_rct", "0"+str(target_day))
                _etl(
                    source_url=target_uri,
                    intermediate_csv_file_path=intermediate_csv_file_path,
                    dataset_id=target_table['dataset_id'],
                    table_id=target_table['table_id'],
                    schema_file_path=target_table['schema_file_path']
                )

                print(str(target_day)+'日前までロード完了')
    except:
        print(traceback.format_exc())