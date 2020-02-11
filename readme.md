# Collect_weather_data and load to Google BigQuery
 [気象庁](https://www.data.jma.go.jp/)のデータをダウンロードし、Google BigQueryにロードする

### usage
```
$ python main.py
```

## how to start
### Precondition
- required python version : 3.xx
- sign up Google Cloud Platform
- create service account 
- add roles to service account
    - roles/bigquery.dataEditor
    - roles/bigquery.jobUser

### set up this tool
```
$ pip install -r requirementes.txt
$ mv conf/config.ini{.sample,}
$ vi conf/config.ini
$ mv conf/target_table_list.json{.sample,}
$ vi conf/target_table_list.json
```