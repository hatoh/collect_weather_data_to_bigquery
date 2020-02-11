# collect weather data and load to Google BigQuery
 [気象庁](https://www.data.jma.go.jp/obd/stats/data/mdrr/docs/csv_dl_readme.html)の気象データをダウンロードし、Google BigQueryにロードする

### usage
```
$ python main.py
```

## how to start
### Precondition
- required python version : 3.xx
- sign up Google Cloud Platform
- create service account
- create service account json 
- add roles to service account
    - roles/bigquery.dataEditor
    - roles/bigquery.jobUser

### set up this tool
```
$ pip install -r requirements.txt
$ mv conf/config.ini{.sample,}
$ vi conf/config.ini
$ mv conf/target_table_list.json{.sample,}
$ vi conf/target_table_list.json
```