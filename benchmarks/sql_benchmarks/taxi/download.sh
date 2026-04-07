wget -i files.txt -P data_csv/
mkdir data/
datafusion-cli -q -f ./csv_to_parquet.sql
rmdir data_csv