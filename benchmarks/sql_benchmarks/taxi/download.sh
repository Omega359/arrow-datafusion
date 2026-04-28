for url in $(cat files.txt); do curl --retry 3 --create-dirs --output-dir ./data_csv -w "Downloaded: %{filename_effective}" -# -O -C - "$url"; done
mkdir data/
datafusion-cli -q -f ./csv_to_parquet.sql
rmdir -rf data_csv