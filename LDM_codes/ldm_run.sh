




bq query --use_legacy_sql=false --format=csv 'SELECT DISTINCT subs_week_and_year FROM `skyuk-uk-data-repo-3a-prod.uk_pub_olive_mirror_ic.cust_weekly_base` WHERE subs_week_and_year >=201729 and subs_week_and_year<=201813 order by subs_week_and_year'|tail -n +3 > subs_week_year.txt
cat subs_week_year.txt | while read line
do
   spark-submit   --packages com.databricks:spark-avro_2.11:4.0.0 --num-executors 10 --executor-cores 8  --executor-memory 20G --driver-memory 20G --conf spark.debug.maxToStringFields=100 ldm_run_live_v1.py --yearweek $line --output_schema_name 'spark' --output_table_name 'fractal_customer_360'
done


