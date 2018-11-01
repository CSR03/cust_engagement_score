from __future__ import absolute_import
import json
import pprint
import subprocess
import pyspark
from pyspark.sql import SQLContext,Row
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import *
from google.cloud import bigquery
import pandas as pd
import re
sc = pyspark.SparkContext()
spark = SparkSession(sc)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sc.setLogLevel("ERROR")
import sys 
from io import StringIO
import argparse
import warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


def process_raw_data(dataset,table,query,project_id):
    try:
        start = datetime.now()
        client = bigquery.Client(project = project_id)
        job_config = bigquery.QueryJobConfig()
        job_config.allow_large_results = True
        dest_dataset_ref = client.dataset(dataset)
        dest_table_ref = dest_dataset_ref.table(table)
        job_config.destination = dest_table_ref
        job_config.write_disposition = 'WRITE_TRUNCATE'
        print('loaded params in '+str(datetime.now()-start))
        print(query)
        query_job = client.query(query, job_config=job_config)
        print('query fired in '+str(datetime.now()-start))
        query_job.result()  # Wait for job to complete
        print('completed in '+str(datetime.now()-start))
    except Exception as e:
        print('failed - '+str(e))

def generate_deltas(sd,yw):
	offers_software_join_cols = [\
	'a.account_number',
	'a.Offer_End_Modified',\
	'a.subs_type',\
	'a.offer_status',\
	'a.percent_value',\
	'a.Intended_Whole_offer_amount',\
	'a.Offer_Leg_Start_Dt_Actual',\
	'a.Offer_Leg_End_Dt_Actual',\
	'a.last_modified_dt',\
	]
	offers_software_cols = ['x.'+col[2:] for col in offers_software_join_cols]
	original_offers_software_cols = [col[2:] for col in offers_software_join_cols]
	offers_subs_types = [\
	'SKY_KIDS',\
	'SPORTS',\
	'HD Pack',\
	'SKY_BOX_SETS',\
	'DTV HD',\
	'MS+',\
	'DTV Extra Subscription',\
	'DTV Primary Viewing',\
	'STANDALONESURCHARGE',\
	'CINEMA',\
	'Broadband DSL Line',\
	'SKY TALK LINE RENTAL',\
	'SKY TALK SELECT',\
	'Sky Go Extra',\
	]
	pl_entries_cols = \
	[\
	'a.account_number',\
	'a.AB_Next_Status_Code',\
	'a.AB_Pending_Termination',\
	'a.Same_Day_AB_Reactivation',\
	'a.Same_Day_PC_Reactivation',\
	]
	query_list = [['line_speed_query','select k.* from ('+'SELECT x.account_number,x.x_down_speed_mbps,x.x_line_length_km,x.x_up_speed_mbps,x.stability,x.rag_status,c.subs_week_and_year  FROM (    SELECT      account_number,      x_down_speed_mbps, x_line_length_km,      x_up_speed_mbps,      stability,      snapshot_date,      rag_status  FROM      `'+get_schema_table_dict['line_speed_detail']+'`    WHERE      snapshot_date BETWEEN DATE_ADD(DATE \''+sd+'\',INTERVAL -30 DAY)      AND DATE_ADD(DATE \''+sd+'\',INTERVAL 30 DAY)) x INNER JOIN    `'+get_schema_table_dict['sky_calendar']+'` c  ON    x.snapshot_date = date(c.calendar_date) wHERE c.subs_week_and_year = ' + yw + ' and account_number is not null and account_number <> \'?\') k ','skyuk-uk-prechurn-poc'],\
['pipeline_query','select '+','.join(pl_entries_cols)+' from (select * from `'+get_schema_table_dict['entries_dtv']+'` where subs_week_and_year = '+yw[1:-1]+') a inner join `'+get_schema_table_dict['sky_calendar']+'` b on a.event_dt = date(b.calendar_date) where b.subs_week_and_year = ' + yw,'skyuk-uk-prechurn-poc'],\
['offers_query','select '+','.join(offers_software_join_cols)+' from (select '+','.join(offers_software_cols)+',PARSE_DATE(\'%Y-%m-%d\',\'' + sd + '\') as sd from  `'+get_schema_table_dict['offers_software']+'` x where PARSE_DATE(\'%Y-%m-%d\',\'' + sd + '\') between offer_leg_start_dt_actual and offer_leg_end_dt_actual ) a inner join `'+get_schema_table_dict['sky_calendar']+'` b on a.sd = date(b.calendar_date)','skyuk-uk-prechurn-poc'],\
['vip_eligibility_query','''SELECT x.VIP_PROGRAM_ELIGIBILITY, DATE(x.VIP_TENURE_START_DATE) as VIP_TENURE_START_DATE, y.account_number FROM `'''+get_schema_table_dict['dim_customer']+'''` x INNER JOIN `'''+get_schema_table_dict['account_fo_extn']+'''` y  ON x.party_id = y.PRIMARY_CONTACTOR_SRC_SYSTEM_ID where DATE(x.VIP_TENURE_START_DATE) between DATE_ADD(DATE \''''+sd+'''\', INTERVAL -30 DAY) and DATE_ADD(DATE \''''+sd+'''\', INTERVAL 30 DAY)''','skyuk-uk-prechurn-poc'],\
['vip_claim_query','''select c.account_number, d.subs_week_and_year,a.STATUS,b.TYPE_CODE from `'''+get_schema_table_dict['customer_rewards']+'''` a left join `'''+get_schema_table_dict['vip_rewards']+'''` b on a.REWARD_ID = b.ID inner join `'''+get_schema_table_dict['account_fo_extn']+'''` c on a.PARTY_ID = c.PRIMARY_CONTACTOR_SRC_SYSTEM_ID inner join `'''+get_schema_table_dict['sky_calendar']+'''` d on DATE(a.CLAIMED_DATE) = date(d.calendar_date) where d.subs_week_and_year = ''' + yw,'skyuk-uk-prechurn-poc'],\
['nps_query','select a.account_number,a.q1,a.q2,a.q2_7,b.subs_week_and_year from `'+get_schema_table_dict['csat_survey']+'` a inner join `'+get_schema_table_dict['sky_calendar']+'` b on date(a.call_date) = date(b.calendar_date) where b.subs_week_and_year = ' + yw,'skyuk-uk-prechurn-poc'],\
['calls_query','select x.*,y.subs_week_and_year from(select a.COMMUNICATION_CHANNEL, a.INTERACTION_DIRECTION, a.INTERACTION_START_DT, a.INTERACTION_DURATION_SEC, a.TOTAL_MEDIATION_EVENTS, a.TOTAL_MEDIATION_DURATION, a.TOTAL_ABANDONED_MEDIATIONS, a.TOTAL_RESOURCE_EVENTS, a.TOTAL_UNIQUE_AGENTS,a.TOTAL_CUSTOMER_TALK_DURATION, a.TOTAL_CUSTOMER_HOLD_EVENTS, a.TOTAL_CUSTOMER_HOLD_COUNT,a.TOTAL_CUSTOMER_HOLD_DURATION, a.TOTAL_WARM_TRANSFERS, a.TOTAL_COLD_TRANSFERS, a.TOTAL_CUST_CONFERENCED_EVENTS, a.CUSTOMER_ABANDON_FLAG,b.account_number from `'+get_schema_table_dict['interaction_summary']+'` a inner join `'+get_schema_table_dict['account_fo_extn']+'` b on a.party_id = b.PRIMARY_CONTACTOR_SRC_SYSTEM_ID where party_id <> \'?\') x inner join `'+get_schema_table_dict['sky_calendar']+'` y on date(x.INTERACTION_START_DT) = date(y.calendar_date) where y.subs_week_and_year =' + yw,'skyuk-uk-prechurn-poc'],\
['downloads_query','''select account_number,subs_week_and_year,genre_desc,sum(download_count) total_downloads from
(select a.account_number,b.subs_week_and_year,a.card_id,a.provider_id,a.asset_id,date(a.last_modified_dt) download_date,genre_desc,count(*) download_count
from `'''+get_schema_table_dict['anytime_downloads']+'''` a inner join `'''+get_schema_table_dict['sky_calendar']+'''` b on date(a.last_modified_dt) = date(b.calendar_date) where (a.cs_referer not like '%Watch Next%' or a.cs_referer not like '%Download Next%' or a.cs_referer not like '%DownloadNext%' or a.cs_referer not like '%WatchNext%' or a.cs_referer
not like '%Personalised Recommendation%' or a.cs_referer not like '%Trailer%' or a.cs_referer not like '%Mosaic Tile%') AND a.requested_bytes_from = 188
and b.subs_week_and_year = '''+yw+''' group by 1,2,3,4,5,6,7)
group by 1,2,3''','skyuk-uk-prechurn-poc'],\
['billing_query','''select y.account_number,y.subs_week_and_year,y.bill_total,y.amount_paid,y.payment_delay_in_days from
(SELECT 
f.account_number,b.subs_week_and_year,b.BASE_CURRENCY_RATE*b.TOTAL_DUE_AMT bill_total,-1*b.BASE_CURRENCY_RATE*b.TOTAL_PAID_AMT amount_paid,DATE_DIFF(date(b.BALANCE_ZERO_DT),date(b.tax_date), DAY) payment_delay_in_days,ROW_NUMBER() OVER (PARTITION BY f.account_number ORDER BY b.tax_date DESC) as ranking
FROM
(select x.*,c.subs_week_and_year from (select * from `'''+get_schema_table_dict['wh_bills']+'''`) x inner join `'''+get_schema_table_dict['sky_calendar']+'''` c on date(x.tax_date) = date(c.calendar_date) where c.subs_week_and_year = '''+yw+''' and x.bill_period like 'M%')  b
INNER JOIN
`'''+get_schema_table_dict['cust_account_bo']+'''` cabo
ON
(b.BO_ACCOUNT_NO = cabo.SRC_SYSTEM_ID)
INNER JOIN
`'''+get_schema_table_dict['cust_account_fo']+'''` f
ON
(cabo.FO_SRC_SYSTEM_ID = f.SRC_SYSTEM_ID)) y
where y.ranking = 1''','skyuk-uk-prechurn-poc'],\
['cwb_query','''SELECT
a.account_number,
a.subs_week_and_year,
a.ADSL_Enabled,
a.BB_Product_Holding,
a.BB_Provider,
a.BB_Status_Code,
CAST(REGEXP_REPLACE(a.curr_offer_length_bb, 'M','') as INT64) AS curr_offer_length_bb,
CAST(REGEXP_REPLACE(a.Curr_Offer_Length_DTV, 'M','') as INT64) AS Curr_Offer_Length_DTV,
CAST(REGEXP_REPLACE(a.Curr_Offer_Length_LR, 'M','') as INT64) AS Curr_Offer_Length_LR,
CAST(REGEXP_REPLACE(a.Curr_Offer_Length_Movies, 'M','') as INT64) Curr_Offer_Length_Movies,
CAST(REGEXP_REPLACE(a.Curr_Offer_Length_Sports, 'M','') as INT64) Curr_Offer_Length_Sports,
CAST(REGEXP_REPLACE(a.Curr_Offer_Length_Talk, 'M','') as INT64) Curr_Offer_Length_Talk,
a.DTV_Product_Holding,
a.DTV_Status_Code,
a.HD_Product_Holding,
a.HD_Status_Code,
a.Home_Owner_Status,
a.LR_Product_Holding,
a.LR_Status_Code,
a.MS_Product_Holding,
a.MS_Status_Code,
a.Movies_Product_Holding,
a.Movies_Status_Code,
a.Prems_Product_Holding,
a.Prems_Status_Code,
CAST(REGEXP_REPLACE(a.Prev_Offer_Length_DTV, 'M','') as INT64) Prev_Offer_Length_DTV,
CAST(REGEXP_REPLACE(a.Prev_Offer_Length_LR, 'M','') as INT64) Prev_Offer_Length_LR,
CAST(REGEXP_REPLACE(a.Prev_Offer_Length_Movies, 'M','') as INT64) Prev_Offer_Length_Movies,
CAST(REGEXP_REPLACE(a.Prev_Offer_Length_Sports, 'M','') as INT64) Prev_Offer_Length_Sports,
CAST(REGEXP_REPLACE(a.Prev_Offer_Length_Talk, 'M','') as INT64) Prev_Offer_Length_Talk,
a.SGE_Product_Holding,
a.SGE_Status_Code,
a.SkyPlus_Product_Holding,
a.SkyPlus_Status_Code,
a.Sports_Product_Holding,
a.Sports_Status_Code,
a.TT_Product_Holding,
a.TT_Status_Code,
a.Talk_Product_Holding,
a.Talk_Status_Code,
a.financial_strategy,
a.h_affluence,
a.h_age_coarse,
a.h_age_fine,
a.h_equivalised_income_band,
a.h_family_lifestage,
a.h_household_composition,
a.h_income_band,
a.h_mosaic_group,
a.h_presence_of_child_aged_0_4,
a.h_presence_of_child_aged_12_17,
a.h_presence_of_child_aged_5_11,
a.h_presence_of_young_person_at_address,
a.h_property_type,
a.h_residence_type,
a.p_true_touch_group,
a.skyfibre_enabled,
a.Age,
a.AnyBB_Calls_Ever,
a.BB_Enter_3rd_Party_Ever,
a.BB_Enter_CusCan_Ever,
a.BB_Enter_HM_Ever,
a.BB_Enter_SysCan_Ever,
a.BB_Subscriber_Activations_Ever,
a.BB_Subscription_Activations_Ever,
a.BT_Consumer_Market_Share,
a.Broadband_Average_Demand,
a.Curr_Offer_Amount_BB,
a.Curr_Offer_Amount_DTV,
a.Curr_Offer_Amount_LR,
a.Curr_Offer_Amount_Movies,
a.Curr_Offer_Amount_Sports,
a.Curr_Offer_Amount_Talk,
a.DTV_AB_Reactivations_Ever,
a.DTV_ABs_Ever,
a.DTV_Activations_Ever,
a.DTV_Churns_Ever,
a.DTV_CusCan_Churns_Ever,
a.DTV_NewCust_Activations_Ever,
a.DTV_PC_Reactivations_Ever,
a.DTV_PC_To_ABs_Ever,
a.DTV_PCs_Ever,
a.DTV_PO_Cancellations_Ever,
a.DTV_PO_Reinstates_Ever,
a.DTV_PO_Winbacks_Ever,
a.DTV_Reinstates_Ever,
a.DTV_SC_Reinstates_Ever,
a.DTV_SC_Winbacks_Ever,
a.DTV_SameDayCancels_Ever,
a.DTV_SysCan_Churns_Ever,
a.Movies_Activations_Ever,
a.Movies_Downgrades_Ever,
a.Movies_New_Adds_Ever,
a.Movies_Platform_Churns_Ever,
a.Movies_Reinstates_Ever,
a.Movies_Upgrades_Ever,
a.Offers_Applied_Ever_BB,
a.Offers_Applied_Ever_DTV,
a.Offers_Applied_Ever_LR,
a.Offers_Applied_Ever_Movies,
a.Offers_Applied_Ever_Sports,
a.Offers_Applied_Ever_Talk,
a.Prems_Product_Count,
a.Sky_Consumer_Market_Share,
a.Sports_Activations_Ever,
a.Sports_Downgrades_Ever,
a.Sports_New_Adds_Ever,
a.Sports_Platform_Churns_Ever,
a.Sports_Product_Count,
a.Sports_Reinstates_Ever,
a.Sports_Upgrades_Ever,
a.TT_Product_Count,
a.TalkTalk_Consumer_Market_Share,
a.Throughput_Speed,
a.Value_Calls_Ever,
a.Virgin_Consumer_Market_Share,
a.h_income_value,
a.h_number_of_adults,
a.h_number_of_bedrooms,
a.h_number_of_children_in_household,
a.max_speed_uplift,
a.p_true_touch_type,
a.skyfibre_enabled_perc,
a.skyfibre_planned_perc,
a.Country_Name,
a.Exchange_Status,
a._1st_LiveChat_outcome,
a._1st_LiveChat_reason,
a._1st_LiveChat_site,
a._1st_TA_outcome,
a._1st_TA_reason,
a._1st_TA_site,
a.Movies_Product_Count,
a.cable_postcode,
a.DTV_Curr_Contract_Start_Dt,
a.BB_Curr_Contract_Start_Dt,
a.Talk_Curr_Contract_Start_Dt,
a.DTV_1st_Activation_Dt,
a.DTV_Active,
a.AnyBB_Calls_In_Next_7D,
a.BB_Enter_3rd_Party_In_Next_7D,
a.BB_Enter_CusCan_In_Next_7D,
a.BB_Enter_HM_In_Next_7D,
a.BB_Enter_SysCan_In_Next_7D,
a.BB_Subscriber_Activations_In_Next_7D,
a.BB_Subscription_Activations_In_Next_7D,
a.DTV_AB_Reactivations_In_Next_7D,
a.DTV_ABs_In_Next_7D,
a.DTV_Activations_In_Next_7D,
a.DTV_Churns_In_Next_7D,
a.DTV_CusCan_Churns_In_Next_7D,
a.DTV_NewCust_Activations_In_Next_7D,
a.DTV_PC_Reactivations_In_Next_7D,
a.DTV_PC_To_ABs_In_Next_7D,
a.DTV_PCs_In_Next_7D,
a.DTV_PO_Cancellations_In_Next_7D,
a.DTV_PO_Reinstates_In_Next_7D,
a.DTV_PO_Winbacks_In_Next_7D,
a.DTV_Reinstates_In_Next_7D,
a.DTV_SC_Reinstates_In_Next_7D,
a.DTV_SC_Winbacks_In_Next_7D,
a.DTV_SameDayCancels_In_Next_7D,
a.DTV_SysCan_Churns_In_Next_7D,
a.LiveChat_nonsaves_in_next_7d,
a.LiveChat_saves_in_next_7d,
a.LiveChats_in_next_7d,
a.Movies_Activations_In_Next_7D,
a.Movies_Downgrades_In_Next_7D,
a.Movies_New_Adds_In_Next_7D,
a.Movies_Platform_Churns_In_Next_7D,
a.Movies_Reinstates_In_Next_7D,
a.Movies_Upgrades_In_Next_7D,
a.Offers_Applied_Next_7D_BB,
a.Offers_Applied_Next_7D_DTV,
a.Offers_Applied_Next_7D_LR,
a.Offers_Applied_Next_7D_Movies,
a.Offers_Applied_Next_7D_Sports,
a.Offers_Applied_Next_7D_Talk,
a.Payment_Due_Day_of_Month,
a.Sports_Activations_In_Next_7D,
a.Sports_Downgrades_In_Next_7D,
a.Sports_New_Adds_In_Next_7D,
a.Sports_Platform_Churns_In_Next_7D,
a.Sports_Reinstates_In_Next_7D,
a.Sports_Upgrades_In_Next_7D,
a.TA_nonsaves_in_next_7d,
a.TA_saves_in_next_7d,
a.TAs_in_next_7d,
a.Value_Calls_In_Next_7D,
c.subs_month_of_year
FROM
`'''+get_schema_table_dict['cwb_query']+'''` a
INNER JOIN (
SELECT
            CAST(subs_week_and_year AS INT64) AS subs_week_and_year, subs_month_of_year
FROM
            `'''+get_schema_table_dict['sky_calendar']+'''`
GROUP BY
            1,
            2) c
ON
a.subs_week_and_year = CAST(c.subs_week_and_year AS INT64)
WHERE
            a.country = 'UK'
            and a.dtv_active = 1 and
a.subs_week_and_year ='''+yw[1:-1],'skyuk-uk-prechurn-poc'],\
['stb_query','''select x.account_number,x.x_box_type,x.subs_week_and_year from 
(select a.account_number,a.x_box_type,b.subs_week_and_year from 
((select account_number , x_box_type , box_installed_dt,PARSE_DATE('%Y-%m-%d', \''''+sd+'''\') sd,row_number() 
OVER (PARTITION BY account_number ORDER BY box_installed_dt DESC) rank from `'''+get_schema_table_dict['set_top_box']+'''` where x_box_type
not in ('Sky Q Mini') and last_modified_dt <= PARSE_DATE('%Y-%m-%d',\''''+sd+'''\') ) a
inner join 
(select subs_week_and_year,calendar_date from `'''+get_schema_table_dict['sky_calendar']+'''`) b
on a.sd = date(b.calendar_date)) where a.rank = 1) x''','skyuk-uk-prechurn-poc'],\
['accenture_query','''SELECT
j.*
FROM (
SELECT
            x.account_number,
            x.event_sub_category,
            x.vip_loyalty_status,
            x.vip_tier_name,
            x.event_type,
            y.subs_week_and_year
FROM (
            SELECT
              b.account_number_unobfuscated AS account_number,
              case when a.event_sub_category = 'Recording / Playback Issue' then 'Recording / Playback Issue' when a.event_sub_category = 'Recording/ Playback Issue'
              then 'Recording / Playback Issue' when a.event_sub_category = 'Default/Vague ' then 'Default/Vague' when a.event_sub_category = 'Default / Vague' then 'Default/Vague' else a.event_sub_category end as event_sub_category,
              a.vip_loyalty_status,
              a.vip_tier_name,
              a.event_start_date,
              a.event_type
            FROM
              `skyuk-uk-ds-csg-prod.csg_customer_journey_eu.Full_Customer_Transaction_journey` a
            INNER JOIN
              `skyuk-uk-ds-csg-prod.csg_mapping_tables.account_number_map` b
            ON
              a.account_number = b.account_number_obfuscated) x
INNER JOIN
            `'''+get_schema_table_dict['sky_calendar']+'''` y
ON
            x.event_start_date = date(y.calendar_date)
WHERE
y.subs_week_and_year = '''+yw+'''    AND (x.event_sub_category IS NOT NULL
OR x.event_sub_category NOT IN ('Blank',
'')
OR x.event_sub_category not like '%Default%')) j''','skyuk-uk-prechurn-poc']]




	for i,query_details in enumerate(query_list):
		print(i)
		table_name = query_details[0]
		query = query_details[1]
		project = query_details[2]
		print(query)
		process_raw_data('deltas_1',table_name,query,project)

def delete_directory():
	try:
        	bucket = 'temp_ces'
	        project = spark._jsc.hadoopConfiguration().get('fs.gs.project.id')
	        input_directory = 'gs://pyspark-input'.format(bucket)
	        # Manually clean up the staging_directories, otherwise BigQuery
	        # files will remain indefinitely.
	        input_path = spark._jvm.org.apache.hadoop.fs.Path(input_directory)
	        input_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(input_path, True)
	        output_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_output'.format(bucket)
        	output_path = spark._jvm.org.apache.hadoop.fs.Path(output_directory)
	        output_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(
                output_path, True)
	except Exception as e:
		print('Error deleting directory , error details -',str(e))
		pass

def fetch_data(dataset_id):
	schema_id = 'deltas_1'
	subprocess.check_call(
	'bq --location=EU extract --destination_format AVRO '
	'--compression SNAPPY {d}.{t} gs://{gcs_1}/{gcs_2}.avro'.format(d=schema_id,t=dataset_id,gcs_1='pyspark-input',gcs_2=dataset_id+"-*").split())
	df = spark.read.format("com.databricks.spark.avro").load("gs://pyspark-input//"+dataset_id+"-*"+".avro")
	return df

def fetch_data_old(dataset_id):
	# Use the Cloud Storage bucket for temporary BigQuery export data used
	# by the InputFormat. This assumes the Cloud Storage connector for
	# Hadoop is configured.
	bucket = 'temp_ces'
	project = spark._jsc.hadoopConfiguration().get('fs.gs.project.id')
	input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input/{}'.format(bucket,dataset_id)
	# Manually clean up the staging_directories, otherwise BigQuery
	# files will remain indefinitely.
	input_path = spark._jvm.org.apache.hadoop.fs.Path(input_directory)
	input_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(input_path, True)
	# Stage data formatted as newline-delimited JSON in Cloud Storage.
	output_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/{}'.format(bucket,dataset_id)
	output_files = output_directory + '/part-*'
	output_path = spark._jvm.org.apache.hadoop.fs.Path(output_directory)
	output_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(
		output_path, True)
	conf = {
		# Input Parameters.
		'mapred.bq.project.id': project,
		'mapred.bq.gcs.bucket': bucket,
		'mapred.bq.temp.gcs.path': input_directory,
		'mapred.bq.input.project.id': 'skyuk-uk-prechurn-poc',
		'mapred.bq.input.dataset.id': 'deltas_1',
		'mapred.bq.input.table.id': dataset_id,
	}
	start = datetime.now()
	# Load data in from BigQuery.
	table_data = sc.newAPIHadoopRDD(
		'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
		'org.apache.hadoop.io.LongWritable',
		'com.google.gson.JsonObject',
		conf=conf)	
	table_json = table_data.map(lambda x: x[1])
	table_df = spark.read.json(table_json)
	print('fetched in ',str(datetime.now()-start))
	return table_df

def process_line_speed_data(df):
  types = df.select("rag_status").distinct().rdd.flatMap(lambda x: x).collect()
  types_expr = [F.when(F.col("rag_status") == ty, 1).otherwise(0).alias("rag_status_" + ty) for ty in types]
  df_ohe = df.select("*", *types_expr)
  return df_ohe.groupBy(['account_number','subs_week_and_year'])\
  .agg(min("x_down_speed_mbps").alias("min_x_down_speed_mbps"),
    max("x_down_speed_mbps").alias("max_x_down_speed_mbps"), 
    min('x_line_length_km').alias("min_x_line_length_km"), 
    max('x_line_length_km').alias("max_x_line_length_km"), 
    min('x_up_speed_mbps').alias("min_x_up_speed_mbps"),
    max('x_up_speed_mbps').alias("max_x_up_speed_mbps"), 
    min('stability').alias("min_stability"), 
    max('stability').alias("max_stability"), 
    sum('rag_status_AMBER').alias("rag_status_AMBER"), 
    sum('rag_status_GREEN').alias("rag_status_GREEN"), 
    sum('rag_status_RED').alias("rag_status_RED"), 
    sum('rag_status_UNKNOWN').alias("rag_status_UNKNOWN"))

def process_vip_rewards_data(df): 
	final_Df = df
	status_code = final_Df.select("STATUS").distinct().rdd.flatMap(lambda x: x).collect()
	status_expr = [F.when(F.col("STATUS") == ty, 1).otherwise(0).alias("status_" + ty) for ty in status_code]
	# Frame for source type column 
	type_code = final_Df.select("type_code").distinct().rdd.flatMap(lambda x: x).collect()
	type_code_expr = [F.when(F.col("type_code") == ty, 1).otherwise(0).alias("type_code_" + ty) for ty in type_code]
	df_ohe = final_Df.select("account_number", "subs_week_and_year", *status_expr+type_code_expr) 
	agg_columns = filter(lambda x: x.lower() not in ['account_number','subs_week_and_year'],df_ohe.columns)
	agg_expr = [F.sum(F.col(ty)).alias(ty) for ty in agg_columns]
	return  df_ohe.groupby(['account_number','subs_week_and_year']).agg(*agg_expr)

def process_nps_data(df):
	df1 = df.na.fill('None')
	df2 = df.withColumnRenamed("q1", "nps_score_1").withColumnRenamed("q2","nps_score_2").drop("q2_7")
	floatColumns = filter(lambda x: x not in ("account_number","subs_week_and_year"),df2.columns)
	base_df = df2 
	for val in floatColumns:
		base_df=base_df.withColumn(val, base_df[val].cast(FloatType()))
	df_aggreg_Df = base_df.groupBy(['account_number','subs_week_and_year']).mean()\
	.withColumnRenamed("avg(nps_score_1)", "nps_score_1").withColumnRenamed("avg(nps_score_2)", "nps_score_2")
	return df_aggreg_Df

def process_calls_data(df): 
	intColumns = filter(lambda x: x.lower() not in ['account_number','subs_week_and_year','communication_channel','interaction_direction','interaction_start_dt'],df.columns)
	base_df = df 
	for val in intColumns:
		base_df=base_df.withColumn(val, base_df[val].cast(IntegerType())) 
	communication_channel = base_df.select("communication_channel").distinct().rdd.flatMap(lambda x: x).collect()
	communication_channel_expr = [F.when(F.col("communication_channel") == ty, 1).otherwise(0).alias("communication_channel_" + ty) for ty in communication_channel]
	# Frame for source type column 
	interaction_direction = base_df.select("interaction_direction").distinct().rdd.flatMap(lambda x: x).collect()
	interaction_direction_expr = [F.when(F.col("interaction_direction") == ty, 1).otherwise(0).alias("interaction_direction_" + ty) for ty in interaction_direction]
	df_ohe = base_df.select("*", *communication_channel_expr+interaction_direction_expr) 
	agg_columns = filter(lambda x: x.lower() not in ['account_number','subs_week_and_year','communication_channel','interaction_direction','interaction_start_dt'],df_ohe.columns)
	agg_expr = [F.sum(F.col(ty)).alias(ty) for ty in agg_columns] + [F.mean(F.col("interaction_duration_sec")).alias("mean_conversation_duration")]
	return df_ohe.groupby(['account_number','subs_week_and_year']).agg(*agg_expr)\
	.withColumnRenamed("customer_abandon_flag", "total_customer_abandons")	

def process_pipeline_data(df):
	# Get the column names other than string 
	pipeline_data = df 
	getColumnNames = [col for col in pipeline_data.columns if col not in ['account_number']]
	#agg_cols= [sum(x).alias("sum_"+x) for x in getColumnNames]
	agg_cols = [F.sum(F.col(ty)).alias(ty) for ty in getColumnNames]
	return df.groupBy("account_number").agg(*agg_cols)

def process_downloads_data(df):
	df1 = df.withColumn('genre_desc', lower(col("genre_desc")))
	genre_desc = df1.select("genre_desc").distinct().rdd.flatMap(lambda x: x).collect()
	genre_desc_expr = [F.when(F.col("genre_desc") == ty, 1).otherwise(0).alias("genre_desc_" + str(ty)) for ty in genre_desc]
	revised_Df = df1.select("account_number", "subs_week_and_year","total_downloads",*genre_desc_expr)
	base_df = revised_Df
	downloadColumns = list(filter(lambda x: x not in ['account_number','subs_week_and_year','total_downloads'], base_df.columns)) 	
	for val in downloadColumns:
		base_df=base_df.withColumn(val, base_df[val]*base_df['total_downloads'])
	aggregColumns = list(filter(lambda x: x not in ['account_number','subs_week_and_year','total_downloads'], base_df.columns))
	agg_cols = [F.sum(F.col(ty)).alias(ty) for ty in aggregColumns]
	df_aggreg_Df = base_df.groupBy(['account_number','subs_week_and_year'])\
	.agg(*agg_cols)
	return df_aggreg_Df

def process_offers_data(df):
    left_df = df.select('account_number').distinct()
    subscription_sub_type_array =[\
    'SKY_KIDS',\
    'SPORTS',\
    'HD Pack',\
    'SKY_BOX_SETS',\
    'DTV HD',\
    'MS+',\
    'DTV Extra Subscription',\
    'DTV Primary Viewing',\
    'STANDALONESURCHARGE',\
    'CINEMA',\
    'Broadband DSL Line',\
    'SKY TALK LINE RENTAL',\
    'SKY TALK SELECT',\
    'Sky Go Extra',\
    ]
    for subs in subscription_sub_type_array: 
            df1 = df[df['subs_type']==subs].drop("subs_type").drop("subs_week_and_year").drop_duplicates() 
            ranked_df = df1.withColumn("row_num",row_number().over(Window.partitionBy("account_number").orderBy("Intended_Whole_offer_amount")))\
               .filter(col("row_num") == 1)\
               .drop(col("row_num"))
            colNames  = filter(lambda x: x!='account_number',ranked_df.columns) 
            right_df_renamed = ranked_df 
            concat_string = subs.replace(" ", "_")
            for i in colNames:
                right_df_renamed =right_df_renamed.withColumnRenamed(i, concat_string+"_"+i)
            left_df = left_df.join(right_df_renamed, on=["account_number"], how="left") 
    return left_df

def process_accenture_data(df):
	new_df = df 
	ohe_encode_tuple_lists  = []
	for feature in ['event_type','event_sub_category']:
		ohe_encode_tuple_lists = ohe_encode_tuple_lists +  new_df.select(feature).distinct().rdd.flatMap(lambda x: x).map(lambda x: (feature,x)).collect()
	for feature in list(filter(lambda x:x[1]!=None, ohe_encode_tuple_lists)):
		feature_name = feature[0]
		feature_value = feature[1]
		new_df = new_df.withColumn(feature_name+'_'+feature_value, F.when(F.col(feature_name) == feature_value, 1).otherwise(0))
	l= [col for col in new_df.columns if ('event_type' in col) or ('event_sub_category' in col)] 
	agg_expr = [F.sum(F.col(ty)).alias(ty) for ty in l] + [F.first(F.col('vip_tier_name')).alias('vip_tier_name')] + [F.first(F.col('vip_loyalty_status')).alias('vip_loyalty_status')]
	df2 =  new_df.groupBy(['account_number','subs_week_and_year']).agg(*agg_expr)
	df2.printSchema()
	return df2 

def add_durations_days(df,sd):
	getColumnNames = map(lambda x: x.lower(), df.columns)
	possible_date_columns = ['_dt_actual', '_date', '_modified_dt', 'start_dt']
	featureNames_date  = []
	for val in getColumnNames:
		if any(x in val for x in possible_date_columns):
			print(val)
			featureNames_date.append(val)
			df = df.withColumn(val, to_date(val, "yyyy-MM-dd"))
			timediff= datediff(to_date(val, "yyyy-MM-dd"),((to_date(lit(sd), "yyyy-MM-dd"))))
			#timediff= datediff(val,((to_date(lit(sd), "yyyy-MM-dd"))))
			df = df.withColumn(val+"_duration_days",timediff)
	return df

def upload_data_old(df):
	# Output Parameters.
	start = datetime.now()
	bucket = 'temp_ces'
	output_dataset = 'spark'
	output_table = 'table_hist_5'
	output_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/{}'.format(bucket,output_table)
	output_files = output_directory + '/part-*'
	output_path = spark._jvm.org.apache.hadoop.fs.Path(output_directory)
	output_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(
		output_path, True)
	sql_context = SQLContext(sc)
	#df1 = type_conversion(df)
	(df.write.format('json').save(output_directory, header='true'))
	subprocess.check_call(
		'bq load --source_format NEWLINE_DELIMITED_JSON '
		'{dataset}.{table} {files} ./ldm_schema_1.json'.format(dataset=output_dataset, table=output_table, files=output_files).split())
	print('Uploaded in ',str(datetime.now()-start))

def upload_data(df,dataset_id,table_id):
	# Output Parameters.
	start = datetime.now()
	output_directory = 'gs://pyspark-output/ldm_data'
	output_files = output_directory + '/part-*'
	output_path = spark._jvm.org.apache.hadoop.fs.Path(output_directory)
	output_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(output_path, True)
	df.write.parquet(output_directory)
	subprocess.check_call(
		'bq load --source_format PARQUET '
		'--noreplace '
		'--schema_update_option=ALLOW_FIELD_ADDITION '
		'{dataset}.{table} {files}'.format(dataset=dataset_id, table=table_id, files=output_files).split())
	print('Uploaded in ',str(datetime.now()-start))	

def rename_cols(df):
	for name in df.schema.names:
		pattern = re.compile('[^\w_]')
		renamed =  pattern.sub('',name).lower()
		df=df.withColumnRenamed(name,renamed)
	return df

def check_for_data(project_id,dataset_id,table_id):
	row_count = pd.read_gbq('select count(*) from `'+dataset_id+'.'+table_id+'`',dialect='standard',project_id = project_id)
	return int(row_count.iloc[0,0])

def get_schema_cols(df):
	#cols = pd.read_gbq('select * from `spark.ldm_column_list`',project_id = 'skyuk-uk-prechurn-poc',dialect='standard')
	cols = list(df.columns)
	#cols = list(cols.iloc[:,0])
	dt_cols = [col for col in cols if ('_dt_actual' in col or '_date' in col or '_modified_dt' in col or 'start_dt' in col)]
	cols1 = [col for col in cols if col not in dt_cols]+ [col for col in cols if 'duration_days' in col]
	return cols1
	
def get_schema_json(df):
	import json
	with open('df_schema_1.json', 'w') as outfile:
		json.dump(df.schema.jsonValue(), outfile)

def generate_ldm(sd,yw):
	start = datetime.now()
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','line_speed_query') > 0:
		line_speed_data = fetch_data('line_speed_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','vip_claim_query') > 0:
		vip_claim_data = fetch_data('vip_claim_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','nps_query') > 0:
		nps_data = fetch_data('nps_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','calls_query') > 0:
		calls_data = fetch_data('calls_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','pipeline_query') > 0:
		pipeline_data = fetch_data('pipeline_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','downloads_query') > 0:
		downloads_data = fetch_data('downloads_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','vip_eligibility_query') > 0:
		vip_eligibility_data = fetch_data('vip_eligibility_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','billing_query') > 0:
		billing_data = fetch_data('billing_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','cwb_query') > 0:
		cwb_data = fetch_data('cwb_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','offers_query') > 0:
		offers_data = fetch_data('offers_query')
		print('fetched data 1')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','stb_query') > 0:
		stb_data = fetch_data('stb_query')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','accenture_query') > 0:
		accenture_data = fetch_data('accenture_query')
	print('fetched in ',str(datetime.now()-start))
	start = datetime.now()
	out1 = cwb_data
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','line_speed_query') > 0:
		out2 = process_line_speed_data(line_speed_data)
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','vip_claim_query') > 0:
		out3 = process_vip_rewards_data(vip_claim_data)
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','nps_query') > 0:
		out4 = process_nps_data(nps_data)
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','calls_query') > 0:
		out5 = process_calls_data(calls_data)
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','pipeline_query') > 0:
		out6 = process_pipeline_data(pipeline_data)
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','downloads_query') > 0:
		out7 = process_downloads_data(downloads_data)
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','vip_eligibility_query') > 0:
		out8 = vip_eligibility_data
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','billing_query') > 0:
		out9 = billing_data
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','offers_query') > 0:
		out10 = process_offers_data(offers_data)
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','stb_query') > 0:
		out11 = stb_data
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','accenture_query') > 0:
		out12 = process_accenture_data(accenture_data)
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','line_speed_query') > 0:
		out = out1.join(out2, on=['account_number',  'subs_week_and_year'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','vip_claim_query') > 0:
		out = out.join(out3, on=['account_number',  'subs_week_and_year'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','nps_query') > 0:
		out = out.join(out4, on=['account_number',  'subs_week_and_year'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','calls_query') > 0:
		out = out.join(out5, on=['account_number',  'subs_week_and_year'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','pipeline_query') > 0:
		out = out.join(out6, on=['account_number'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','downloads_query') > 0:
		out = out.join(out7, on=['account_number',  'subs_week_and_year'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','vip_eligibility_query') > 0:
		out = out.join(out8, on=['account_number'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','billing_query') > 0:
		out = out.join(out9, on=['account_number',  'subs_week_and_year'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','offers_query') > 0:
		out = out.join(out10, on=['account_number'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','stb_query') > 0:
		out = out.join(out11, on=['account_number',  'subs_week_and_year'], how='left').filter(col('account_number')!='?')
	if check_for_data('skyuk-uk-prechurn-poc','deltas_1','accenture_query') > 0:
		out = out.join(out12, on=['account_number',  'subs_week_and_year'], how='left').filter(col('account_number')!='?')
	out = rename_cols(out).filter(col('account_number')!='?')
	out = add_durations_days(out,sd)
	print('processed in ',str(datetime.now()-start))
	print(out.printSchema())
	return out

def get_runseqs():
	sky_calendar = pd.read_gbq('select end_date,subs_week_and_year from `generated.snapshot_calendar`',project_id = 'skyuk-uk-prechurn-poc', dialect = 'standard')
	sky_calendar = sky_calendar[(sky_calendar.subs_week_and_year >= 201639) & (sky_calendar.subs_week_and_year <= 201728)]
	sky_calendar.sort_values(by = 'subs_week_and_year',inplace = True)
	sky_calendar['end_date'] = sky_calendar['end_date'].astype(str)
	sky_calendar['subs_week_and_year'] = sky_calendar['subs_week_and_year'].astype(str)
	return sky_calendar.values


def read_gbq_schema(table_name):
	#read schema from big query
	client = bigquery.Client(project='skyuk-uk-prechurn-poc')
	dataset_id = 'spark'
	dataset_ref = client.dataset(dataset_id)
	dataset = client.get_dataset(dataset_ref)
	table_ref = dataset.table(table_name)
	table = client.get_table(table_ref)
	field_names = [field.name for field in table.schema]
	field_types = [field.field_type for field in table.schema]
	table_schema = pd.DataFrame({'columns':field_names,'data_types':field_types})
	return table_schema

def delete_bigquery_table(dataset_id,table_id):
	client = bigquery.Client()
	table_ref = client.dataset(dataset_id).table(table_id)
	client.delete_table(table_ref)



def discrete_feature_didq(discrete_columns, subs_week_year):
	project_id = 'skyuk-uk-prechurn-poc'
	temp = []
	for i in discrete_columns:
		min_query = 'select distinct('+i+') ' +' from `spark.table_hist_14`' + 'where subs_week_and_year=' + str(subs_week_year)
		print('discrete_featuure' +i)
		project_id = 'skyuk-uk-prechurn-poc'
		categories = pd.read_gbq(min_query,project_id,dialect = 'standard')
		temp = temp + [(i, int(subs_week_year), list(categories.iloc[:,0]), len(list(categories.iloc[:,0])), return_date_value(datetime.now()))]
	df = pd.DataFrame(temp, columns=['colname', 'subs_week_and_year',  'unique_values','number_of_categories', 'created_dt'])
	df.to_gbq('spark.didq_discrete_columns', project_id,if_exists='append')


def continuous_feature_didq(continuous_columns, subs_week_year):
	project_id = 'skyuk-uk-prechurn-poc'
	for  i in continuous_columns:
		min_query = 'select subs_week_and_year, '  +  'max('+i+'), '+  'min('+i+'), ' +  'avg('+i+'), ' + \
		'stddev('+i+')'  +' from `spark.table_hist_14`'+'\
		where subs_week_and_year = ' + str(subs_week_year) +'\
		 group by subs_week_and_year'
		categories = pd.read_gbq(min_query,project_id,dialect = 'standard')
		categories['colname'] = i 
		categories['created_dt_check'] = return_date_value(datetime.now())
		categories.columns = ['subs_week_and_year', 'maximum_value', 'minimum_value', 'mean_value', 'standard_deviation', 'colname', 'created_dt_check']
		temp = categories[['colname', 'subs_week_and_year', 'maximum_value', 'minimum_value', 'mean_value', 'standard_deviation','created_dt_check']]
		temp.maximum_value = temp.maximum_value.astype(float)
		temp.minimum_value = temp.minimum_value.astype(float)
		temp.to_gbq('spark.didq_continuous_columns',project_id,if_exists='append')



def return_date_value(start_time):
	old_stdout = sys.stdout
	result = StringIO()
	sys.stdout = result
	print(start_time) 
	sys.stdout = old_stdout
	return  result.getvalue().rstrip()


def check_whether_table_exists(project_id, schema_name, log_table_name): 
	query = 'SELECT table_id FROM'+ '`'+ project_id+'.'+schema_name+'.__TABLES_SUMMARY__`'
	df  = pd.read_gbq(query, project_id,dialect = 'standard')
	return log_table_name in  list(df['table_id'])




def compute_logs(start_time, end_time,yw):
	end_diff_time = end_time - start_time
	temp = []
	temp = temp + [(int(yw), str(return_date_value(start_time)), str(return_date_value(end_time)), str(return_date_value(end_diff_time)), "customer_360",ldm_output_table_name )]
	df = pd.DataFrame(temp, columns=['subs_week_and_year', 'start_time',  'end_time','time_difference', 'process_name','ldm_table_name'])
	df.to_gbq('spark.project_run_logs', project_id,if_exists='append')



def start_ldm(run_seq_start, ldm_output_schema, ldm_output_table): 
	start = datetime.now()
	#runseq_list = get_runseqs()
	#schema_name = 'olive'
	#table_name = 'cust_weekly_base'
	#start_date = pd.to_datetime(pd.read_gbq('select max(end_date) ' +'from' + '`'+schema_name+'.'+table_name+'` '+'where subs_week_and_year='+str(run_seq_start),project_id,dialect = 'standard').iloc[0,0]).date() 
	start_date = pd.read_gbq('select max(end_date) ' +'from' + '`'+get_schema_table_dict['cwb_query']+'` '+'where subs_week_and_year='+str(run_seq_start),project_id,dialect = 'standard').iloc[0,0] 
	date_temp = pd.to_datetime(start_date).date()
	sd = return_date_value(date_temp)
	yw = '\''+str(run_seq_start)+'\''
	#for idx,(sd,yw) in enumerate(runseq_list):
	start_time = datetime.now() 
	print(sd,yw)
	delete_directory()
	generate_deltas(sd,yw)
	out = generate_ldm(sd,yw)
	upload_data(out,ldm_output_schema, ldm_output_table)
	end_time = datetime.now()
	yw = yw[1:-1]
	compute_logs(start_time, end_time, int(yw))
	#df = read_gbq_schema(ldm_output_table)
	#tuples = [tuple(x) for x in df.values]
	#discrete_columns   = list(map(lambda x:x[0], filter(lambda x:(x[1]=='STRING') & (x[0]!='account_number') & (x[0]!='dtv_1st_activation_dt') , tuples)))
	#continuous_columns = list(map(lambda x:x[0], filter(lambda x:((x[1]=='FLOAT') | (x[1] =='INTEGER')) & (x[0]!='subs_week_and_year'), tuples)))
	#discrete_feature_didq(discrete_columns, yw)
	#continuous_feature_didq(continuous_columns, yw)
	del out
	#print('Code ran in '+str(datetime.now()-start))

# Function to trigger the LDM Job 

def trigger_ldm_job_old(run_seq_start, schema_name, table_name, ldm_output_schema, ldm_output_table):
	if(check_whether_table_exists('skyuk-uk-prechurn-poc', schema_name, table_name)==True): 
		max_subs_week_and_year = pd.read_gbq('select max(subs_week_and_year) ' +'from' + '`'+schema_name+'.'+table_name+'` '+'where process_name="customer_360" and ldm_table_name="' + ldm_output_table_name+'"',project_id,dialect = 'standard').iloc[0,0]
		if run_seq_start <= max_subs_week_and_year:
			print('For this given sequence, the LDM is already prepared')
		elif (run_seq_start  >  max_subs_week_and_year):
			start_ldm(str(run_seq_start), ldm_output_schema,ldm_output_table)		
		else: 
			print('Wrong inputs given')
	else:	
		start_time = datetime.now()
		end_time = datetime.now()
		end_diff_time = end_time - start_time 
		temp = [(-1, return_date_value(start_time), return_date_value(end_time), return_date_value(end_diff_time), "customer_360" )]
		df = pd.DataFrame(temp, columns=['subs_week_and_year', 'start_time',  'end_time','time_difference', 'process_name'])
		df.to_gbq(str(schema_name) + '.' + str(table_name) , project_id,if_exists='append')


def trigger_ldm_job(run_seq_start, schema_name, table_name, ldm_output_schema, ldm_output_table):
	if(check_whether_table_exists('skyuk-uk-prechurn-poc', schema_name, table_name)==True): 
		query = 'select subs_week_and_year ' +'from ' + '`'+schema_name+'.'+\
		table_name+'` '+'where process_name="customer_360" and subs_week_and_year='+ str(run_seq_start) +' and ldm_table_name="'+ldm_output_table_name+'"'
		if pd.read_gbq(query,project_id,dialect = 'standard').shape[0]==0:
			start_ldm(str(run_seq_start), ldm_output_schema,ldm_output_table)
		else:
			print('LDM is already prepared')
	else:	
		start_time = datetime.now()
		end_time = datetime.now()
		end_diff_time = end_time - start_time 
		temp = [(-1, return_date_value(start_time), return_date_value(end_time), return_date_value(end_diff_time), "customer_360", ldm_output_table_name )]
		df = pd.DataFrame(temp, columns=['subs_week_and_year', 'start_time',  'end_time','time_difference', 'process_name','ldm_table_name'])
		df.to_gbq(str(schema_name) + '.' + str(table_name) , project_id,if_exists='append')





if __name__=='__main__':
	start = datetime.now()
	#df = pd.read_csv('project_id.csv')
	df = pd.read_csv('project_id.csv','\t')
	get_schema_table_dict = dict(zip(df.column_name, df.project_id))
	import argparse
	parser = argparse.ArgumentParser(description = 'test')
	parser.add_argument('--yearweek',help = 'yearweek for which code is run')
	parser.add_argument('--output_schema_name',help = 'The target database')
	parser.add_argument('--output_table_name',help = 'The target table')
	args = parser.parse_args()
	project_id='skyuk-uk-prechurn-poc'
	print('runseq ',args.yearweek)
	run_seq_start = args.yearweek
	ldm_output_schema_name = args.output_schema_name # Schema 
	ldm_output_table_name = args.output_table_name
	trigger_ldm_job(run_seq_start, ldm_output_schema_name, 'fractal_customer_360_logs', ldm_output_schema_name, ldm_output_table_name)


# Project Name, schema, table 



	



