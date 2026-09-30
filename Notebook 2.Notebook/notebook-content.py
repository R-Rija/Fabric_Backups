# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ca779aa0-014f-4240-8027-2ae2cf30876d",
# META       "default_lakehouse_name": "Loyalty_Customer",
# META       "default_lakehouse_workspace_id": "b4a3c763-a8cb-4108-bf6f-379159cdaf1c",
# META       "known_lakehouses": [
# META         {
# META           "id": "ca779aa0-014f-4240-8027-2ae2cf30876d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO retail_loyalty_engagement_table(
# MAGIC     ACCOUNTING_PERIOD_NUMBER,
# MAGIC     ACCOUNTING_YEAR,
# MAGIC     PERIOD_END_DATE,
# MAGIC     CUSTOMER_ID,
# MAGIC     EMAIL_ADDRESS,
# MAGIC     STATE,
# MAGIC     CONSENT_TYPE,
# MAGIC     BRAND_CARD_FLAG,
# MAGIC     LOYALTY_PROGRAM_FLAG,
# MAGIC     PARTNER_PROGRAM_FLAG,
# MAGIC     PROGRAM_LINKING_STATUS,
# MAGIC     BRAND_LOYALTY_LINKAGE,
# MAGIC     CUSTOMER_STATUS,
# MAGIC     LOYALTY_JOINING_DATE,
# MAGIC     PARTNER_JOIN_DATE,
# MAGIC     RFM_HIGH_SEGMENT,
# MAGIC     RFM_LOW_SEGMENT,
# MAGIC     ONLINE_TXN_CNT,
# MAGIC     INSTORE_TXN_CNT,
# MAGIC     ONLINE_UNITS,
# MAGIC     INSTORE_UNITS,
# MAGIC     ONLINE_SALES,
# MAGIC     INSTORE_SALES,
# MAGIC     TOTAL_SALES,
# MAGIC     TOTAL_UNITS,
# MAGIC     TOTAL_TXN,
# MAGIC     SPEND_12M,
# MAGIC     UNITS_12M,
# MAGIC     TXN_12M
# MAGIC )
# MAGIC VALUES (
# MAGIC     202609,
# MAGIC     2026,
# MAGIC     '30-09-2026',
# MAGIC     999999,
# MAGIC     'testcustomer999999@example.com',
# MAGIC     'NSW',
# MAGIC     'Email',
# MAGIC     1,
# MAGIC     1,
# MAGIC     0,
# MAGIC     'Linked',
# MAGIC     'Linked',
# MAGIC     'Active',
# MAGIC     '15-01-2026',
# MAGIC     NULL,
# MAGIC     'Active',
# MAGIC     'High Value',
# MAGIC     5,
# MAGIC     4,
# MAGIC     8,
# MAGIC     7,
# MAGIC     125.50,
# MAGIC     98.75,
# MAGIC     224.25,
# MAGIC     15,
# MAGIC     9,
# MAGIC     224.25,
# MAGIC     15,
# MAGIC     9
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
