import base64
import json
import logging
from datetime import date, datetime, timedelta
import requests
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
from google.cloud import bigquery
from google.cloud import secretmanager_v1beta1 as secretmanager
from google.cloud.exceptions import NotFound

logger = logging.getLogger()
#TODO:
# Change interface to http requests ? use JSON Strings for now
# Improve logging for errors and exceptions - Enable cloud logging


schema_facebook_stat = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ad_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ad_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("adset_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("adset_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField('conversions', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'STRING'))),
    bigquery.SchemaField('actions', 'RECORD', mode='REPEATED',
        fields=(bigquery.SchemaField('action_type', 'STRING'),
                bigquery.SchemaField('value', 'STRING')))

]

clustering_fields_facebook = ['campaign_id', 'campaign_name']

def get_secret(project_id, secret_id):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode('UTF-8')
    return payload

def exist_dataset_table(client, table_id, dataset_id, project_id, schema, clustering_fields=None):

    try:
        dataset_ref = f"{project_id}.{dataset_id}"
        client.get_dataset(dataset_ref)  # Make an API request.

    except NotFound:
        dataset_ref = f"{project_id}.{dataset_id}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        dataset = client.create_dataset(dataset)  # Make an API request.
        logger.info(f"Created dataset {client.project}.{dataset.dataset_id}")

    try:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        client.get_table(table_ref)  # Make an API request.

    except NotFound:

        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        table = bigquery.Table(table_ref, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )

        if clustering_fields is not None:
            table.clustering_fields = clustering_fields

        table = client.create_table(table)  # Make an API request.
        logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    return 'ok'


def insert_rows_bq(client, table_id, dataset_id, project_id, data):

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = client.get_table(table_ref)
    if data:
        resp = client.insert_rows_json(
            json_rows = data,
            table = table_ref,
        )
        logger.info(f"Success uploaded to table {table.table_id}")


def get_facebook_data(attributes, since, until, bigquery_client):
        table_id = attributes['table_id']
        dataset_id = attributes['dataset_id']
        project_id = attributes['project_id']
        app_id = get_secret(project_id, "FACEBOOK_APP_ID")
        app_secret = get_secret(project_id, "FACEBOOK_APP_SECRET")
        access_token = get_secret(project_id, "FACEBOOK_ACCESS_TOKEN")
        account_id = attributes['fb_account_id']

        try:
            FacebookAdsApi.init(app_id, app_secret, access_token)

            account = AdAccount('act_'+str(account_id))
            insights = account.get_insights(fields=[
                    AdsInsights.Field.account_id,
                    AdsInsights.Field.campaign_id,
                    AdsInsights.Field.campaign_name,
                    AdsInsights.Field.adset_name,
                    AdsInsights.Field.adset_id,
                    AdsInsights.Field.ad_name,
                    AdsInsights.Field.ad_id,
                    AdsInsights.Field.spend,
                    AdsInsights.Field.impressions,
                    AdsInsights.Field.clicks,
                    AdsInsights.Field.actions,
                    AdsInsights.Field.conversions
            ], params={
                'level': 'ad',
                'time_range': {
                    'since': since.strftime("%Y-%m-%d"),
                    'until': until.strftime("%Y-%m-%d")
                },'time_increment': 1
            })

        except Exception as e:
                logger.info(e)
                print(e)
                raise

        fb_source = []

        for index, item in enumerate(insights):

            actions = []
            conversions = []

            if 'actions' in item:
                for i, value in enumerate(item['actions']):
                    actions.append({'action_type' : value['action_type'], 'value': value['value']})

            if 'conversions' in item:
                for i, value in enumerate(item['conversions']):
                    conversions.append({'action_type' : value['action_type'], 'value': value['value']})

            fb_source.append({'date': item['date_start'],
                               'ad_id' : item['ad_id'],
                               'ad_name' : item['ad_name'],
                               'adset_id' : item['adset_id'],
                               'adset_name' : item['adset_name'],
                               'campaign_id' : item['campaign_id'],
                               'campaign_name' : item['campaign_name'],
                               'clicks' : item['clicks'],
                               'impressions' : item['impressions'],
                               'spend' : item['spend'] ,
                               'conversions' : conversions,
                               'actions' : actions
                            })

        if exist_dataset_table(bigquery_client, table_id, dataset_id, project_id, schema_facebook_stat, clustering_fields_facebook) == 'ok':

            insert_rows_bq(bigquery_client, table_id, dataset_id, project_id, fb_source)

            return 'ok'


def process_request(event, context):
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    except:
        pubsub_message = event['data'].decode('utf-8')

    attributes = json.loads(pubsub_message)

    logger.info(attributes)

    bigquery_client = bigquery.Client()

    if 'since' in attributes:
        since = attributes['since'].strftime('%Y-%m-%d')
    else:
        since = date.today() - timedelta(1)
    if 'until' in attributes:
        until = attributes['until'].strftime('%Y-%m-%d')
    else:
        until = date.today() - timedelta(1)

    get_facebook_data(attributes, since, until, bigquery_client)