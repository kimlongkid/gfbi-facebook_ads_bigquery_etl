# Facebook ads to google bigQuery ETL

_ETL based on cloud function (google cloud) that retrieves data from the Facebook Insight API:_
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

ETL uploads this data to Google Bigquery every day.

## Getting Started

These instructions will get you a copy of the project up and running on your google cloud project for development and testing purposes.

- Secrets are loaded using the google cloud secrets API
  FACEBOOK_APP_ID
  FACEBOOK_APP_SECRET
  FACEBOOK_APP_TOKEN

### Installing

Create scheduler for facebook ads ETL:

- Parameters can be passed as JSON String
  project*id: your google cloud project_id
  dataset_id: for example "facebook"
  table_id: for example "fb_stat"
  fb_account_id: your facebook account id without act*
  since: start date - defaults to yesterday
  until: end date - defaults to yesterday

#### Example

```JSON
{
    "project_id": "my_project",
    "dataset_id": "fb_data",
    "table_id": "ac_name",
    "fb_account_id": "number",
    "since": "2020-01-01",
    "until": "2020-02-01"
}
```

## Authors

- **Andrey Osipov** Original Author
- Philipp Rosenbeck

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
