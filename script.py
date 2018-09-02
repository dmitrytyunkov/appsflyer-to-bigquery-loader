import argparse
import os
import pandas as pd
import datetime
import json

schema = [{'name': 'id', 'type': 'INTEGER'}, {'name': 'date', 'type': 'DATETIME'},
          {'name': 'from', 'type': 'DATE'}, {'name': 'to', 'type': 'DATE'},
          {'name': 'count_rows', 'type': 'INTEGER'}, {'name': 'table', 'type': 'STRING'}]

def read_table_from_appsflyer(api_token, app_id, table, start_date, end_date):
    url = 'https://hq.appsflyer.com/export/{3}/{4}/v5?api_token={2}&from={0}&to={1}'.format(start_date, end_date,
                                                                                            api_token, app_id, table)
    df = pd.read_csv(url)
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.replace('(', '')
    df.columns = df.columns.str.replace(')', '')
    df.columns = df.columns.str.replace(' ', '_')
    return df

def write_table_into_bigquery(project_id, dataset_id, table, df, res_df, oauth_key):
    df.to_gbq(destination_table='{0}.{1}'.format(dataset_id, table), project_id=project_id,
                               if_exists='append', private_key=oauth_key)
    res_df.to_gbq(destination_table='{0}.import_appsflyer_log'.format(dataset_id), project_id=project_id, if_exists='append',
                     private_key=oauth_key, table_schema=schema)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--config',
                        dest='config',
                        required=True,
                        help='Configuration file with bigquery project settings, appsflyer project settings and '
                             'frequency upload data.')
    parser.add_argument('--oauth_file',
                        dest='oauth_file',
                        required=True,
                        help='File to authorize bigquory.')
    known_args, not_know_args = parser.parse_known_args(argv)

    OAUTH_PATH = known_args.oauth_file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = OAUTH_PATH
    oauth_file = open(OAUTH_PATH, 'r')
    oauth_key = oauth_file.read()
    data = {}
    if os.path.isfile(known_args.config):
        with open(known_args.config) as file:
            data = json.load(file)
    else:
        print 'Missing configuration file (config.json)'
        return

    app_id = data['appsflyer_app_id']
    api_token = data['appsflyer_api_token']
    project_id = data['bigquery_project_id']
    dataset_id = data['bigquery_dataset_id']
    tables = data['tables']
    min_days_period = data['min_days_period']

    for table in tables:
        full_loaded = pd.read_gbq(query='SELECT * FROM `{0}.{1}.import_appsflyer_log`'.format(project_id, dataset_id, table),
                                  private_key=oauth_key, dialect='standard')
        loaded = pd.read_gbq(query='SELECT * FROM `{0}.{1}.import_appsflyer_log` WHERE table=\'{2}\''.format(project_id, dataset_id, table),
                             private_key=oauth_key, dialect='standard')

        if loaded.empty:
            id = 0
            if full_loaded.empty:
                id = 1
            else:
                full_loaded = full_loaded.sort_values(by=['id'])
                id = full_loaded.at[full_loaded.index[-1], 'id'] + 1

            run_date = datetime.datetime.now()
            from_date = datetime.date.today() - datetime.timedelta(days=30)
            to_date = datetime.date.today() - datetime.timedelta(days=1)

            partners_by_date_df = read_table_from_appsflyer(app_id=app_id, api_token=api_token, table=table,
                                                            start_date=from_date.strftime('%Y-%m-%d'),
                                                            end_date=to_date.strftime('%Y-%m-%d'))
            print "Table: {0} was downloaded from appsflyer. Start date: {1}, end date: {2}".format(table,
                                                                                           from_date.strftime(
                                                                                               '%Y-%m-%d'),
                                                                                           to_date.strftime('%Y-%m-%d'))
            count_rows = partners_by_date_df.shape[0]
            loaded_df = pd.DataFrame(data=[[id, run_date, from_date, to_date, count_rows, table]], columns=["id", "date", "from",
                                                                                                     "to", "count_rows", "table"])
            write_table_into_bigquery(project_id=project_id, dataset_id=dataset_id, table=table, df=partners_by_date_df,
                                      res_df=loaded_df, oauth_key=oauth_key)

            print "Table: {0} was uploaded to bigquery".format(table)
        else:
            full_loaded = full_loaded.sort_values(by=['id'])
            loaded = loaded.sort_values(by=['id'])
            sub_dates = datetime.datetime.now() - loaded.at[loaded.index[-1], 'date']
            if sub_dates.days < min_days_period:
                print('Since the last download, less than {0} days have passed'.format(min_days_period))
            else:
                id = full_loaded.at[full_loaded.index[-1], 'id'] + 1
                run_date = datetime.datetime.now()
                from_date = loaded.at[loaded.index[-1], 'to']
                to_date = datetime.date.today() - datetime.timedelta(days=1)

                partners_by_date_df = read_table_from_appsflyer(app_id=app_id, api_token=api_token, table=table,
                                                                start_date=from_date.strftime('%Y-%m-%d'),
                                                                end_date=to_date.strftime('%Y-%m-%d'))
                print "Table: {0} was downloaded from appsflyer. Start date: {1}, end date: {2}".format(table,
                                                                                               from_date.strftime(
                                                                                                   '%Y-%m-%d'),
                                                                                               to_date.strftime(
                                                                                                   '%Y-%m-%d'))
                count_rows = partners_by_date_df.shape[0]
                loaded_df = pd.DataFrame(data=[[id, run_date, from_date, to_date, count_rows, table]], columns=["id", "date",
                                                                                                         "from", "to",
                                                                                                         "count_rows", "table"])
                write_table_into_bigquery(project_id=project_id, dataset_id=dataset_id, table=table, df=partners_by_date_df,
                                          res_df=loaded_df, oauth_key=oauth_key)
                print "Table: {0} was uploaded to bigquery".format(table)

main()