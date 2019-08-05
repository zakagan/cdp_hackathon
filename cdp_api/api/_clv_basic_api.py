import json
import time
import boto3
import os
from google.cloud import bigquery
from flask import Flask, Response, make_response, jsonify, request, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

JSON_MIME_TYPE = 'application/json'
DB_ENV = os.environ['DB_ENV']


@app.route('/checks3')
def check_s3():
        # Get the account number so we can determine the name of the queryresults bucket
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    s3_output_location = 's3://aws-athena-query-results-{}-us-east-1/'.format(account_id)

    # Make the Athena call
    athena = boto3.client('athena')
    with open('sql_query.txt', 'r') as queryfile:
        sql_query = queryfile.read()
    response = athena.start_query_execution(sql_query, ResultConfiguration={'OutputLocation': s3_output_location})

    QueryString = 

    # Wait for the query to complete
    response = athena.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']
    while response['Status']['State'] == 'RUNNING':
        print('Waiting for query to complete . . .')
        time.sleep(4)
        response = athena.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']
    if response['Status']['State'] == 'SUCCEEDED':

        # Paginators are easier to use when returning large amounts of data
        paginator = athena.get_paginator('get_query_results')
        for results in paginator.paginate(QueryExecutionId=response['QueryExecutionId']):
            return make_response(jsonify(results['ResultSet']), 200, {'Content-Type': JSON_MIME_TYPE})
    else:
        return make_response(jsonify('Query execution failed: %s' % response), 500, {'Content-Type': JSON_MIME_TYPE})

@app.route('/', methods=['GET'])
def api_usage():
    usage_dict = {"data" :
                    {"usage": 
                        {"/account_clv/<company_id>": "provide company id to recieve associated CLV data",
                        "/company_id_map/" : "lists companies in the database and their company ids for lookup "}
                    }
        }

    return make_response(jsonify(usage_dict), 200, {'Content-Type': JSON_MIME_TYPE})

@app.route('/account_clv/<company_id>', methods=['GET'])
def account_clv_lookup(company_id):
    client = bigquery.Client()
    content = request.json

    query_job = client.query("""SELECT ps_total_lifetime_rev FROM `{}.PIB_CLV.all_accounts_clv` where company_sfdc_id = \"{}\"""".format(DB_ENV, company_id))

    results = query_job.result()  # Waits for job to complete.

    return_clv = 0
    for i in results:
        return_clv += i.get("ps_total_lifetime_rev")

    return_dict = {"data": {"result_lifetime_rev": return_clv}}
    return make_response(jsonify(return_dict), 200, {'Content-Type': JSON_MIME_TYPE})

@app.route('/company_id_map/', methods=['GET'])
def company_id_lookup():
    client = bigquery.Client()
    content = request.json

    query_job = client.query("""SELECT company_name, company_sfdc_id FROM `{}.PIB_CLV.all_accounts_clv`""".format(DB_ENV))

    results = query_job.result()  # Waits for job to complete.

    company_sfdc_id ={}
    for i in results:
        company_sfdc_id.update({i.get("company_name") : i.get("company_sfdc_id")})

    return_dict = {"data": {"company_id_map": company_sfdc_id}}
    return make_response(jsonify(return_dict), 200, {'Content-Type': JSON_MIME_TYPE})



@app.errorhandler(404)
def not_found(e):
    return '', 404
