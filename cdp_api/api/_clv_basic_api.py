import json
import time
import boto3
from google.cloud import bigquery
from flask import Flask, Response, make_response, jsonify, request, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

JSON_MIME_TYPE = 'application/json'


@app.route('/checks3')
def check_s3():
        # Get the account number so we can determine the name of the queryresults bucket
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    s3_output_location = 's3://aws-athena-query-results-{}-us-east-1/'.format(account_id)

    # Make the Athena call
    athena = boto3.client('athena')
    response = athena.start_query_execution(
    QueryString = """ SELECT bad.cibs_billing_account_id,
         bad.industry,
         base_iii.*
        FROM 
        (SELECT comp.company_name,
         base_ii.*
        FROM 
            (SELECT ahf.company_sfdc_id,
         base.*
            FROM 
                (SELECT opp.opportunity_name,
         opp.sale_type,
         opp.sales_stage,
         opp.created_date,
         opp.closed_date,
         opp.opportunity_effective_date,
         opp.compelling_reasons_to_buy,
         opp.loss_reason_code,
         opp.opportunity_country,
         opp.fiscal_year,
         opp.fiscal_quarter,
         opp.pib_opportunity_sfdc_id,
         opp.billing_account,
         psd.forecast_month,
         psd.product_line,
         psd.product_service,
         psd.product_category,
         psd.net_recurring_amount,
         psd.net_one_time_amount,
         psd.currency_iso_code
                FROM 
                    (SELECT opportunity_name,
         billing_account,
         sale_type,
         sales_stage,
         annualized_opportunity_revenue,
         created_date,
         closed_date,
         opportunity_effective_date,
         compelling_reasons_to_buy,
         loss_reason_code,
         opportunity_country,
         fiscal_year,
         fiscal_quarter,
         pib_opportunity_sfdc_id
                    FROM hackathon.pib_opportunity_dimension
                    WHERE status_flag <> 'DELETE'
                    GROUP BY  opportunity_name, billing_account, sale_type, sales_stage, annualized_opportunity_revenue, created_date, closed_date, opportunity_effective_date, compelling_reasons_to_buy, loss_reason_code, opportunity_country, fiscal_year, fiscal_quarter, pib_opportunity_sfdc_id ) opp
                    LEFT JOIN 
                        (SELECT forecast_month,
         product_line,
         product_service,
         product_category,
         net_recurring_amount,
         net_one_time_amount,
         currency_iso_code, opportunity
                        FROM hackathon.pib_product_services_dimension
                        WHERE status_flag <> 'DELETE'
                        GROUP BY  forecast_month, product_line, product_service, product_category, net_recurring_amount, net_one_time_amount, currency_iso_code, opportunity) psd
                            ON psd.opportunity = opp.pib_opportunity_sfdc_id) base
                        LEFT JOIN 
                            (SELECT *
                            FROM hackathon.pib_account_hierarchy_fact) ahf
                                ON base.pib_opportunity_sfdc_id = ahf.pib_opportunity_sfdc_id) base_ii
                            LEFT JOIN 
                                (SELECT company_sfdc_id,
         company_name
                                FROM hackathon.pib_company_dimension
                                WHERE status_flag <> 'DELETE'
                                GROUP BY  company_sfdc_id, company_name) comp
                                    ON comp.company_sfdc_id = base_ii.company_sfdc_id) base_iii
                                LEFT JOIN 
                                    (SELECT cibs_billing_account_id,
         industry,
         contract_id,
         salesforce_billing_account_id
                                    FROM hackathon.pib_billing_account_dimension
                                    GROUP BY  cibs_billing_account_id, industry, contract_id, salesforce_billing_account_id) bad
                                        ON bad.salesforce_billing_account_id = base_iii.billing_account
                                    WHERE sales_stage IN ('Closed (Win/Loss)','Won - 100%','Lost Revenue - 100%')
    GROUP BY bad.cibs_billing_account_id,
         bad.industry, base_iii.company_name, base_iii.company_sfdc_id, base_iii.opportunity_name, base_iii.sale_type, base_iii.sales_stage, base_iii.created_date, base_iii.closed_date, base_iii.opportunity_effective_date, base_iii.compelling_reasons_to_buy, base_iii.loss_reason_code, base_iii.opportunity_country, base_iii.fiscal_year, base_iii.fiscal_quarter, base_iii.pib_opportunity_sfdc_id, base_iii.billing_account, base_iii.forecast_month, base_iii.product_line, base_iii.product_service, base_iii.product_category, base_iii.net_recurring_amount, base_iii.net_one_time_amount, base_iii.currency_iso_code""",
        ResultConfiguration={'OutputLocation': s3_output_location}
    )

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

    query_job = client.query("""SELECT ps_total_lifetime_rev FROM `djsyndicationhub-stag.PIB_CLV.all_accounts_clv` where company_sfdc_id = \"{}\"""".format(company_id))

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

    query_job = client.query("""SELECT company_name, company_sfdc_id FROM `djsyndicationhub-stag.PIB_CLV.all_accounts_clv`""")

    results = query_job.result()  # Waits for job to complete.

    company_sfdc_id ={}
    for i in results:
        company_sfdc_id.update({i.get("company_name") : i.get("company_sfdc_id")})

    return_dict = {"data": {"company_id_map": company_sfdc_id}}
    return make_response(jsonify(return_dict), 200, {'Content-Type': JSON_MIME_TYPE})



@app.errorhandler(404)
def not_found(e):
    return '', 404
