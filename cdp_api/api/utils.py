from flask import make_response, jsonify
import time
import boto3

JSON_MIME_TYPE = 'application/json'

books = [{
    'id': 33,
    'title': 'The Raven',
    'author_id': 1
}]


def search_book(books, book_id):
    for book in books:
        if book['id'] == book_id:
            return book


def json_response(data='', status=200, headers=None):
    headers = headers or {}
    if 'Content-Type' not in headers:
        headers['Content-Type'] = JSON_MIME_TYPE

    return make_response(data, status, headers)


# def athena_test():
#     # Get the account number so we can determine the name of the queryresults bucket
#     sts = boto3.client('sts')
#     account_id = sts.get_caller_identity()['Account']
#     s3_output_location = 's3://aws-athena-query-results-{}-us-east-1/'.format(account_id)

#     # Make the Athena call
#     athena = boto3.client('athena')
#     response = athena.start_query_execution(
#         QueryString = "select * from hackathon.omniture_fact limit 5",
#         ResultConfiguration={'OutputLocation': s3_output_location}
#     )

#     # Wait for the query to complete
#     response = athena.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']
#     while response['Status']['State'] == 'RUNNING':
#         print('Waiting for query to complete . . .')
#         time.sleep(4)
#         response = athena.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']

#     if response['Status']['State'] == 'SUCCEEDED':
#         print('Printing the results')

#         # Paginators are easier to use when returning large amounts of data
#         paginator = athena.get_paginator('get_query_results')
#         for results in paginator.paginate(QueryExecutionId=response['QueryExecutionId']):
#             for row in results['ResultSet']['Rows']:
#                 print([x.get('VarCharValue') for x in row['Data']])
#     else:
#         print('Query execution failed: %s' % response)