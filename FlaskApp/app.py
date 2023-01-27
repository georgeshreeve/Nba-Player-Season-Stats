from flask import Flask
from flask import got_request_exception
from flask_restful import Resource, Api, request
from flask import request, make_response, jsonify
from flask_httpauth import HTTPBasicAuth
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import boto3
import os
from loguru import logger
import json
from datetime import datetime, timedelta
import pandas as pd
from celery import Celery
import blinker
import redis
import fsspec
import s3fs

username = os.environ.get('USERNAME_')
password = os.environ.get('PASSWORD_')
access_key = os.environ.get('AWS_ACCESS_KEY_ID')
secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
region = os.environ.get('REGION_NAME')
database = os.environ.get('DATABASE')
output_location = os.environ.get('OUTPUTLOCATION')
bucket = os.environ.get('AWS_BUCKET')


app = Flask(__name__)
api = Api(app, prefix = "/api/v1")
auth = HTTPBasicAuth()

simple_app = Celery('CeleryWorker', broker='redis://redis:6379/0', backend='redis://redis:6379/0')

USER_DATA = {
    username : password
}

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"]
)

logger.add('/var/log/API_LOG.log', rotation = '3 MB', diagnose = False, serialize = True)

def log_exception(sender, exception, **extra):
    logger.error(exception)

got_request_exception.connect(log_exception, app)


@auth.verify_password
def verify(username, password):
    if not (username and password):
        logger.info("Authentication Error")
        return False
    return USER_DATA.get(username) == password


class Helpers:
    def AthenaClient():
        client = boto3.client('athena', aws_access_key_id = access_key, aws_secret_access_key = secret_access_key, region_name = region)
        return client
    def S3Client():
        client = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret_access_key, region_name = region)
        return client


class GeneratePlayers(Resource):

    PlayerName: str
    Id: int

    @limiter.limit("100/day")
    @auth.login_required
    def post(self):
        try:
            simple_app.send_task('CeleryWorker.DeleteOldQueries')
            logger.info("Sent asyn task: Delete Old Queries")
            try:
                data = request.get_json()
            except:
                data = {}
            PlayerName = data.get("PlayerName")
            Id = data.get("Id")

            if PlayerName and Id:
                query = "SELECT * FROM players WHERE player like '%{}%' and Id = {}".format(PlayerName,Id)
            elif PlayerName and not Id:
                query = "SELECT * FROM players WHERE player like '%{}%'".format(PlayerName)
            elif Id and not PlayerName:
                query = "SELECT * FROM players WHERE Id = {}".format(Id)
            else:
                query = "SELECT * FROM players"

            client = Helpers.AthenaClient()

            response_query_execution_id = client.start_query_execution(
                QueryString = query,
                QueryExecutionContext = {
                    'Database': database
                },
                ResultConfiguration = {
                    'OutputLocation': output_location
                }
            )
            QueryId = response_query_execution_id.get('QueryExecutionId')

            response_get_query_details = client.get_query_execution(
                QueryExecutionId = QueryId
                )

            QueryStatus = response_get_query_details['QueryExecution']['Status']

            ExpiryDate = format(datetime.utcnow() + timedelta(hours = 24), '%Y-%m-%d %H:%M:%S')

            if QueryStatus['State'] in ["FAILED","CANCELLED"]:
                logger.error("error in execution {} {} {}".format(data, QueryId, QueryStatus['State']))

                return make_response(jsonify(
                    {
                        "Message": {
                            "QueryId": QueryId,
                            "Status": QueryStatus,
                            "ExpiryDate": ExpiryDate
                        }
                    }
                ),404)

            elif QueryStatus['State'] in ["QUEUED","RUNNING","SUCCEEDED"]:
                logger.info("Successful execution {} {} {}".format(data, QueryId, QueryStatus['State']))

                return make_response(jsonify(
                    {
                        "Message": {
                            "QueryId": QueryId,
                            "Status": QueryStatus,
                            "ExpiryDate": ExpiryDate
                        }
                    }
                ),200)
            
            else:

                logger.info("Unknown Athena query status code {} {} {}".format(data ,QueryId, QueryStatus['State']))
                
                return make_response(jsonify(
                    {
                        "Message": "Internal Server Error"
                    }
                ),500)
            
        except Exception as e:
            logger.error(e)


class GenerateStats(Resource):

    Id: int
    Year: int
    PlayerName: str

    @limiter.limit("100/day")
    @auth.login_required
    def post(self):
        try:
            simple_app.send_task('CeleryWorker.DeleteOldQueries')
            logger.info("Sent asyn task: Delete Old Queries")
            try:
                data = request.get_json()
            except:
                data = {}
            Id = data.get("Id")
            Year = data.get("Year")
            PlayerName = data.get("PlayerName")
            logger.info("{} {} {}".format(Id,PlayerName,Year))

            if Id:
                query = "SELECT * FROM season_stats WHERE Id = {}".format(Id)
            elif Year:
                if PlayerName:
                    query = "SELECT * FROM season_stats WHERE Year = {} AND player LIKE '%{}%".format(Year,PlayerName)
                else:
                    query = "SELECT * FROM season_stats WHERE Year = {}".format(Year)
            else:
                query = "SELECT * FROM season_stats LIMIT 1000"
            
            client = Helpers.AthenaClient()

            response_query_execution_id = client.start_query_execution(
                QueryString = query,
                QueryExecutionContext = {
                    'Database': database
                },
                ResultConfiguration = {
                    'OutputLocation': output_location
                }
            )

            QueryId = response_query_execution_id.get('QueryExecutionId')

            response_get_query_details = client.get_query_execution(
                QueryExecutionId = QueryId
                )

            QueryStatus = response_get_query_details['QueryExecution']['Status']

            ExpiryDate = format(datetime.utcnow() + timedelta(hours = 24), '%Y-%m-%d %H:%M:%S')

            if QueryStatus['State'] in ["FAILED","CANCELLED"]:
                logger.error("error in execution {} {} {}".format(data, QueryId, QueryStatus['State']))

                return make_response(jsonify(
                    {
                        "Message": {
                            "QueryId": QueryId,
                            "Status": QueryStatus,
                            "ExpiryDate": ExpiryDate
                        }
                    }
                ),404)

            elif QueryStatus['State'] in ["QUEUED","RUNNING","SUCCEEDED"]:
                logger.info("Successful execution {} {} {}".format(data, QueryId, QueryStatus['State']))

                return make_response(jsonify(
                    {
                        "Message": {
                            "QueryId": QueryId,
                            "Status": QueryStatus,
                            "ExpiryDate": ExpiryDate
                        }
                    }
                ),200)
            
            else:

                logger.info("Unknown Athena query status code {} {} {}".format(data ,QueryId, QueryStatus['State']))
                
                return make_response(jsonify(
                    {
                        "Message": "Internal Server Error"
                    }
                ),500)
        except Exception as e:
            logger.error(e)

class GetData(Resource):

    QueryId: str

    @limiter.limit("100/day")
    @auth.login_required
    def get(self):
        try:
            try:
                data = request.get_json()
            except:
                data = {}
            QueryId = data.get('QueryId')
            if not QueryId:
                logger.info("QueryId not supplied in request {}".format(data))
                return make_response(jsonify(
                    {
                        "Message": {
                            "QueryId": QueryId,
                            "Status": "Query Id is required"
                        },
                        "data": []
                    }
                ),404)
            else:
                athena_clinet = Helpers.AthenaClient()
                try:
                    response_get_query_details = athena_clinet.get_query_execution(
                        QueryExecutionId = QueryId
                    )
                except Exception as e:
                    logger.error(e)
                    return make_response(jsonify(
                        {
                            "Message": {
                                "QueryId": QueryId,
                                "Status": "Query Does not exist"
                            },
                            "data": []
                        }
                    ),404)
                QueryStatus = response_get_query_details['QueryExecution']['Status']['State']
                if QueryStatus != "SUCCEEDED":
                    logger.info("Query Not ready yet {} {} {}".format(data, QueryId, QueryStatus))
                    return make_response(jsonify(
                        {
                            "Message": {
                                "QueryId": QueryId,
                                "Status": "Query not ready " + QueryStatus
                            },
                            "data": []
                        }
                    ),404)
                else:
                    query_response = pd.read_csv(
                        "s3://{}/QueryResults/{}.csv".format(bucket,QueryId),
                        storage_options={
                            "key": access_key,
                            "secret": secret_access_key
                        }
                        )
                    if not query_response.empty:
                        data = []
                        response = query_response.to_json(orient = 'records', lines = True).splitlines()
                        for line in response[:]:
                            obj = json.loads(line)
                            data.append(obj)
                        logger.info("Successful execution {} {}".format(QueryId, QueryStatus))
                        return make_response(jsonify(
                            {
                                "Message": {
                                    "QueryId": QueryId,
                                    "Status": QueryStatus
                                },
                                "data": data
                            }
                        ),200)
                    else:
                        logger.error("Query Id does not exist {}".format(QueryId))
                        return make_response(jsonify(
                            {
                                "Message": {
                                    "QueryId": QueryId,
                                    "Status": "Query Produced a Dataset With 0 Records"
                                },
                                "data": []
                            }
                        ),404)
        except Exception as e:
            logger.error(e)




class HelloWorld(Resource):
    @limiter.limit("100/day")
    @auth.login_required
    def get(self):
        logger.info("successful execution")     
        return make_response(jsonify({"message":"Hello World"}), 200)

api.add_resource(HelloWorld, "/HelloWorld")
api.add_resource(GeneratePlayers, "/GeneratePlayers")
api.add_resource(GenerateStats, "/GenerateStats")
api.add_resource(GetData, "/GetData")



if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000')
