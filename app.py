from flask import Flask, jsonify, request, session, redirect, url_for
import pymysql
import jwt
import requests
import base64
from jwt.algorithms import RSAAlgorithm
import boto3
import json
import time
import logging

app = Flask(__name__)

# mysql -h mysql-database.czrn9xpuxd4a.us-east-2.rds.amazonaws.com -u admin -pEa12345678!
def get_db_connection():
    return pymysql.connect(host='mysql-database.czrn9xpuxd4a.us-east-2.rds.amazonaws.com',
                           user='admin',
                           password='Ea12345678!',
                           db='6156service',
                           cursorclass=pymysql.cursors.DictCursor)


def publish_to_sns(subject, message):
    topicArn = 'arn:aws:sns:us-east-2:858059470707:6156topic'
    snsClient = boto3.client(
        "sns",
        aws_access_key_id="AKIA4PSCUO5Z3NQ2GHPR",
        aws_secret_access_key="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",
        region_name="us-east-2"
    )

    response = snsClient.publish(TopicArn=topicArn, Message=message, Subject= subject)
    app.logger.info(response['ResponseMetadata']['HTTPStatusCode'])


def get_info_from_token(id_token, access_token):
    user_pool_id = 'us-east-2_ZqnrAhXRt'
    region = 'us-east-2'

    jwks_uri = f"https://cognito-idp.{region}.amazonaws.com/{user_pool_id}/.well-known/jwks.json"
    jwks = requests.get(jwks_uri).json()['keys']

    id_headers = jwt.get_unverified_header(id_token)
    id_key = next((k for k in jwks if k['kid'] == id_headers['kid']), None)
    if id_key is None:
        raise ValueError("Invalid ID token. Key ID not found.")
    rsa_key = RSAAlgorithm.from_jwk(json.dumps(id_key))
    id_decoded = jwt.decode(id_token, rsa_key, algorithms=['RS256'], options={'verify_aud': False, 'verify_iss': False})
    uuid = id_decoded.get('sub')

    access_headers = jwt.get_unverified_header(access_token)
    access_key = next((k for k in jwks if k['kid'] == access_headers['kid']), None)
    if access_key is None:
        raise ValueError("Invalid access token. Key ID not found.")
    rsa_key = RSAAlgorithm.from_jwk(json.dumps(access_key))
    access_decoded = jwt.decode(access_token, rsa_key, algorithms=['RS256'], options={'verify_aud': False, 'verify_iss': False})
    scopes = access_decoded.get('scope', '').split()

    return uuid, scopes

@app.route('/jobs', methods=['POST'])
def create_posting():
    id_token = request.headers.get('Authorization')
    access_token = request.headers.get('Authorization2')
    if not id_token or not access_token:
        return jsonify({'message': 'Tokens not provided'}), 401
    try:
        uuid, scopes = get_info_from_token(id_token, access_token)
        print("UUID:", uuid)
        print("Scopes:", scopes)
        
        required_scope = 'create:jobs'
        if required_scope not in scopes:
            return jsonify({'message': 'Insufficient scope'}), 403

        data = request.json
        conn = get_db_connection()
        cursor = conn.cursor()

        query = '''INSERT INTO posting (category, description, employerID, experience, location, package, title, type, company_name)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        cursor.execute(query, (data['category'], data['description'], uuid, data['experience'], 
                               data['location'], data['package'], data['title'], data['type'], data['company_name']))
        conn.commit()
        cursor.close()
        conn.close()

        # subject = 'New Job Posting: ' + data['title']
        # message = 'A new job posting has been added to the database. Title: ' + data['title']
        # publish_to_sns(subject, message)

        return jsonify({'message': 'Posting created'}), 201

    except jwt.ExpiredSignatureError:
        return jsonify({'message': 'Signature expired. Please log in again.'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Invalid token. Please log in again.'}), 401
    except Exception as e:
        return jsonify({'message': str(e)}), 500


@app.route('/jobs', methods=['GET'])
def get_all_posting():
    conn = get_db_connection()
    cursor = conn.cursor()

    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    offset = (page - 1) * limit

    query = "SELECT * FROM posting WHERE 1=1"
    count_query = "SELECT COUNT(*) FROM posting WHERE 1=1"
    query_params = []

    if 'category' in request.args:
        query += " AND category = %s"
        query_params.append(request.args['category'])
    
    if 'location' in request.args:
        query += " AND location = %s"
        query_params.append(request.args['location'])

    query += " LIMIT %s OFFSET %s"
    query_params.extend([limit, offset])

    cursor.execute(query, query_params)
    result = cursor.fetchall()

    cursor.execute(count_query)
    total_records = cursor.fetchone()['COUNT(*)']
    total_pages = (total_records + limit - 1) // limit

    cursor.close()
    conn.close()

    return jsonify({
        'data': result,
        'total_records': total_records,
        'total_pages': total_pages,
        'current_page': page,
        'limit': limit
    })


@app.route('/jobs/employer', methods=['GET'])
def get_employer_posting():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'No token provided'}), 401

    try:
        uuid = get_uuid_from_token(token)
        conn = get_db_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM posting WHERE employerID = %s"

        cursor.execute(query, (uuid,))
        result = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify({
            'data': result,
        })

    except ValueError as e:
        return jsonify({'message': str(e)}), 401
    except jwt.ExpiredSignatureError:
        return jsonify({'message': 'Signature expired. Please log in again.'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Invalid token. Please log in again.'}), 401
    except Exception as e:
        return jsonify({'message': str(e)}), 500


@app.route('/jobs/my', methods=['GET'])
def get_my_posting():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'No token provided'}), 401

    try:
        uuid = get_uuid_from_token(token)
        conn = get_db_connection()
        cursor = conn.cursor()

        query_student = '''
        SELECT p.* FROM posting p
        JOIN application a ON p.postingID = a.postingID
        WHERE a.studentID = %s
        '''
        cursor.execute(query_student, (uuid,))
        student_result = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify({
            'data': student_result
        })

    except ValueError as e:
        return jsonify({'message': str(e)}), 401
    except jwt.ExpiredSignatureError:
        return jsonify({'message': 'Signature expired. Please log in again.'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Invalid token. Please log in again.'}), 401
    except Exception as e:
        return jsonify({'message': str(e)}), 500


@app.route('/jobs/<int:posting_id>', methods=['GET'])
def get_posting(posting_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM posting WHERE postingID = %s"
    cursor.execute(query, (posting_id,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if result:
        return jsonify(result)
    else:
        return jsonify({'message': 'Job posting not found'}), 404


@app.route('/jobs/<int:posting_id>', methods=['PUT'])
def update_posting(posting_id):
    data = request.json
    conn = get_db_connection()
    cursor = conn.cursor()
    query = '''UPDATE posting SET category=%s, description=%s, employerID=%s, experience=%s, 
               location=%s, package=%s, title=%s, type=%s WHERE postingID=%s'''
    cursor.execute(query, (data['category'], data['description'], data['employerID'], data['experience'], 
                           data['location'], data['package'], data['title'], data['type'], posting_id))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': 'Posting updated'}), 200


@app.route('/jobs/<int:posting_id>', methods=['DELETE'])
def delete_posting(posting_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = 'DELETE FROM posting WHERE postingID=%s'
    cursor.execute(query, (posting_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'message': 'Posting deleted'}), 200


@app.route('/test', methods=['GET'])
def testing():
    return jsonify({'message': 'Testing!!'})

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlaskApp")

def logger_middleware(app):
    def middleware(environ, start_response):
        start = time.time()
        response = app(environ, start_response)
        duration = time.time() - start
        logger.info(f"Received {environ['REQUEST_METHOD']} request on {environ['PATH_INFO']} - {duration:.2f}s")
        return response
    return middleware
app.wsgi_app = logger_middleware(app.wsgi_app)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='0.0.0.0', port=5000)