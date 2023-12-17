from flask import Flask, jsonify, request, session, redirect, url_for
import pymysql
import jwt
import requests
import base64
from jwt.algorithms import RSAAlgorithm
import boto3

app = Flask(__name__)

def get_db_connection():
    return pymysql.connect(host='mysql-database.czrn9xpuxd4a.us-east-2.rds.amazonaws.com',
                           user='admin',
                           password='Ea12345678!',
                           db='6156service',
                           cursorclass=pymysql.cursors.DictCursor)

def exchange_code_for_token(code):
    app.logger.info('Exchanging code for token')
    token_url = 'https://linkliv.auth.us-east-2.amazoncognito.com/oauth2/token'
    client_id = '5in91u4mqc5kbjhb0b15vfecpf'
    client_secret = 's7uu8f5lt7c8ges8aekk0f05ttc7c9ep03k8iqk3bss5ifsi975'
    redirect_uri = 'https://xz9t45v28k.execute-api.us-east-2.amazonaws.com/beta/login'

    client_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    data = {
        'grant_type': 'authorization_code',
        'client_id': client_id,
        'code': code,
        'redirect_uri': redirect_uri
    }

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {client_auth}'
    }

    response = requests.post(token_url, data=data, headers=headers)
    if response.status_code != 200:
        app.logger.info(f"Failed to exchange code for token: {response.json()}")
        return None
    return response.json()

def get_pem_from_jwks(jwks, kid):
    app.logger.info('Retrieving PEM from JWKS')
    for key in jwks['keys']:
        if key['kid'] == kid:
            return RSAAlgorithm.from_jwk(key)
    app.logger.info('No matching KID found in JWKS')
    return None

def validate_token(token):
    app.logger.info('Validating token')
    jwks_url = 'https://cognito-idp.us-east-2.amazonaws.com/us-east-2_ZqnrAhXRt/.well-known/jwks.json'
    jwks = requests.get(jwks_url).json()

    headers = jwt.get_unverified_header(token)
    kid = headers['kid']

    pem = get_pem_from_jwks(jwks, kid)
    if pem is None:
        app.logger.info('PEM is None')
        return False

    try:
        jwt.decode(token, pem, algorithms=['RS256'])
        app.logger.info('Token successfully validated')
        return True
    except jwt.ExpiredSignatureError:
        app.logger.info('Token expired')
        return False
    except jwt.InvalidTokenError:
        app.logger.info('Invalid token')
        return False

def handle_token(code):
    app.logger.info('Handling token')
    if not code:
        app.logger.info('No code provided')
        return None

    token_response = exchange_code_for_token(code)
    if not token_response:
        app.logger.info('No token response')
        return None

    access_token = token_response.get('access_token')
    if not access_token:
        app.logger.info('No access token found in response')
        return None

    try:
        if validate_token(access_token):
            return access_token
    except jwt.ExpiredSignatureError:
        app.logger.info('Token expired during handling')
    except jwt.InvalidTokenError:
        app.logger.info('Invalid token during handling')

    return None

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












@app.route('/login')
def login():
    code = request.args.get('code')

    if not code:
        app.logger.info('No code provided')
        return jsonify({'message': 'No code provided'}), 400

    access_token = handle_token(code)

    if not access_token:
        app.logger.info('Authentication failed')
        return jsonify({'message': 'Unauthorized'}), 401

    # return jsonify({'message': 'Success'})
    frontend_url = 'http://lionkedin-angular.s3-website-us-east-1.amazonaws.com/home'
    return redirect(f"{frontend_url}?access_token={access_token}")

@app.route('/jobs', methods=['POST'])
def create_posting():
    data = request.json
    conn = get_db_connection()
    cursor = conn.cursor()
    query = '''INSERT INTO posting (category, description, employerID, experience, location, package, title, type)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
    cursor.execute(query, (data['category'], data['description'], data['employerID'], data['experience'], 
                           data['location'], data['package'], data['title'], data['type']))
    conn.commit()
    cursor.close()
    conn.close()

    subject = 'New Job Posting: ' + data['title']
    message = 'A new job posting has been added to the database. Title: ' + data['title']
    publish_to_sns(subject, message)

    return jsonify({'message': 'Posting created'}), 201

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
def testing(posting_id):
    return jsonify({'message': 'Testing'}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')


# mysql -h mysql-database.czrn9xpuxd4a.us-east-2.rds.amazonaws.com -u admin -pEa12345678!