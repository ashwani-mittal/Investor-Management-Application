from mysql.connector import connect
from flask import Flask, request
import json


app = Flask(__name__)


db_config = {
    'host':'stocks.cfu1xy0gmqpq.us-east-2.rds.amazonaws.com',
    'user':'ashwanim',
    'password':'axm220114',
    'database':'stocksdb',
    'port': 3306
}

@app.route('/get-all-investors', methods = ['GET'])
def get_all_investors():
    content_type = request.headers.get('Content-Type')
    if content_type not in ('text/plain','application/json'):
        return f'Unsupported content-type', 415
    if content_type is None or content_type == '':
        content_type = 'text/plain'
    try:
        cnx = connect(**db_config)
        cursor = cnx.cursor(dictionary=True)
        cursor.execute('select * from investors')
        result = cursor.fetchall()
        investors = ''
        if content_type == 'text/plain':
            for record in result:
                investors += f"(firstname: {record['firstname']},lastname: {record['lastname']},account_balance: {record['account_balance']},address: {record['address']})\n" # type: ignore
        else:
            investors = json.dumps(result)    
    except Exception as e:
        return (f'Unable to get investors: {str(e)}', 500)
    finally:
        cursor.close()
        cnx.close()
    return investors
        

@app.route('/get-investor/<id>', methods = ['GET']) # type: ignore
def get_investor(id):
    try:
        cnx = connect(**db_config)
        cursor = cnx.cursor(dictionary=True)
        cursor.execute('select * from investors where id = %s', (id,))
        result = cursor.fetchone()
        cursor.close()
        cnx.close()
    except Exception as e:
        return (f'Unable to get investor with ID {id}: {str(e)}', 501)
    finally:
        cursor.close()
        cnx.close()
    return result if result is not None else 'No Data Found.', 200

@app.route('/get-investors-by-name', methods = ['GET'])
def get_investors_by_name():
    try:
        firstname = request.args.get('firstname')
        if firstname is None or firstname == '':
            return 'Please pass first name as a query parameter', 500
        cnx = connect(**db_config)
        cursor = cnx.cursor(dictionary=True)
        cursor.execute('select * from investors where firstname = %s', (firstname,))
        result = cursor.fetchall()
    except Exception as e:
        return (f'Unable to get investors with name: {firstname}: {str(e)}', 501)  # type: ignore
    finally:
        cursor.close()
        cnx.close()
    return ('No Data Found',500) if result is None else (result,202)

@app.route('/create-new-investor', methods = ['POST'])
def create_new_investor():
    try:
        body = request.get_json()
        account_balance = None if 'account_balance' not in body else body['account_balance']
        address = None if 'address' not in body else body['address']
        create_investor(body['firstname'], body['lastname'], account_balance, address)
        return 'New investor created', 200
    except KeyError as ke:
        return (f'Expected fields were not found in body: {str(ke)}', 501)
    except Exception as e:
        return (f'Unable to create investor: {str(e)}', 501)

@app.route('/delete-investors', methods = ['DELETE'])
def delete_investors():
    try:
        investor_ids= request.get_json()
        if len(investor_ids) == 0:
            return '', 200
        cnx = connect(**db_config)
        cursor = cnx.cursor()
        for id in investor_ids:
            cursor.execute('delete from investors where id in (%s)',(id,))
        cnx.commit()
    except Exception as e:
        return (f'Failed to delete investors: {str(e)}', 501)
    finally:
        cursor.close()
        cnx.close()
    return '', 200
    
@app.route('/update-address/<id>', methods = ['PUT'])
def update_address(id):
    try:
        address = request.args.get('address')
        if address is None or address == '':
            return 'Please pass first name as a query parameter', 500
        cnx = connect(**db_config)
        cursor = cnx.cursor(dictionary=True)
        cursor.execute('update investors set address = %s where id = %s', (address,id))
        cnx.commit()

    except Exception as e:
        return (f'Failed to update address: {str(e)}', 501)
    finally:
        cursor.close()
        cnx.close()
    return '',200


def create_investor(firstname,lastname, account_balance,address):
    try:
        cnx = connect(**db_config)
        cursor = cnx.cursor(dictionary=True)
        cursor.execute('insert into investors (firstname,lastname,account_balance, address) values (%s,%s,%s,%s)', (firstname,lastname,account_balance,address))
        cnx.commit()
        cursor.close()
        cnx.close()
    except Exception as e:
        raise(e)


if __name__ == '__main__':
    app.run(port = 5525)
