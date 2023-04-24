from flask import Flask, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


@app.route('/tospring', methods=['GET'])
def api_data():
    gold = request.args.get('GOLD')
    level = request.args.get('LEVEL')
    result = gold + level
    return result


if __name__ == '__main__':
    app.run(debug=False, host="127.0.0.1", port=5000)