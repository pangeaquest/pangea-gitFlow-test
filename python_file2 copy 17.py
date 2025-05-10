from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/data', methods=['GET', 'POST'])
def data():
    if request.method == 'GET':
        return jsonify({"message": "This is a GET request"})
    elif request.method == 'POST':
        data = request.json
        return jsonify({"message": "This is a POST request", "data": data})

if __name__ == '__main__':
    app.run(debug=True)
