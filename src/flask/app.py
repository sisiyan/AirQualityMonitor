from flask import Flask, render_template, json, request
app = Flask(__name__)

@app.route("/")
def main():
    return render_template('index.html')

@app.route('/showResult', methods=['POST'])
def showResult():
    _statename = request.form['State']
    _countyname = request.form['County']

    # validate the received values
    if _statename and _countyname:
        return json.dumps({'html':'<span>All fields good !!</span>'})
    else:
        return json.dumps({'html':'<span>Enter the required fields</span>'})

if __name__ == "__main__":
    app.run()
