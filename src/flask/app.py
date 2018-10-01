from flask import Flask, render_template, json, request
from wtforms import Form, StringField, SelectField
from searchform import SearchForm

app = Flask(__name__)
app.secret_key = 'development key'


@app.route("/")
def main():
    form = SearchForm()

    if request.method == 'POST':
        if form.validate() == False:
            flash('All fields are required.')
            return render_template('index.html', form = form)
        else:
            return render_template('success.html')
    elif request.method == 'GET':
        return render_template('index.html', form = form)


# @app.route('/showResult', methods=['POST'])
# def showResult():
#     _statename = request.form['State']
#     _countyname = request.form['County']
#
#     # validate the received values
#     if _statename and _countyname:
#         return json.dumps({'html':'<span>All fields good !!</span>'})
#     else:
#         return json.dumps({'html':'<span>Enter the required fields</span>'})

if __name__ == "__main__":
    app.run()
