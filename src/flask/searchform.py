from flask_wtf import Form
from wtforms import TextField, IntegerField, TextAreaField, SubmitField, RadioField, SelectField

from wtforms import validators, ValidationError

class SearchForm(Form):
    state = TextField("State",[validators.Required("Please enter state name.")])
    county = TextField("County",[validators.Required("Please enter county name.")])

    submit = SubmitField("Send")
