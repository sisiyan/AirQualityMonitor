# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime as dt
import query



weather_params = ('Temperature', 'Wind Speed', 'Relative Humidity', 'Barometric Pressure')
gases_params = ('CO', 'SO2', 'NO2', 'Ozone')
particulates_params = ('PM2.5 FRM', 'PM2.5 non FRM', 'PM10 Mass')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1(children = 'Welcome to search for local air quality and weather!'),

    html.Label('State'),
    dcc.Input(id='state', value='Connecticut', type='text'),

    html.Label('County'),
    dcc.Input(id='county',value='New Haven', type='text'),

    html.Label('Choose weather parameter'),
    dcc.Dropdown(
        id='weather',
        options=[{'label': p, 'value': p} for p in weather_params],
        multi=False, value = 'Temperature'
    ),

    html.Label('Choose gas pollutant parameter'),
    dcc.Dropdown(
        id='gases',
        options=[{'label': p, 'value': p} for p in gases_params],
        multi=False, value = 'SO2'
    ),

    html.Label('Choose particulate pollutant parameter'),
    dcc.Dropdown(
        id='particulates',
        options=[{'label': p, 'value': p} for p in particulates_params],
        multi=False, value = 'PM2.5 FRM'
    ),

    dcc.Graph(id='weather_graph'),
    dcc.Graph(id='gases_graph'),
    dcc.Graph(id='particulates_graph')
])


@app.callback(
    dash.dependencies.Output('weather_graph', 'figure'),
    [dash.dependencies.Input('state', 'value'),
     dash.dependencies.Input('county', 'value'),
     dash.dependencies.Input('weather', 'value')]
)
def update_weatherGraph(state,county,weather):
    print ("inside update graph")
    connection = query.get_connection()
    col_name, unit, df_w = query.query_weather(connection, state, county, weather)

    return {
        'data': [{
            'x': df_w['date_GMT'].values,
            'y': df_w[col_name].values
        }],
        'layout':{
                'title':'Weather Trend',
                'xaxis':{
                    'title':'Time'
                },
                'yaxis':{
                     'title': unit
                }
            }
    }

@app.callback(
    dash.dependencies.Output('gases_graph', 'figure'),
    [dash.dependencies.Input('state', 'value'),
     dash.dependencies.Input('county', 'value'),
     dash.dependencies.Input('gases', 'value')]
)
def update_gasGraph(state,county,gases):
    print ("inside update graph")
    connection = query.get_connection()
    col_name, unit, df_g = query.query_gases(connection, state, county, gases)

    return {
        'data': [{
            'x': df_g['date_GMT'].values,
            'y': df_g[col_name].values
        }],
        'layout':{
                'title':'Gas Pollutant Trend',
                'xaxis':{
                    'title':'Time'
                },
                'yaxis':{
                     'title': unit
                }
            }
    }

@app.callback(
    dash.dependencies.Output('particulates_graph', 'figure'),
    [dash.dependencies.Input('state', 'value'),
     dash.dependencies.Input('county', 'value'),
     dash.dependencies.Input('particulates', 'value')]
)
def update_particulateGraph(state,county,particulates):
    print ("inside update graph")
    connection = query.get_connection()
    col_name, unit, df_p = query.query_particulates(connection, state, county, particulates)

    return {
        'data': [{
            'x': df_p['date_GMT'].values,
            'y': df_p[col_name].values
        }],
        'layout':{
                'title':'particulate Pollutant Trend',
                'xaxis':{
                    'title':'Time'
                },
                'yaxis':{
                     'title': unit
                }
            }
    }

if __name__ == '__main__':
    app.run_server(host="ec2-54-146-48-213.compute-1.amazonaws.com")
