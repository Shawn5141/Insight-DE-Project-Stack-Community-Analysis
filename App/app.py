#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import networkx as nx
import pandas as pd
from datetime import datetime
from textwrap import dedent as d
import json

import subprocess
import os
import math

# from s3fs.core import S3FileSystem
# from pyarrow.parquet import ParquetDataset
from DataModel import *
from configFile import *
from LineGraph import line_graph
from NetworkGraph import network_graph
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from s3_to_spark.Calculation import CalculateRangeYearTagCount2,CalculateSingleTagCount
"""



# import the css template, and pass the css template into dash
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server
app.title = "Stack Overflow Network"



YEAR=[2010, 2019]
filterNumber = 3000
ActiveUserShowChosen = False
ACCOUNT="A0001"
tagSelection = []

# read from tagName
Edge = pd.read_csv('/home/ubuntu/Stack-Community/App/tmp/edge.csv')
Node = pd.read_csv('/home/ubuntu/Stack-Community/App/tmp/singleTagCount.csv')
nodeDict={}


GolbalActiveUserMap = {}
# prevGraph =None

for index, row in Node.iterrows():
    nodeDict[row['Tags']] = row['singleTagCount']

nodeOption = [
    {"label": str(node), "value": str(node)}
    for node in nodeDict.keys()
]
for index, row in Edge.iterrows():
    nodeOption.append( {"label": str(row['Original_Tags']), "value": str(row['Original_Tags'])})
    
    

styles = {
    'pre': {
        'border': 'thin lightgrey solid',
        'overflowX': 'scroll'
    }
}

app.layout = html.Div([
    html.Div([html.H1("Stack Overflow Tag Network Analysis")],
             className="row",
             style={'textAlign': "center"}),
    #html.Abbr("\u003F", title="Hello, I am hover-enabled helpful information."),
    html.Br(),
    html.Div(
        className="row",
        children=[
            
            html.Div(
                className="six columns",
                children=[
                    html.Div(
                            className="eight columns",
                            children=[
                                 dcc.Markdown(d("""
                                **Tags To Search**
                                Input the tag to visualize.
                                """)),      
                                 dcc.Dropdown(
                                    id="tagSelect",
                                    options=nodeOption,
                                    multi=True,
                                    value=[],

                                    className="dcc_control",
                                    ),
                                html.Br(),
                                dcc.Markdown(d("""
                                **Active Users ** : show top 3 users who answer the most.

                                """)),
                                dcc.RadioItems(
                                options=[
                                    {'label': 'Show No Active Users', 'value': 'False'},
                                    {'label': 'Show Active Users', 'value': 'True'},

                                ],
                                value='False',
                                labelStyle={'display': 'inline-block'},
                                id="ActiveUser_radioitems",
                    ) ,
                ],
            ),
            html.Div(
                className="twelve columns",
                children=[
                    dcc.Graph(id="my-graph",figure=network_graph(YEAR,tagSelection ,filterNumber,ActiveUserShowChosen)), 
                   dcc.Markdown(d("""
                    Node & Edge size : number of tags/composite tags  used in posts
                   
                    """)),      
                            ],
                            style={'height': '80px'}
                        ),
                    
                    
                    
                   ],
                 
            ),
           

            #########################################right side two output component
                        html.Div(
                className="four columns",
                children=[
                    dcc.Markdown(d("""
                            **Granularity filter ** : Select Post Number for filtering.

                            """)),
                     
                    html.Div(
                        #className="four columns",
                        children=[
                        dcc.Input(
                            id="filterNum",
                            value=3000,
                            placeholder="input filter Number",

                        )]
                    ),
                    html.Br(),
                    html.Div(
                        className="two columns",
                        children=[
                            dcc.Markdown(d("""
                            **Time Range ** : Select Year Range.
                            
                            """)),
                            dcc.RangeSlider(
                                id='my-range-slider',
                                min=2008,
                                max=2020,
                                step=1,
                                value=[2008, 2019],
                                marks={
                                    2008: {'label':'2008'},
                                    2009: {'label':'2009'},
                                    2010: {'label': '2010'},
                                    2011: {'label': '2011'},
                                    2012: {'label': '2012'},
                                    2013: {'label': '2013'},
                                    2014: {'label': '2014'},
                                    2015: {'label': '2015'},
                                    2016: {'label': '2016'},
                                    2017: {'label': '2017'},
                                    2018: {'label': '2018'},
                                    2019: {'label': '2019'},
                                    2020: {'label':'2020'}
                                }
                            ),
                            html.Br(),
                           
                            
                        ],
                        style={'display': 'inline-block','height': '50px','width':'600px'}
                    ),
                    
                    html.Div(
                    className="two columns",
                    children=[
                        
                       dcc.Graph(id='tag-trend',figure=line_graph(tagSelection)),
                      
                    ],
                        
                   style = {'display': 'inline-block', 'width':'700px','height': '350px'}
                ),
                     
                ]
            )
        ]
    )
])



@app.callback(
    dash.dependencies.Output('my-graph', 'figure'),
    [dash.dependencies.Input('my-range-slider', 'value'),
     dash.dependencies.Input('tagSelect', 'value') ,
     dash.dependencies.Input('filterNum', 'value'),
     dash.dependencies.Input('ActiveUser_radioitems', 'value')],
    [dash.dependencies.State('my-graph', 'figure')])
def update_output(value,tagSelection,filterNum,ActiveUserShowChosen,fig_state):
    YEAR = value
    filterNumber = filterNum
    tagSelect = tagSelection
    ActiveUserShowChosen = True if ActiveUserShowChosen=="True" else False
    #print("fig_state",fig_state)
    return network_graph(value,tagSelect, filterNumber,ActiveUserShowChosen)
 

@app.callback(
    dash.dependencies.Output('tagSelect', 'value'),
    [dash.dependencies.Input('my-graph', 'selectedData'),
    dash.dependencies.State('tagSelect', 'value')])
def update_options(selectedData, value):
    #print("update_options selectedData",selectedData,value)
    if selectedData and len(selectedData["points"])>0 and "hovertext" in selectedData["points"][0] and selectedData["points"][0]["hovertext"]:
        data = selectedData["points"][0]["hovertext"].split('<')[0]
        if data in value:
            value.remove(data)
        else:
            value.append(data)
           
    return value

@app.callback(
    dash.dependencies.Output('tag-trend', 'figure'),
    dash.dependencies.Input('tagSelect', 'value'))
def update_trend( value):
    return line_graph(value)


    
if __name__ == '__main__':
      
    import socket
    host = socket.gethostbyname(socket.gethostname())
    app.run_server(debug=True, host=host, port = 4444)

#     app.run_server(
#         port=8000,
#         host='0.0.0.0'
#     )
