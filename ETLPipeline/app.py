#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import networkx as nx
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from colour import Color
from datetime import datetime
from textwrap import dedent as d
import json

import subprocess
import os
import math
import numpy as np

from s3fs.core import S3FileSystem
from pyarrow.parquet import ParquetDataset
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
app.title = "Stack Overflow Network"

YEAR=[2010, 2019]
filterNumber = 3000
ACCOUNT="A0001"
tagSelection = []
prev_tagSelection =[]
# read from tagName
Edge = pd.read_csv('/home/ubuntu/Stack-Community/ETLPipeline/App/tmp/edge.csv')
Node = pd.read_csv('/home/ubuntu/Stack-Community/ETLPipeline/App/tmp/singleTagCount.csv')
nodeDict={}
trendMap={}
TagName2trendMap ={}
for index, row in Node.iterrows():
    nodeDict[row['Tags']] = row['singleTagCount']

nodeOption = [
    {"label": str(node), "value": str(node)}
    for node in nodeDict.keys()
]
for index, row in Edge.iterrows():
    nodeOption.append( {"label": str(row['Original_Tags']), "value": str(row['Original_Tags'])})
    
    
#/home/ubuntu/Stack-Community/ETLPipeline/calculate.sh


def getYearList(TagName):
    global TagName2trendMap
    if TagName in TagName2trendMap:
        return TagName2trendMap[TagName]
    pid = os.getpid()
    fileName = '_'.join([e for e in TagName.split(',')])
    path = os.getcwd()+'/App/tmp'+"/"+fileName+"_yearTagCount.csv"
    if not os.path.isfile(path):
        command = os.getcwd() +"/App/calculate.sh"+" --method="+str(2)+" --pid="+str(pid)+" --path="+path+" --list="+TagName
        process_output = subprocess.call([command],shell=True)
    return pd.read_csv(path)

def line_graph(tagSelect):
    global trendMap
    global prev_tagSelection
    
    
    d = {'Year': [], 'count': []}
    df = pd.DataFrame(data=d)
    if not prev_tagSelection and not tagSelect:
        print("no value",prev_tagSelection,tagSelect)
        return go.Figure(data=[go.Scatter(x=[], y=[])])
    if prev_tagSelection==tagSelect:
        print("prev equal to curr",prev_tagSelection)
        return trendMap[tuple(prev_tagSelection)]
    print("curr",tagSelect)
    for idx,tagName in enumerate(tagSelect):
        if idx==0:
            df = getYearList(tagName)
            print("first df",df.head())
            df = df.drop(columns=['Tags','Unnamed: 0'])
            df = df.rename(columns={"count": tagName+"_count"})
        else:
            print(idx,"following df",df.head())
            tmp = getYearList(tagName)
            tmp = tmp.drop(columns=['Tags','Unnamed: 0'])
            tmp = tmp.rename(columns={"count": tagName+"_count"})
            df = df.join(tmp.set_index('Year'),lsuffix='_left', rsuffix='_right', on='Year')
        print(idx,df.head())
    
    
    y_val = [tag+"_count" for tag in tagSelect]
    print("finish merging",y_val)
    fig = px.line(df, x="Year", y=y_val)
    fig.update_traces(mode='markers+lines')
    fig.update_layout(margin={'l': 10, 'b': 50, 't': 10, 'r': 0}, hovermode='closest')
        
    TagName2trendMap[tuple(tagSelect)] = df
    prev_tagSelection = tagSelect
    trendMap[tuple(prev_tagSelection)] = fig
    return fig

previous_year = None

yearMap = set()
edgeMap = {}
nodeMap = {}

def network_graph(yearRange,tagSelect,filterNumber=3000):
    print("network begin",yearRange,tagSelect,filterNumber)
    """
    tag      count     
    [1]      500
    [1,2,3]  1234
    TODO:
    1.
    check if the yearRange exist in cache map
    if not in cache map then check if it's in database (subprocess to run in background )
    if not in database, then compute using spark 
        
    2. 
    change annually tag store in database   
    
    """
    

    #global previous_year
    global yearMap
    global edgeMap
    global nodeMap
    yearRange= tuple(yearRange)
    if yearRange not in yearMap:
        print("not in yearMap",yearRange)
    #     #/home/ubuntu/Stack-Community/ETLPipeline/calculate.sh
    #     command = os.getcwd() +"/App/calculate.sh " + str(yearRange[0])+" "+str(yearRange[1])+" "+str("300")
    #     process_output = subprocess.call([command],shell=True)
    #     print("after process")
        path = "/home/ubuntu/Stack-Community/ETLPipeline/App/tmp/"
        prefix = str(yearRange[0])+str(yearRange[1])
        EdgePath = path+prefix+"edge.csv"
        NodePath = path+prefix+"singleTagCount.csv"
        Edge = pd.read_csv(EdgePath)
        Node = pd.read_csv(NodePath)
        #previous_year = yearRange
        yearMap.add(yearRange)
        edgeMap[yearRange] = Edge
        nodeMap[yearRange] = Node
    else:
        Edge = edgeMap[yearRange]
        Node = nodeMap[yearRange]
    
    # for select tag display
    selectEdge = set()
    selectNode = set()    
    G= nx.MultiDiGraph()
    
    #G = nx.random_geometric_graph(200, 0.125) 
    node_dict = {}
    edge_dict ={}
    for index, row in Edge.iterrows():
        if row['YearTagCount']>int(filterNumber):
            node_dict[row['Original_Tags']] = row['YearTagCount']
            edge_dict[(row['Original_Tags'],row['Tags'])] = row['YearTagCount']
            if row['Tags'] in tagSelect or row['Original_Tags'] in tagSelect :
                selectEdge.add(row['Original_Tags'])
                selectNode.add(row['Tags'])
#                 for tag in row['Original_Tags'].split(','):
#                     selectNode.add(tag)
            
            
    maxNode_size = -float('inf')    
    for index, row in Node.iterrows():
        if row['singleTagCount']>int(filterNumber):
            node_dict[row['Tags']] = row['singleTagCount']
            if row['Tags'] in tagSelect:
                selectNode.add(row['Tags'])
    
    
    for key,val in node_dict.items():
        G.add_node(key, size=val)
        maxNode_size = max(maxNode_size,val)
        
    for key,val in edge_dict.items():
        G.add_edge(key[0],key[1],weight=val,length=val/max(Edge['YearTagCount']))
        
    
    
    pos = nx.spring_layout(G, k=0.2*1/np.sqrt(len(G.nodes())), iterations=20)
    for node in G.nodes:
        G.nodes[node]['pos'] = list(pos[node])
        
    
    
    colors = list(Color('lightcoral').range_to(Color('darkred'), len(G.edges())))
    colors = ['rgb' + str(x.rgb) for x in colors]
    traceRecode = []  # contains edge_trace, node_trace, middle_node_trace
    traceRecode_select = [] # for select tag display
    index = 0
    for edge in G.edges:
        x0, y0 = G.nodes[edge[0]]['pos']
        x1, y1 = G.nodes[edge[1]]['pos']
        
        weight=G.edges[edge]['weight']/ max(Edge['YearTagCount'])*10
        trace = go.Scatter(x=tuple([x0, x1, None]), y=tuple([y0, y1, None]),
                           mode='lines',
                           line={'width': weight},
                           marker=dict(color=colors[index]),
                           text=[],
                           hovertext=[],
                           line_shape='spline',
                           opacity=0.6)
        
        trace['text']+= tuple([None])
        trace['hovertext']+= tuple([edge])
        traceRecode.append(trace)
        for e in edge:
            if e in selectEdge:
            
                trace_select = trace

            else:
                
                trace_select = go.Scatter(x=tuple([x0, x1, None]), y=tuple([y0, y1, None]),
                               mode='lines',
                               line={'width': weight},
                               marker=dict(color=colors[index]),
                               text=[],
                               hovertext=[],
                               line_shape='spline',
                               opacity=0)
            if trace_select not in traceRecode_select:
                traceRecode_select.append(trace_select)
        index = index + 1
       
    

    index = 0
    node_list = list(node_dict.items())
    for node in G.nodes():
        node_trace = go.Scatter(x=[], y=[],text=[], hovertext=[], mode='markers+text', textposition="bottom center",
                            hoverinfo="text", marker={'size': 100,'color': 'LightSkyBlue'})
        
        x, y = G.nodes[node]['pos']
        text = node_list[index][0]
        node_trace['x'] += tuple([x])
        node_trace['y'] += tuple([y])
        hovertext = text+'<br>'+'Post Num: '+str(G.nodes[node]['size'])
        node_trace['hovertext'] += tuple([hovertext])
        node_trace['marker']['size']=G.nodes[node]['size']/maxNode_size*50
        if G.nodes[node]['size']>int(filterNumber):
            node_trace['text'] += tuple([text])
        else:
            node_trace['text'] +=tuple([None])
        
        
        
        index = index + 1
        if node in selectNode:
            node_trace['marker']['color'] = 'GOLDENROD'
            trace_select = node_trace
            print("node",node,trace_select)
            
        else:
            #print("not in select node node",node)
            trace_select = go.Scatter(x=[], y=[],text=[], hovertext=[], mode='markers+text', textposition="bottom center",
                            hoverinfo="text", marker={'size': 100,'color': 'WHITESMOKE'})
            trace_select['x'] += tuple([x])
            trace_select['y'] += tuple([y])
            hovertext = text+'<br>'+'Post Num: '+str(G.nodes[node]['size'])
            trace_select['hovertext'] += tuple([hovertext])
            trace_select['marker']['size']=G.nodes[node]['size']/maxNode_size*50

        traceRecode.append(node_trace)
        traceRecode_select.append(trace_select)
    
    if len(selectEdge)!=0 or len(selectNode)!=0:
        print("select mode")
        
        figure = {
        "data": traceRecode_select,
        "layout": go.Layout( showlegend=False, hovermode='closest',
                            margin={'b': 100, 'l': 100, 'r': 100, 't': 100},
                            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
                            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
                            height=600,
                            clickmode='event+select',
                            )}
        return figure
 
    figure = {
        "data": traceRecode,
        "layout": go.Layout( showlegend=False, hovermode='closest',
                            margin={'b': 100, 'l': 100, 'r': 100, 't': 100},
                            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
                            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
                            height=600,
                            clickmode='event+select',
                            )}
    #print(figure)
    return figure
        
    
    
    

# styles: for right side hover/click component
styles = {
    'pre': {
        'border': 'thin lightgrey solid',
        'overflowX': 'scroll'
    }
}

app.layout = html.Div([
    #########################Title
    html.Div([html.H1("Stack Overflow Tag Network Graph")],
             className="row",
             style={'textAlign': "center"}),
    #############################################################################################define the row
    html.Div(
        className="row",
        children=[
            ##############################################left side two input components

           
            ############################################middle graph component
            html.Div(
                className="seven columns",
                children=[dcc.Graph(id="my-graph",
                                    figure=network_graph(YEAR,tagSelection ,filterNumber)),
                         
                         
                          
                          
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
                            ],
                            style={'height': '300px'}
                        )
                         
                         ],
            ),

            #########################################right side two output component
                        html.Div(
                className="two columns",
                children=[
                    dcc.Markdown(d("""
                            **Time Range **

                            Select Post Number filter & Year Range.
                            """)),
                     
                    html.Div(
                        className="two columns",
                        children=[
                        dcc.Input(
                            id="filterNum",
                            value=3000,
                            placeholder="input filter Number",

                        )]
                    ),
                    
                    html.Div(
                        className="two columns",
                        children=[
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
                            #html.Div(id='output-container-range-slider')
                        ],
                        style={'display': 'inline-block','height': '100px','width':'400px'}
                    ),
                    
                    html.Div(
                    className="two columns",
                    children=[
                        
                       dcc.Graph(id='tag-trend',figure=line_graph(tagSelection)),
                      
                    ],
                        
                   style = {'display': 'inline-block', 'width':'700px'}
                ),
                     
                ]
            )
        ]
    )
])

###################################callback for left side components
@app.callback(
    dash.dependencies.Output('my-graph', 'figure'),
    [dash.dependencies.Input('my-range-slider', 'value'),dash.dependencies.Input('tagSelect', 'value') ,dash.dependencies.Input('filterNum', 'value')])
def update_output(value,tagSelection,filterNum):
    YEAR = value
    filterNumber = filterNum
    tagSelect = tagSelection
    
    return network_graph(value,tagSelect, filterNumber)
    # to update the global variable of YEAR and ACCOUNT
# ################################callback for right side components
# @app.callback(
#     dash.dependencies.Output('hover-data', 'children'),
#     [dash.dependencies.Input('my-graph', 'hoverData')])
# def display_hover_data(hoverData):
#     return json.dumps(hoverData, indent=2)


# @app.callback(
#     dash.dependencies.Output('click-data', 'children'),
#     [dash.dependencies.Input('my-graph', 'clickData')])
# def display_click_data(clickData):
#     return json.dumps(clickData, indent=2)


@app.callback(
    dash.dependencies.Output('tagSelect', 'value'),
    [dash.dependencies.Input('my-graph', 'selectedData'),
    dash.dependencies.State('tagSelect', 'value')])
def update_options(selectedData, value):
    if selectedData and len(selectedData["points"])>0 and "hovertext" in selectedData["points"][0] and selectedData["points"][0]["hovertext"]:
        data = selectedData["points"][0]["hovertext"].split('<')[0]
        if data in value:
            value.remove(data)
        else:
            value.append(data)
           
    return value

@app.callback(
    dash.dependencies.Output('tag-trend', 'figure'),
    [dash.dependencies.Input('my-graph', 'selectedData'),
    dash.dependencies.State('tagSelect', 'value')])
def update_trend(selectedData, value):
    if selectedData and len(selectedData["points"])>0 and "hovertext" in selectedData["points"][0] and selectedData["points"][0]["hovertext"]:
        data = selectedData["points"][0]["hovertext"].split('<')[0]
        if data in value:
            value.remove(data)
        else:
            value.append(data)
           
    return line_graph(value)


# @app.callback(
#     dash.dependencies.Output('tag-trend', 'figure'),
#     [dash.dependencies.Input('tagSelect', 'value'),
#     dash.dependencies.State('my-graph', 'selectedData')])
# def update_trend_using_dropDown(value,selectedData):
# #     if selectedData and len(selectedData["points"])>0 and "hovertext" in selectedData["points"][0] and selectedData["points"][0]["hovertext"]:
# #         data = selectedData["points"][0]["hovertext"].split('<')[0]
# #         if data in value:
# #             value.remove(data)
# #         else:
# #             value.append(data)
           
#     return line_graph(value)
    
if __name__ == '__main__':
        #/home/ubuntu/Stack-Community/ETLPipeline/calculate.sh
    
    #network_graph(YEAR,FilterNumber)
    #app.run_server(debug=True)
    import socket
    host = socket.gethostbyname(socket.gethostname())
    print(host)
    app.run_server(debug=True, host=host, port = 4444)
