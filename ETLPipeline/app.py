#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import networkx as nx
import plotly.graph_objs as go

import pandas as pd
from colour import Color
from datetime import datetime
from textwrap import dedent as d
import json

import subprocess
import os
import math
import numpy as np

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
FilterNumber = 3000
ACCOUNT="A0001"

Edge = pd.read_csv('/home/ubuntu/Stack-Community/ETLPipeline/edge.csv')
Node = pd.read_csv('/home/ubuntu/Stack-Community/ETLPipeline/singleTagCount.csv')
nodeDict={}
for index, row in Node.iterrows():
    nodeDict[row['Tags']] = row['singleTagCount']

nodeOption = [
    {"label": str(node), "value": str(node)}
    for node in nodeDict.keys()
]




def network_graph(yearRange,FilterNumber=3000):
    """
    tag      count     
    [1]      500
    [1,2,3]  1234

    """
#     #/home/ubuntu/Stack-Community/ETLPipeline/calculate.sh
#     command = os.getcwd() +"/App/calculate.sh " + str(yearRange[0])+" "+str(yearRange[1])+" "+str(FilterNumber)
#     process_output = subprocess.call([command],shell=True)
#     print("after process")
    Edge = pd.read_csv('/home/ubuntu/Stack-Community/ETLPipeline/edge.csv')
    Node = pd.read_csv('/home/ubuntu/Stack-Community/ETLPipeline/singleTagCount.csv')
    
    node_option = []
    
    
    
    
    G= nx.MultiDiGraph()
    
    #G = nx.random_geometric_graph(200, 0.125) 
    node_dict = {}
    edge_dict ={}
    for index, row in Edge.iterrows():
        node_dict[row['Original_Tags']] = row['YearTagCount']
        edge_dict[(row['Original_Tags'],row['Tags'])] = row['YearTagCount']
        
    for index, row in Node.iterrows():
        node_dict[row['Tags']] = row['singleTagCount']
    maxNode_size = -float('inf')
    for key,val in node_dict.items():
        G.add_node(key, size=val)
        maxNode_size = max(maxNode_size,val)
    for key,val in edge_dict.items():
        G.add_edge(key[0],key[1],weight=val,length=val/max(Edge['YearTagCount']))
        
    
    #pos = nx.drawing.layout.random_layout(G)
    pos = nx.spring_layout(G, k=0.2*1/np.sqrt(len(G.nodes())), iterations=20)
    for node in G.nodes:
        G.nodes[node]['pos'] = list(pos[node])
        node_option.append({"label": str(node), "value": str(node)})
    
    
    colors = list(Color('lightcoral').range_to(Color('darkred'), len(G.edges())))
    colors = ['rgb' + str(x.rgb) for x in colors]
    traceRecode = []  # contains edge_trace, node_trace, middle_node_trace
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
        
        if G.nodes[node]['size']>FilterNumber*10:
            node_trace['text'] += tuple([text])
        else:
            node_trace['text'] +=tuple([None])
        #node_trace['text'] += tuple([text])
        
        node_trace['marker']['size']=G.nodes[node]['size']/maxNode_size*50
        print(node_trace['marker']['size'],G.nodes[node]['size'])
        index = index + 1

        traceRecode.append(node_trace)
    
    
    figure = {
        "data": traceRecode,
        "layout": go.Layout( showlegend=False, hovermode='closest',
                            margin={'b': 100, 'l': 100, 'r': 100, 't': 100},
                            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
                            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
                            height=600,
                            clickmode='event+select',
#                             annotations=[
#                                 dict(
#                                     ax=(G.nodes[edge[0]]['pos'][0] + G.nodes[edge[1]]['pos'][0]) / 2,
#                                     ay=(G.nodes[edge[0]]['pos'][1] + G.nodes[edge[1]]['pos'][1]) / 2, axref='x', ayref='y',
#                                     x=(G.nodes[edge[1]]['pos'][0] * 3 + G.nodes[edge[0]]['pos'][0]) / 4,
#                                     y=(G.nodes[edge[1]]['pos'][1] * 3 + G.nodes[edge[0]]['pos'][1]) / 4, xref='x', yref='y',
#                                     showarrow=False,
#                                     opacity=0.01
#                                 ) for edge in G.edges]
                            )}
    return figure
        
    
    
    
# ##############################################################################################################################################################
# def network_graph(yearRange, AccountToSearch):
#     #/home/ubuntu/Stack-Community/ETLPipeline/calculate.sh
#     command = os.getcwd() +"/App/calculate.sh " + str(yearRange[0])+" "+str(yearRange[1])+" "+str(FilterNumber)
#     process_output = subprocess.call([command],shell=True)
#     print("after process")

    

#     # filter the record by datetime, to enable interactive control through the input box
#     edge1['Datetime'] = "" # add empty Datetime column to edge1 dataframe
#     accountSet=set() # contain unique account
#     for index in range(0,len(edge1)):
#         edge1['Datetime'][index] = datetime.strptime(edge1['Date'][index], '%d/%m/%Y')
#         if edge1['Datetime'][index].year<yearRange[0] or edge1['Datetime'][index].year>yearRange[1]:
#             edge1.drop(axis=0, index=index, inplace=True)
#             continue
#         accountSet.add(edge1['Source'][index])
#         accountSet.add(edge1['Target'][index])

#     # to define the centric point of the networkx layout
#     shells=[]
#     shell1=[]
#     shell1.append(AccountToSearch)
#     shells.append(shell1)
#     shell2=[]
#     for ele in accountSet:
#         if ele!=AccountToSearch:
#             shell2.append(ele)
#     shells.append(shell2)


#     G = nx.from_pandas_edgelist(edge1, 'Source', 'Target', ['Source', 'Target', 'TransactionAmt', 'Date'], create_using=nx.MultiDiGraph())
#     nx.set_node_attributes(G, node1.set_index('Account')['CustomerName'].to_dict(), 'CustomerName')
#     nx.set_node_attributes(G, node1.set_index('Account')['Type'].to_dict(), 'Type')
#     # pos = nx.layout.spring_layout(G)
#     # pos = nx.layout.circular_layout(G)
#     # nx.layout.shell_layout only works for more than 3 nodes
#     if len(shell2)>1:
#         pos = nx.drawing.layout.shell_layout(G, shells)
#     else:
#         pos = nx.drawing.layout.spring_layout(G)
#     for node in G.nodes:
#         G.nodes[node]['pos'] = list(pos[node])


#     if len(shell2)==0:
#         traceRecode = []  # contains edge_trace, node_trace, middle_node_trace

#         node_trace = go.Scatter(x=tuple([1]), y=tuple([1]), text=tuple([str(AccountToSearch)]), textposition="bottom center",
#                                 mode='markers+text',
#                                 marker={'size': 50, 'color': 'LightSkyBlue'})
#         traceRecode.append(node_trace)

#         node_trace1 = go.Scatter(x=tuple([1]), y=tuple([1]),
#                                 mode='markers',
#                                 marker={'size': 50, 'color': 'LightSkyBlue'},
#                                 opacity=0)
#         traceRecode.append(node_trace1)

#         figure = {
#             "data": traceRecode,
#             "layout": go.Layout(title='Interactive Transaction Visualization', showlegend=False,
#                                 margin={'b': 40, 'l': 40, 'r': 40, 't': 40},
#                                 xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
#                                 yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
#                                 height=600
#                                 )}
#         return figure


#     traceRecode = []  # contains edge_trace, node_trace, middle_node_trace
#     ############################################################################################################################################################
#     colors = list(Color('lightcoral').range_to(Color('darkred'), len(G.edges())))
#     colors = ['rgb' + str(x.rgb) for x in colors]

#     index = 0
#     for edge in G.edges:
#         x0, y0 = G.nodes[edge[0]]['pos']
#         x1, y1 = G.nodes[edge[1]]['pos']
#         weight = float(G.edges[edge]['TransactionAmt']) / max(edge1['TransactionAmt']) * 10
#         trace = go.Scatter(x=tuple([x0, x1, None]), y=tuple([y0, y1, None]),
#                            mode='lines',
#                            line={'width': weight},
#                            marker=dict(color=colors[index]),
#                            line_shape='spline',
#                            opacity=1)
#         traceRecode.append(trace)
#         index = index + 1
#     ###############################################################################################################################################################
#     node_trace = go.Scatter(x=[], y=[], hovertext=[], text=[], mode='markers+text', textposition="bottom center",
#                             hoverinfo="text", marker={'size': 50, 'color': 'LightSkyBlue'})

#     index = 0
#     for node in G.nodes():
#         x, y = G.nodes[node]['pos']
#         hovertext = "CustomerName: " + str(G.nodes[node]['CustomerName']) + "<br>" + "AccountType: " + str(
#             G.nodes[node]['Type'])
#         text = node1['Account'][index]
#         node_trace['x'] += tuple([x])
#         node_trace['y'] += tuple([y])
#         node_trace['hovertext'] += tuple([hovertext])
#         node_trace['text'] += tuple([text])
#         index = index + 1

#     traceRecode.append(node_trace)
#     ################################################################################################################################################################
#     middle_hover_trace = go.Scatter(x=[], y=[], hovertext=[], mode='markers', hoverinfo="text",
#                                     marker={'size': 20, 'color': 'LightSkyBlue'},
#                                     opacity=0)

#     index = 0
#     for edge in G.edges:
#         x0, y0 = G.nodes[edge[0]]['pos']
#         x1, y1 = G.nodes[edge[1]]['pos']
#         hovertext = "From: " + str(G.edges[edge]['Source']) + "<br>" + "To: " + str(
#             G.edges[edge]['Target']) + "<br>" + "TransactionAmt: " + str(
#             G.edges[edge]['TransactionAmt']) + "<br>" + "TransactionDate: " + str(G.edges[edge]['Date'])
#         middle_hover_trace['x'] += tuple([(x0 + x1) / 2])
#         middle_hover_trace['y'] += tuple([(y0 + y1) / 2])
#         middle_hover_trace['hovertext'] += tuple([hovertext])
#         index = index + 1

#     traceRecode.append(middle_hover_trace)
#     #################################################################################################################################################################
#     figure = {
#         "data": traceRecode,
#         "layout": go.Layout( showlegend=False, hovermode='closest',
#                             margin={'b': 100, 'l': 100, 'r': 100, 't': 100},
#                             xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
#                             yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
#                             height=600,
#                             clickmode='event+select',
#                             annotations=[
#                                 dict(
#                                     ax=(G.nodes[edge[0]]['pos'][0] + G.nodes[edge[1]]['pos'][0]) / 2,
#                                     ay=(G.nodes[edge[0]]['pos'][1] + G.nodes[edge[1]]['pos'][1]) / 2, axref='x', ayref='y',
#                                     x=(G.nodes[edge[1]]['pos'][0] * 3 + G.nodes[edge[0]]['pos'][0]) / 4,
#                                     y=(G.nodes[edge[1]]['pos'][1] * 3 + G.nodes[edge[0]]['pos'][1]) / 4, xref='x', yref='y',
#                                     showarrow=True,
#                                     arrowhead=3,
#                                     arrowsize=4,
#                                     arrowwidth=1,
#                                     opacity=1
#                                 ) for edge in G.edges]
#                             )}
#     return figure
# ######################################################################################################################################################################
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
                className="eight columns",
                children=[dcc.Graph(id="my-graph",
                                    figure=network_graph(YEAR, FilterNumber)),
                         
                         
                          
                          
                          html.Div(
                            className="eight columns",
                            children=[
                                dcc.Markdown(d("""
                            **Tags To Search**
                            Input the tag to visualize.
                            """)),
                                
                              
                            dcc.Dropdown(
                            id="well_types",
                            options=nodeOption,
                            multi=True,
                            
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
                            **Time Range To Visualize**

                            Slide the bar to define year range.
                            """)),
                    html.Div(
                        className="twelve columns",
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
                            html.Div(id='output-container-range-slider')
                        ],
                        style={'height': '300px','width':'400px'}
                    ),
                    
                    html.Div(
                    [
                        
                        dcc.Graph(
                            id="Tag Count",
                            figure=dict(
                                layout=dict(
#                                     plot_bgcolor=app_color["graph_bg"],
#                                     paper_bgcolor=app_color["graph_bg"],
                                )
                            ),
                        ),
                        dcc.Interval(
                            id="wind-speed-update",
                            interval=100,
                            n_intervals=0,
                        ),
                    ],
                    className="two-thirds column wind__speed__container",
                )
                   
                ]
            )
        ]
    )
])

###################################callback for left side components
@app.callback(
    dash.dependencies.Output('my-graph', 'figure'),
    [dash.dependencies.Input('my-range-slider', 'value'), dash.dependencies.Input('input1', 'value')])
def update_output(value,input1):
    YEAR = value
    FilterNumber = input1
    #download()
    return network_graph(value, input1)
    # to update the global variable of YEAR and ACCOUNT
# ################################callback for right side components
@app.callback(
    dash.dependencies.Output('hover-data', 'children'),
    [dash.dependencies.Input('my-graph', 'hoverData')])
def display_hover_data(hoverData):
    return json.dumps(hoverData, indent=2)


@app.callback(
    dash.dependencies.Output('click-data', 'children'),
    [dash.dependencies.Input('my-graph', 'clickData')])
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)





if __name__ == '__main__':
        #/home/ubuntu/Stack-Community/ETLPipeline/calculate.sh
    
    #network_graph(YEAR,FilterNumber)
    #app.run_server(debug=True)
    import socket
    host = socket.gethostbyname(socket.gethostname())
    print(host)
    app.run_server(debug=False, host=host, port = 4444)
