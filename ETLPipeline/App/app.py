#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import networkx as nx
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import numpy as np
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
import psycopg2 as pg
import pandas.io.sql as psql
from DataModel import *
from configFile import *
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

conf = config()
param_dic = {
"host" : conf.host_ip,
"database" : conf.database,
"user" : conf.username,
"password" : conf.password
}


YEAR=[2010, 2019]
filterNumber = 3000
ActiveUserShowChosen = False
ACCOUNT="A0001"
tagSelection = []
prev_tagSelection =[]
# read from tagName
Edge = pd.read_csv('/home/ubuntu/Stack-Community/ETLPipeline/App/tmp/edge.csv')
Node = pd.read_csv('/home/ubuntu/Stack-Community/ETLPipeline/App/tmp/singleTagCount.csv')
nodeDict={}
trendMap={}
TagName2trendMap ={}
GolbalActiveUserMap = {}
prevGraph =None

for index, row in Node.iterrows():
    nodeDict[row['Tags']] = row['singleTagCount']

nodeOption = [
    {"label": str(node), "value": str(node)}
    for node in nodeDict.keys()
]
for index, row in Edge.iterrows():
    nodeOption.append( {"label": str(row['Original_Tags']), "value": str(row['Original_Tags'])})
    
    
#/home/ubuntu/Stack-Community/ETLPipeline/calculate.sh
#prefixsumyearcount
def generateQueryForYearTag(Tags):
    tags = ["'"+word+"'" for word in Tags.split(',')]
    tags = ','.join(tags)

    return  f'SELECT "Year", "count"\
              FROM yeartagcount\
              WHERE "Tags" = ARRAY[{tags}]::text[]\
              ORDER BY "Year"'
              


def getYearList(TagName):
    # generate spark sql command to parse parqeut from s3
    global TagName2trendMap
    if TagName in TagName2trendMap:
        return TagName2trendMap[TagName]
    pid = os.getpid()
    fileName = '_'.join([e for e in TagName.split(',')])
    path = os.getcwd()+'/tmp'+"/"+fileName+"_yearTagCount.csv"
    if not os.path.isfile(path):
        command = os.getcwd() +"/calculate.sh"+" --method="+str(2)+" --path="+path+" --list="+TagName
        process_output = subprocess.call([command],shell=True)
    return pd.read_csv(path)

def getYearList2(TagName):
    # directly manipulate with database
    global TagName2trendMap
    if TagName in TagName2trendMap:
        return TagName2trendMap[TagName]
    query = generateQueryForYearTag(TagName)
    df = readDataFromPSQL(query)
    tmp = {}
    for e in df:
        tmp[e[0]] = e[1]
    
    return pd.DataFrame(tmp.items(), columns=['Year', 'count'])
   

def line_graph(tagSelect):
    global trendMap
    global prev_tagSelection
    
    
    
    if not prev_tagSelection and not tagSelect:
        #print("no value",prev_tagSelection,tagSelect)
        return go.Figure(data=[go.Scatter(x=[], y=[])])
    if prev_tagSelection==tagSelect:
        #print("prev equal to curr",prev_tagSelection)
        return trendMap[tuple(prev_tagSelection)]
#     print("curr",tagSelect)
#     print("where are you")
    for idx,tagName in enumerate(tagSelect):
        if idx==0:
            df = getYearList2(tagName)
            #df = df.drop(columns=['Tags','Unnamed: 0'])
            #df = df.drop(columns=['Tags'])
            df = df.rename(columns={"count": tagName+"_count"})
        else:
            #print(idx,"following df",df.head())
            tmp = getYearList2(tagName)
            #tmp = tmp.drop(columns=['Tags','Unnamed: 0'])
            #tmp = tmp.drop(columns=['Tags'])
            tmp = tmp.rename(columns={"count": tagName+"_count"})
            df = df.join(tmp.set_index('Year'),lsuffix='_left', rsuffix='_right', on='Year')
        #print(idx,df.head())
    
    if not tagSelect:
        fig = go.Figure(data=[go.Scatter(x=[], y=[])])
        return fig
    y_val = [tag+"_count" for tag in tagSelect]
    fig = px.line(df, x="Year", y=y_val)
    #print("finish merging",y_val,"tag select",tagSelect)
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

def getNodeAndEdgeInPandas(yearRange):
        #global previous_year
        global yearMap
        global edgeMap
        global nodeMap
        yearRange= tuple(yearRange)
        if yearRange not in yearMap:
            #print("not in yearMap",yearRange)
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
        return Edge,Node
    


def generateQueryWithTag(Tags,yearRange):
    #print("generate Query",Tags,yearRange)
    BeginYear,EndYear =  yearRange[0],yearRange[1]
    tags = ["'"+word+"'" for word in Tags.split(',')]
    tags = ','.join(tags)

    return  f'SELECT "DisplayName" ,"UserId",SUM("User_count_per_tag")::float\
             FROM (SELECT *\
              FROM activeuserstable\
              WHERE "Year" BETWEEN {BeginYear} AND {EndYear} AND "Tags" = ARRAY[{tags}]::text[]) AS F\
             GROUP BY "DisplayName","UserId"\
             ORDER BY SUM("User_count_per_tag") DESC\
             limit 3'

def generateQueryWithUser(UserId,yearRange):
    #print("generate Query",Tags,yearRange)
    BeginYear,EndYear =  yearRange[0],yearRange[1]
#     tags = ["'"+word+"'" for word in Tags.split(',')]
#     tags = ','.join(tags)

    return  f'SELECT "Tags",SUM("User_count_per_tag")::float\
             FROM (SELECT *\
              FROM activeuserstable\
              WHERE "Year" BETWEEN {BeginYear} AND {EndYear} AND "UserId"={UserId} ) AS F\
             GROUP BY "Tags"\
             ORDER BY SUM("User_count_per_tag") DESC\
             limit 3'

def readDataFromPSQL(query):
#     conf = config()
#     print(conf.password)
#     connection = pg.connect(f'host={conf.host_ip}, dbname={conf.database}, user={conf.username}, password={conf.password}')
#     dataframe = psql.read_sql(cmd, connection)
#     print(dataframe.head())
    
    #print("read from database",query)
    conn = connectWithDatabase(param_dic)
    cursor = conn.cursor()
    return getInsertData(cursor,query)


def getUserIfTagSelect(yearRange,tagSelect,ActiveUserShowChosen=False):
    global GolbalActiveUserMap 
    LocalActiveUserMap = {}
    
    #print("Inside getUserIfTagSelect",ActiveUserShowChosen,yearRange,tagSelect)
    if ActiveUserShowChosen==True:
        #print("inside for loop to query for user",ActiveUserShowChosen)
        for tag in tagSelect:
            if (tag,yearRange[0],yearRange[0]) in GolbalActiveUserMap:
                LocalActiveUserMap[tag] = GolbalActiveUserMap[(tag,yearRange[0],yearRange[0])]
                continue
            query = generateQueryWithTag(tag,yearRange)
            #print("query in getUserIfTagSelect",query)
            activeUsers = readDataFromPSQL(query)
            GolbalActiveUserMap[(tag,yearRange[0],yearRange[0])] = activeUsers
            LocalActiveUserMap[tag] = GolbalActiveUserMap[(tag,yearRange[0],yearRange[0])]
            
    
    return LocalActiveUserMap

def filterNodeAndEdge(yearRange,filterNumber,tagSelect,Node,Edge,LocalActiveUserMap):
    #print("LocalActiveUserMap",LocalActiveUserMap)
    selectEdge = set()
    selectNode = set()
    total_node_dict = {}
    node_dict = {}
    edge_dict ={}
    
    
    for index, row in Edge.iterrows():
        total_node_dict[row['Original_Tags']] = row['YearTagCount']
        if row['YearTagCount']>int(filterNumber):
            
            node_dict[row['Original_Tags']] = row['YearTagCount']
            
            edge_dict[(row['Original_Tags'],row['Tags'])] = row['YearTagCount']
            if row['Tags'] in tagSelect or row['Original_Tags'] in tagSelect :
                selectEdge.add(row['Original_Tags'])
                selectNode.add(row['Tags'])
                
                for tag in row['Original_Tags'].split(','):
                    selectNode.add(tag)
    for index, row in Node.iterrows():
        total_node_dict[row['Tags']] = row['singleTagCount']
        if row['singleTagCount']>int(filterNumber):
            
            node_dict[row['Tags']] = row['singleTagCount']
            if row['Tags'] in tagSelect:
                selectNode.add(row['Tags'])

    userDataMap ={}
    tag_node_dict ={} # create node based on user name and tag not appear before
    tag_edge_dict ={} # create edge between user and tags
    UserOrderForThatTagMap ={}
    for key,val in LocalActiveUserMap.items():
        for i,userData in enumerate(val):
            userName,userID,TagCount = userData
            #print("\n",userName,userID,TagCount)
            query = generateQueryWithUser(userID,yearRange)
            data = readDataFromPSQL(query)
            userDataMap[userData] = data
            #print(data,"\n")
            #tag_node_dict[userName] = data
            node_dict[userName] = TagCount
            total_node_dict[userName] =TagCount
            UserOrderForThatTagMap[userName] = i+1
            for d in data:
                node_str = ','.join(d[0])
                if node_str not in total_node_dict:
                    node_dict[node_str] = d[1]
                    total_node_dict[node_str] = d[1]
                
                edge_dict[(userName,node_str)] =d[1]
                tag_edge_dict[(userName,node_str)] =d[1]
                if userName not in tag_node_dict:
                    tag_node_dict[userName] = []
                tag_node_dict[userName].append([node_str,d[1]])
               
                
#     print("tag_node_dict",tag_node_dict,"\n")
#     print("tag_edge_dict",tag_edge_dict)
    


    
    return node_dict,edge_dict,selectEdge,selectNode,tag_node_dict,tag_edge_dict,total_node_dict,UserOrderForThatTagMap


    


def createGraph(node_dict,edge_dict,tag_node_dict,tag_edge_dict):
        
    G= nx.MultiDiGraph()
    maxNode_size = -float('inf')    
    for key,val in node_dict.items():
        G.add_node(key, size=val)
        maxNode_size = max(maxNode_size,val)

    for key,val in edge_dict.items():
        G.add_edge(key[0],key[1],weight=val,length=val/max(Edge['YearTagCount']))
        
    

    pos = nx.spring_layout(G, k=0.2*1/np.sqrt(len(G.nodes())), iterations=20)
    for node in G.nodes:
        G.nodes[node]['pos'] = list(pos[node])
    return G,pos,maxNode_size

def network_graph(yearRange,tagSelect,filterNumber=3000,ActiveUserShowChosen=False):
    global prevGraph
    try:
        test=int(filterNumber)
    except:
        
        return prevGraph
    #print("network begin",yearRange,tagSelect,filterNumber)
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
    
    #Generate node based on year select in pandas format
    Edge,Node = getNodeAndEdgeInPandas(yearRange)
    #print("before getUserIfTagSelect")
    LocalActiveUserMap = getUserIfTagSelect(yearRange,tagSelect,ActiveUserShowChosen)
    #add selected node and edge into node dict and edge dict
    #print("before filterNodeAndEdge",LocalActiveUserMap)
    node_dict,edge_dict,selectEdge,selectNode,tag_node_dict,tag_edge_dict,total_node_dict,UserOrderForThatTagMap = filterNodeAndEdge(yearRange,filterNumber,tagSelect,Node,Edge,LocalActiveUserMap)
    #create graph based on dictionary
    G,pos,maxNode_size = createGraph(node_dict,edge_dict,tag_node_dict,tag_edge_dict)
    colors = list(Color('lightcoral').range_to(Color('darkred'), max(len(G.edges()),1)))
    colors = ['rgb' + str(x.rgb) for x in colors]
    traceRecode = []  # contains edge_trace, node_trace, middle_node_trace
    traceRecode_select = [] # for select tag display
    
    #def generateEdge(G,Edge,colors,traceRecode,traceRecode_select,selectEdge):


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
        
        _edge = (edge[0],edge[1])
        if _edge in tag_edge_dict:
            trace['line']['width'] *=50
            trace['marker'] = dict(color='PERU')
            traceRecode_select.append(trace)
        for e in edge:
            _edge = (edge[0],edge[1])
            if _edge in tag_edge_dict:
                continue
            
            if e in selectEdge :
                trace_select = trace
                if e in tagSelect:
                    selectNode.add(e) # middle node add name
            
            else:

                trace_select = go.Scatter(x=tuple([x0, x1, None]), y=tuple([y0, y1, None]),
                               mode='lines',
                               line={'width': weight},
                               marker=dict(color='LIGHTCORAL'),
                               text=[],
                               hovertext=[],
                               line_shape='spline',
                               opacity=0)
            if trace_select not in traceRecode_select:
                traceRecode_select.append(trace_select)
        index = index + 1
            #return traceRecode,traceRecode_select
    #traceRecode,traceRecode_select= generateEdge(G,Edge,colors,traceRecode,traceRecode_select,selectEdge)

    index = 0
    
    node_list = list(node_dict.items())
    for node in G.nodes():
        #print(node,node_dict,node_list)
        node_trace = go.Scatter(x=[], y=[],text=[], hovertext=[], mode='markers+text', textposition="bottom center",
                            hoverinfo="text", marker={'size': 100,'color': 'LightSkyBlue'})
        #print("create new trace",node_trace)
        x, y = G.nodes[node]['pos']
        text = node
        node_trace['x'] += tuple([x])
        node_trace['y'] += tuple([y])
        if 'size' not in G.nodes[node]:
            G.nodes[node]['size'] = total_node_dict[node]
            #print(node,G.nodes[node])
        hovertext = node+'<br>'+'Post Num: '+str(G.nodes[node]['size'])
        node_trace['hovertext'] += tuple([hovertext])
        
        node_trace['marker']['size']=G.nodes[node]['size']/maxNode_size*50
        if G.nodes[node]['size']>int(filterNumber) or node in tagSelect:
            node_trace['text'] += tuple([text])
        else:
            node_trace['text'] +=tuple([None])
        
        
        
        index = index + 1
        if node in selectNode or node in tag_node_dict :
            node_trace['text'] += tuple([text])
            #print("node",node,"tag_node_dict",tag_node_dict,"node trace ",node_trace['text'])
            if node in tagSelect:
                node_trace['marker']['color'] = 'GOLDENROD'
                
            elif node in tag_node_dict:
                node_trace['marker']['color'] = 'YELLOWGREEN'
                node_trace['marker']['size'] *= 100
                if node_trace['marker']['size']>10:
                    node_trace['marker']['size'] = 10
                    
#                 node_trace['marker']['size'] = max(30,node_trace['marker']['size'])
                #print("user ",tag_node_dict[node])
                hovertext = text+"  Top"+str(UserOrderForThatTagMap[text])+'<br>'
                interval = 0
                for i,(key,val) in enumerate(tag_node_dict[node]):
                    val = str(int(val))
                    interval= max(interval,len(key)+len(val))
                for i,(key,val) in enumerate(tag_node_dict[node]):
                    val = str(int(val))
                    space = ' '*(interval-len(key)-len(val))
                    hovertext += 'tag'+str(i+1)+": "+key+space+'  | Num: '+val+'<br>'
                
                node_trace['hovertext'] = tuple([hovertext])
                #print(node,"====",node_trace['hovertext'])
            else:
                node_trace['marker']['color'] ='LightSkyBlue'
            trace_select = node_trace
            #print("node",node,trace_select)
       
        else:
            #print("not in select node node",node)
            trace_select = go.Scatter(x=[], y=[],text=[], hovertext=[], mode='markers+text', textposition="bottom center",
                            hoverinfo="text", marker={'size': 100,'color': 'WHITESMOKE'})
            trace_select['x'] += tuple([x])
            trace_select['y'] += tuple([y])
            #print(node,G.nodes[node])
            
            hovertext = text+'<br>'+'Post Num: '+str(G.nodes[node]['size'])
            trace_select['hovertext'] += tuple([hovertext])
            trace_select['marker']['size']=G.nodes[node]['size']/maxNode_size*50

        traceRecode.append(node_trace)
        traceRecode_select.append(trace_select)
    
    if len(selectEdge)!=0 or len(selectNode)!=0:
        #print("select mode")
        
        figure = {
        "data": traceRecode_select,
        "layout": go.Layout( showlegend=False, hovermode='closest',
                            margin={'b': 100, 'l': 100, 'r': 100, 't': 100},
                            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
                            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
                            height=600,
                            clickmode='event+select',
                            )}
        prevGraph = figure
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
    prevGraph = figure
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
                className="six columns",
                children=[
                    dcc.Graph(id="my-graph",
                                    figure=network_graph(YEAR,tagSelection ,filterNumber,ActiveUserShowChosen)), 
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
                            style={'height': '100px'}
                        ),
                    html.Br(),
                    dcc.RadioItems(
                        options=[
                                    {'label': 'Show No Active Users', 'value': 'False'},
                                    {'label': 'Show Active Users', 'value': 'True'},

                                ],
                                value='False',
                                labelStyle={'display': 'inline-block'},
                                id="ActiveUser_radioitems",
                    ) ,
#                     html.Div(
#                         className="six columns",
#                          children=[
#                             html.Pre(id='hover-data1', style=styles['pre']),
#                          ],style={'height': '200px','width':'200px'}
                        
#                     ),
                    
                   ],
                 
            ),
           

            #########################################right side two output component
                        html.Div(
                className="four columns",
                children=[
                    dcc.Markdown(d("""
                            **Time Range **

                            Select Post Number filter & Year Range.
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





###################################callback for left side components
@app.callback(
    dash.dependencies.Output('my-graph', 'figure'),
    [dash.dependencies.Input('my-range-slider', 'value'),
     dash.dependencies.Input('tagSelect', 'value') ,
     dash.dependencies.Input('filterNum', 'value'),
     dash.dependencies.Input('ActiveUser_radioitems', 'value')],
    [dash.dependencies.State('my-graph', 'figure')])
def update_output(value,tagSelection,filterNum,ActiveUserShowChosen,fig_state):
    #print("ActiveUserShowChosen",ActiveUserShowChosen)
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
      
    #network_graph(YEAR,FilterNumber)
    #app.run_server(debug=True)
    import socket
    host = socket.gethostbyname(socket.gethostname())
    app.run_server(debug=True, host=host, port = 4444)

