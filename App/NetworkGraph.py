import networkx as nx
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import numpy as np
from colour import Color
from DataModel import *

previous_year = None
yearMap = set()
edgeMap = {}
nodeMap = {}
GolbalActiveUserMap = {}
prevGraph =None 
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
            path = "/home/ubuntu/Stack-Community/App/tmp/"
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
    
def getUserIfTagSelect(yearRange,tagSelect,ActiveUserShowChosen=False):
    global GolbalActiveUserMap 
    LocalActiveUserMap = {}
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
            query = generateQueryWithUser(userID,yearRange)
            data = readDataFromPSQL(query)
            userDataMap[userData] = data
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
               
                

    return node_dict,edge_dict,selectEdge,selectNode,tag_node_dict,tag_edge_dict,total_node_dict,UserOrderForThatTagMap


    


def createGraph(node_dict,edge_dict,tag_node_dict,tag_edge_dict,Edge):
        
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
   
    
    #Generate node based on year select in pandas format
    Edge,Node = getNodeAndEdgeInPandas(yearRange)
    
    LocalActiveUserMap = getUserIfTagSelect(yearRange,tagSelect,ActiveUserShowChosen)
    #add selected node and edge into node dict and edge dict
    node_dict,edge_dict,selectEdge,selectNode,tag_node_dict,tag_edge_dict,total_node_dict,UserOrderForThatTagMap = filterNodeAndEdge(yearRange,filterNumber,tagSelect,Node,Edge,LocalActiveUserMap)
    #create graph based on dictionary
    G,pos,maxNode_size = createGraph(node_dict,edge_dict,tag_node_dict,tag_edge_dict,Edge)
    
    
    colors = list(Color('lightcoral').range_to(Color('darkred'), max(len(G.edges()),1)))
    colors = ['rgb' + str(x.rgb) for x in colors]
    
    traceRecode = []  # contains edge_trace, node_trace, middle_node_trace
    traceRecode_select = [] # for select tag display
    
    def createEdgeTrace(traceRecode,traceRecode_select,colors):
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
        return traceRecode,traceRecode_select
    traceRecode,traceRecode_select = createEdgeTrace(traceRecode,traceRecode_select,colors)
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
        
