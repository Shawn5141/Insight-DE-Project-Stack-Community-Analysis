import os
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from DataModel import * 
prev_tagSelection =[]
TagName2trendMap ={}
trendMap={}

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
   

def getAvgAnsweredTime(TagName):
    query = generateQueryForAnswersTime(TagName)
    df = readDataFromPSQL(query)
    df= list(map(lambda x: x[1]/3660,df))
    return round(sum(df)/len(df),2)
    
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
    combine_tagSelect_avgTime = []
    for idx,tagName in enumerate(tagSelect):
        AnsweredTime = getAvgAnsweredTime(tagName)
        combine_tagSelect_avgTime.append([tagName,AnsweredTime])
        if idx==0:
            df = getYearList2(tagName)
            
            df = df.rename(columns={"count": tagName+"_count &<br>Avg answer time:"+str(AnsweredTime)+" hrs"})
        else:
           
            tmp = getYearList2(tagName)
           
            tmp = tmp.rename(columns={"count": tagName+"_count &<br>Avg answer time:"+str(AnsweredTime)+" hrs"})
            df = df.join(tmp.set_index('Year'),lsuffix='_left', rsuffix='_right', on='Year')
        #print(idx,df.head())
    
    if not tagSelect:
        fig = go.Figure(data=[go.Scatter(x=[], y=[])])
        return fig
    y_val = [tag+"_count &<br>Avg answer time:"+str(AnsweredTime)+" hrs" for tag,AnsweredTime in combine_tagSelect_avgTime]
    fig = px.line(df, x="Year", y=y_val)
    #print("finish merging",y_val,"tag select",tagSelect)
    fig.update_traces(mode='markers+lines')
    fig.update_layout(margin={'l': 0, 'b': 50, 't': 10, 'r': 50}, hovermode='closest')
    fig.update_layout(legend_title_text='Tag Trend')
        
    TagName2trendMap[tuple(tagSelect)] = df
    prev_tagSelection = tagSelect
    trendMap[tuple(prev_tagSelection)] = fig
    
    return fig