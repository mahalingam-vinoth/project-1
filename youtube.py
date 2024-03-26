from googleapiclient.discovery import build  
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

#API Key connection 

def apic():  
    api_key='AIzaSyDkrxhqtwNGJHqh7MrCwaochguNOqyGUa8'
    api_service_name='youtube'
    api_version='v3'
    youtube=build(api_service_name,api_version,developerKey = api_key)
    return youtube
youtube=apic()

#channel details 
def channel_info(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    for i in response["items"]:
           data=dict(channelname=i["snippet"]["title"],channelid=["id"],subscribers=i["statistics"]["subscriberCount"],
           views=i["statistics"]["viewCount"],Totlavideo=i["statistics"]["videoCount"],
           description=i["snippet"]["description"],playlistid=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#vide details
def get_videos_id(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id, part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(part='snippet',playlistId=Playlist_Id,maxResults=50,pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids        

#video info
def get_video_details(videos_ids):
    video_data=[]
    for video_id in videos_ids:
        request=youtube.videos().list(part='snippet,ContentDetails,statistics',id=video_id)
        response=request.execute()
        for item in response['items']:
            data=dict(channel_name=item['snippet']['channelTitle'],channel_id=item['snippet']['channelId'],
                      video_id=item['id'],Title=item['snippet']['title'],Tags=item['snippet'].get('tags'),
                      Thumbnail=item['snippet']['thumbnails']['default']['url'],
                      Description=item['snippet'].get('description'),published_date=item['snippet']['publishedAt'],
                      Duration=item['contentDetails']['duration'],
                      views=item['statistics'].get('viewCount'),Likes=item['statistics'].get('likeCount'),
                      comments=item['statistics'].get('commentCount'))
            video_data.append(data)
    return video_data

#get the comments info:-

def get_comment_info(videos_ids):
    comment_data=[]
    try:
        
        for video_id in videos_ids:
            request=youtube.commentThreads().list(part='snippet',videoId=video_id,maxResults=50)
            response=request.execute()


            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                comment_data.append(data)
    except:

        pass

    return comment_data 


#fetch the data on mongo db
client=pymongo.MongoClient('mongodb+srv://mahivino98:mahalingam@cluster0.dnbxmse.mongodb.net/')
db=client['youtube_data']

#insert into mongo db data base

def channel_details(channel_id):
    ch_details=channel_info(channel_id)
    vi_ids=get_videos_id(channel_id)
    vi_details=get_video_details(videos_ids)
    com_details=get_comment_info(videos_ids)


    coll1=db['channel_details']
    coll1.insert_one({"channel_information":ch_details,"video_information":vi_details,"comment_information":com_details})

    return "uploaded completed successfully"

# channel sql table

import mysql.connector

def channels_table():
    mydb=mysql.connector.connect(host='localhost',user='root',password="12345678",database='youtube_data',port='3306')
    cursor=mydb.cursor()

    drop_query="""drop table if exists channels"""
    cursor.execute(drop_query)
    mydb.commit()


    try:
        create_query="""create table if not exists channels(Channel_Name varchar(100),Channel_ID varchar(80) primary key,
                            Subscribers bigint,Views bigint,Total_Videos int,Channel_Description text,Playlist_Id varchar(80))"""
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("channels table already created")

    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query="""insert into channels(Channel_Name,Channel_ID,Subscribers,
                                                Views,Total_Videos,Channel_Description,Playlist_id)
                                                values(%s,%s,%s,%s,%s,%s,%s)"""
        values=(row['channelname'],
                row['channelid'],
                row['subscribers'],
                row['views'],
                row['Totlavideo'],
                row['description'],
                row['playlistid'])    
        try:
        
            cursor.execute(insert_query,values)
            mydb.commmit()
        except:
            print("channels values are already inserted")    



#Sql video table


def videos_table():
    mydb=mysql.connector.connect(host='localhost',user='root',password="12345678",database='youtube_data',port='3306')
    cursor=mydb.cursor()

    create_query="""create table if not exists videos(Channel_Name varchar(100),Channel_ID varchar(80),Video_Id varchar(50),
                                                                Title text,Tags text,Thumbnail varchar(200),Description text,Published_Date timestamp,
                                                                Duration tinyint,Views bigint,Likes bigint,comments int)"""
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["youtube_data"]
    coll1=  db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df1=pd.DataFrame(vi_list)



    for index,row in df1.iterrows():
            insert_query="""insert into videos(Channel_Name,Channel_ID,Video_Id,Title,Tags,Thumbnail,Description,Published_Date,Duration,Views,Likes,comments)
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            values=(row['channel_name'],
                    row['channel_id'],
                    row['video_id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['published_date'],
                    row['Duration'],
                    row['views'],
                    row['Likes'],
                    row['comments'])         
            cursor.execute(insert_query,values)
            mydb.commmit()


#Sql Comment table

def comments_table():
       
    mydb=mysql.connector.connect(host='localhost',user='root',password="12345678",database='youtube_data',port='3306')
    cursor=mydb.cursor()

    create_query="""create table if not exists comments(comment_id varchar(100) primary key,video_id varchar(50),comment_Text text,comment_author varchar(150),
                                                        comment_published timestamp)"""
    cursor.execute(create_query)
    mydb.commit()


    com_list=[]
    db=client["youtube_data"]
    coll1=  db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df2=pd.DataFrame(com_list)


    for index,row in df2.iterrows():
                insert_query="""insert into comments(comment_id,
                                                    video_id,
                                                    comment_Text,
                                                    comment_author,
                                                    comment_published)
                                                        
                                                    values(%s,%s,%s,%s,%s)"""


                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'])    
        
                cursor.execute(insert_query,values)
                mydb.commmit()

def tables():
    channels_table()
    videos_table()
    comments_table()

    return "table created successfully"


def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_video_table():
    vi_list=[]
    db=client["youtube_data"]
    coll1=  db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df1=st.dataframe(vi_list)

    return df1


def show_comment_table():

    com_list=[]
    db=client["youtube_data"]
    coll1=  db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df2=st.dataframe(com_list)

    return df2

# Streamlit code

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING]")
    st.header("skill known with this")
    st.caption("python coding")
    st.caption("Data information")
    st.caption("MongoDB")
    st.caption("Api integration")
    st.caption("Data Management using MongoDB and MySql")

    channel_id=st.text_input("Enter the the channel id")

    if st.button("collect and store data"):
        ch_ids=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["channelid"])

        if channel_id in ch_ids:
            st.success("channel Details for the given channel_id already been exist")

        else:

            insert=channel_details(channel_id)
            st.success(insert)

    if st.button("Migrate to sql"):
        Table=tables()
        st.success(Table)

    show_table=st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS","VIDEOS","COMMENTS"))

    if show_table=="CHANNELS":
        show_channels_table()

    elif show_table=="VIDEOS":
        show_videos_table()

    elif show_table=="COMMENTS":
        show_comments_table()

    
#SQL Connection:

mydb=mysql.connector.connect(host='localhost',user='root',password="12345678",database='youtube_data',port='3306')

cursor=mydb.cursor()

question=st.selctbox("Select your question", ("1.All the videos and the channel name",
                                              "2.channels with the most number of videos",
                                              "3.10 most viewed videos",
                                              "4.comments in each videos ",
                                              "5.Videos with highest likes",
                                              "6.likes of all videos",
                                              "7.views of each channel",
                                              "8.videos published in the year of 2022",
                                              "9.average duration of all videos in each channel ",
                                              "10.videos with highest number of comments"))

                       

#SQL Connection:

mydb=mysql.connector.connect(host='localhost',user='root',password="12345678",database='youtube_data',port='3306')

cursor=mydb.cursor()

question=st.selectbox("Select your question", ("1.All the videos and the channel name",
                                              "2.channels with the most number of videos",
                                              "3.10 most viewed videos",
                                              "4.comments in each videos ",
                                              "5.Videos with highest likes",
                                              "6.likes of all videos",
                                              "7.views of each channel",
                                              "8.videos published in the year of 2022",
                                              "9.average duration of all videos in each channel ",
                                              "10.videos with highest number of comments"))

if question=="1.All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos '''

    cursor.execute(query1)
    t1=cursor.fetchall()
    mydb.commit()


    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2.channels with the most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc '''

    cursor.execute(query2)
    t2=cursor.fetchall()
    mydb.commit()


    df1=pd.DataFrame(t2,columns=["channel_name","No of videos"])
    st.write(df1)

elif question=="3.10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos where views is not null
                order by views desc limit 10 '''

    cursor.execute(query3)
    t3=cursor.fetchall()
    mydb.commit()


    df2=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df2) 

elif question=="4.comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''

    cursor.execute(query4)
    t4=cursor.fetchall()
    mydb.commit()


    df3=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df3)


elif question=="5.Videos with highest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount from videos 
                where likes is not null order by likes desc'''

    cursor.execute(query5)
    t5=cursor.fetchall()
    mydb.commit()


    df4=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df4)

elif question=="6.likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''

    cursor.execute(query6)
    t6=cursor.fetchall()
    mydb.commit()


    df5=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df5)

elif question=="7.views of each channel":
    query7='''select channel_name as channelname,views as totalviews from channels '''

    cursor.execute(query7)
    t7=cursor.fetchall()
    mydb.commit()


    df6=pd.DataFrame(t7,columns=["likecount","videotitle"])
    st.write(df6)


elif question=="8.videos published in the year of 2022":
    query8='''select title as video_title,published_date, as videorelease,channel_name as channelname from videos where extract 
                (year from published_data)=2022 '''

    cursor.execute(query8)
    t8=cursor.fetchall()
    mydb.commit()


    df7=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df7)

elif question=="9.average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name '''

    cursor.execute(query9)
    t9=cursor.fetchall()
    mydb.commit()


    df8=pd.DataFrame(t9,columns=["channelname","averageduration"])
    df8

    T9=[]

    for index,row in df8.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=chennel_title,avgduration=average_duration_str))
    df9=pd.DataFrame(T9)
    st.write(df9)


elif question=="10.videos with highest number of comments":
    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos comments not null order 
                by comments desc '''

    cursor.execute(query10)
    t10=cursor.fetchall()
    mydb.commit()


    df10=pd.DataFrame(t10,columns=["channelname","averageduration"])
    st.write(df10)
        