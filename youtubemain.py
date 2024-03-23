import googleapiclient.discovery
import googleapiclient.errors
import pymongo
import psycopg2
import pandas as pd
from googleapiclient import * 

client=pymongo.MongoClient('mongodb+srv://cgun22:datasci2324@clustercgun.ytbuprn.mongodb.net/?retryWrites=true&w=majority&appName=ClusterCGUN')
## Variable declerations 

apikey='AIzaSyD1u60jt5KzGyp5KhMj596hV4HdXEuqadw'

api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=apikey)

## Getting channel details

def ChannelInfo(channel_id):
    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
    )
    response=request.execute()
    
    for i in response["items"]:
        Showdetails=dict(ChannelName=i["snippet"]["title"],
        ChannelID=i["id"],
        Subscribers=i["statistics"]['subscriberCount'],
        Views=i["statistics"]['viewCount'],
        totalvideos=i["statistics"]['videoCount'],
        ChannelDescription=i["snippet"]['description'],
        playlistID=i['contentDetails']['relatedPlaylists']['uploads']
        )
    return Showdetails
        
# program to get video ID

try:
    def video_ID(channel_id):
        Video_IDs=[]
        request1=youtube.channels().list(
                part="contentDetails",
                id=channel_id
            ).execute()
    
        PlaylistID=request1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        NextPageToken=None
        
        while True:
            response1=youtube.playlistItems().list(
                part='snippet',
                playlistId=PlaylistID,
                maxResults=50,
                pageToken=NextPageToken).execute()

            for i in range(len(response1['items'])):
                Video_IDs.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            NextPageToken=response1.get('nextPageToken')
            
            if NextPageToken is None:
                break    
        return Video_IDs
except:
    pass

#to get the video details

def VideoInfo(Video_Ids):
    
    v_details=[]
    for j in Video_Ids:
        request=youtube.videos().list(
            part="statistics,snippet,contentDetails",
            id=j
        )
        response=request.execute()
    
        for i in response["items"]:
            video_details=dict(VideoName=i['snippet']['title'],
                                VideoID=i["id"],
                                ChannelName=i['snippet']['channelTitle'],
                                ChannelID=i['snippet']['channelId'],
                                VideoDescription=i['snippet']['description'],
                                Tags=i['etag'],
                                PublishedAt=i['snippet']['publishedAt'],
                                ViewCount=i['statistics']['viewCount'],
                                CommentCount=i['statistics'].get('commentCount'),
                                LikeCount=i['statistics'].get('likeCount'),
                                FavouriteCount=i['statistics']['favoriteCount'],
                                Duration=i['contentDetails']['duration'],
                                Thumnails=i['snippet']['thumbnails']['default']['url']
                                )
            v_details.append(video_details)
    return v_details
        
# # #function to get comment details

def CommentInfo(Video_Ids):
    c_details=[]
    try:
        for j in Video_Ids:
            request=youtube.commentThreads().list(
                            part="id,snippet",
                            videoId=j,
                            maxResults=5
                        )
            response=request.execute()

            for i in response['items']:
                comment_details=dict(CommentId=i['snippet']['topLevelComment']['id'],
                                VideoId=i['snippet']['topLevelComment']['snippet']['videoId'],
                                ChannelID=i['snippet']['channelId'],
                                CommentText=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                CommentAuthor=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                CommentPublished=i['snippet']['topLevelComment']['snippet']['publishedAt']                            
                        )
                c_details.append(comment_details)
        print(c_details)

    except:
        pass    
    return c_details

# #code for playlist
def PlaylistInfo(channel_id):

        request=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id
        )
        response=request.execute()

        for i in response['items']:
                PlaylistDetails=dict(PlaylistID=i['id'],
                                Title=i['snippet']['title'],
                                ChannelID=i['snippet']['channelId'],
                                ChannelName=i['snippet']['channelTitle'],
                                PublishedAt=i['snippet']['publishedAt'],
                                Videocount=i['contentDetails']['itemCount']       
                )
        return PlaylistDetails

# # ### Loading data into mongo db


def Channeldetails(channel_id):
        channel_details=ChannelInfo(str(channel_id))
        print("channel details got uploaded successfully")
        playlist_details=PlaylistInfo(channel_id)
        print("playlist details got uploaded successfully")
        videoid_details=video_ID(channel_id)
        Video_Details=VideoInfo(videoid_details)
        print("video details got uploaded successfully")
        Comment_Details=CommentInfo(videoid_details)
        print("comments details got uploaded successfully")

        db=client['Youtube_db']
        collection=db['Youtubechannels']
        collection.insert_one({"Channel_information":channel_details,"Playlist_information":playlist_details,
                        "Video_information":Video_Details,"Comment_information":Comment_Details})
                
        return "channel uploaded successfully"
    
# Channeldetails('UCfk__uUEmg2H-W8Mh1Cx53A')
    
def channel_details(Channelname_s):
    postgres_conn=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="sql",
                            database="youtube_db",
                            port="5432"
                        )
    cursor=postgres_conn.cursor() 

    create_query='''create table if not exists Channels(ChannelName varchar(100),
                                        ChannelID varchar(100) primary key,
                                        Subscribers bigint,
                                        Views bigint,
                                        totalvideos int,
                                        ChannelDescription text,
                                        playlistID varchar(80)
                                        )'''                                     
    try:
        cursor.execute(create_query)
        postgres_conn.commit()
    except:
        print(" table already created")
        pass

### Reading mongo db for channel details
    single_ch_list=[]
    db=client["Youtube_db"]
    collection=db["Youtubechannels"]
    for ch_data in collection.find({"Channel_information.ChannelName":Channelname_s},{"_id":0,}):
        single_ch_list.append(ch_data["Channel_information"])
    s_ch_df=pd.DataFrame(single_ch_list)

#inserting the values in table

    for index,row in s_ch_df.iterrows():
        insert_query='''insert into channels(ChannelName,   
                                        ChannelID,
                                        Subscribers,
                                        Views,
                                        totalvideos,
                                        ChannelDescription,
                                        playlistID)                                       
                                        values(%s,%s,%s,%s,%s,%s,%s)'''
                                        
        values=(row['ChannelName'],
                row['ChannelID'],
                row['Subscribers'],
                row['Views'],
                row['totalvideos'],
                row['ChannelDescription'],
                row['playlistID'])

    try:
        cursor.execute(insert_query,values)
        postgres_conn.commit()
    except Exception as e:
        print(f'Error {e}')
        # print('Anything else that you feel is useful')
        postgres_conn.rollback()
        
    return "process completed"

#creating playlists table in sql

def playlist_details(Channelname_s):
    
    postgres_conn=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="sql",
                            database="youtube_db",
                            port="5432"
                        )
    cursor=postgres_conn.cursor() 

    create_query='''create table if not exists playlists(PlaylistID varchar(100) primary key,
                                        Title varchar(100),
                                        ChannelID varchar(100),
                                        ChannelName varchar(100),
                                        PublishedAt timestamp,
                                        Videocount int
                                        )'''               
                                
    try:
        cursor.execute(create_query)
        postgres_conn.commit()
    except Exception as e:
        print(f'Error {e}')
        postgres_conn.rollback()
    
    single_pl_list=[]
    db=client["Youtube_db"]
    collection=db["Youtubechannels"]
    for pl_data in collection.find({"Channel_information.ChannelName":Channelname_s},{"_id":0,}):
        single_pl_list.append(pl_data["Playlist_information"])
        s_pl_df=pd.DataFrame(single_pl_list)

    # to insert values from mongodb
    for index,row in s_pl_df.iterrows():
        insert_query='''insert into playlists(PlaylistID,
                                        Title,  
                                        ChannelID,
                                        ChannelName,
                                        PublishedAt,
                                        Videocount)                                       
                                        
                                        values(%s,%s,%s,%s,%s,%s)'''
                                        
        values=(row['PlaylistID'],
            row['Title'],
            row['ChannelID'],
            row['ChannelName'],
            row['PublishedAt'],
            row['Videocount']
            )
    
        try:
            cursor.execute(insert_query,values)
            postgres_conn.commit()
        
        except Exception as e:
            print(f'Error {e}')
            postgres_conn.rollback()
            
    return "process completed"

### program to insert videos into sql

def video_details(Channelname_s):

    postgres_conn=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="sql",
                                database="youtube_db",
                                port="5432"
                    )
    cursor=postgres_conn.cursor() 

    create_query='''create table if not exists videoslist( 
                                            VideoID varchar(100) primary key,
                                            VideoName varchar(150),
                                            ChannelName varchar(100),
                                            ChannelID varchar(100),                                          
                                            VideoDescription text,
                                            Tags text,
                                            PublishedAt timestamp,
                                            ViewCount bigint,
                                            CommentCount bigint,
                                            LikeCount bigint,
                                            FavouriteCount bigint,
                                            Duration interval,
                                            Thumnails varchar(200)
                                            )'''               
                                
    try:
        cursor.execute(create_query)
        postgres_conn.commit()

    except Exception as e:
        print(f'Error {e}')
        postgres_conn.rollback()


    single_vi_list=[]
    db=client["Youtube_db"]
    collection=db["Youtubechannels"]
    for vi_data in collection.find({"Channel_information.ChannelName":Channelname_s},{"_id":0,}):
        for i in range(len(vi_data["Video_information"])):            
                single_vi_list.append(vi_data["Video_information"][i])
                s_vi_df=pd.DataFrame(single_vi_list)

#### insert values in videoslist table

            
    for index,row in s_vi_df.iterrows():
            insert_query='''insert into videoslist(VideoID,
                                                VideoName,                                               
                                                ChannelName,  
                                                ChannelID,                                       
                                                VideoDescription,
                                                Tags,
                                                PublishedAt,
                                                ViewCount,
                                                CommentCount,
                                                LikeCount,
                                                FavouriteCount,
                                                Duration,
                                                Thumnails)                                       
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                
            values=(row['VideoID'], 
                        row['VideoName'],                
                        row['ChannelName'], 
                        row['ChannelID'],
                        row['VideoDescription'],
                        row['Tags'],
                        row['PublishedAt'],
                        row['ViewCount'],
                        row['CommentCount'],
                        row['LikeCount'],
                        row['FavouriteCount'],
                        row['Duration'],
                        row['Thumnails']
                )
            
            cursor.execute(insert_query,values)
            postgres_conn.commit()

    return "process completed"

#create table for Comment details in sql

def comments_details(Channelname_s):
    
    postgres_conn=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="sql",
                            database="youtube_db",
                            port="5432"
                        )
    cursor=postgres_conn.cursor() 

    create_query='''create table if not exists comments( CommentId varchar(100) primary key,
                                            VideoId varchar(100),
                                            ChannelID varchar(100),
                                            CommentText text,
                                            CommentAuthor varchar(100),
                                            CommentPublished timestamp
                                            
                                            )'''               
                                
    try:
        cursor.execute(create_query)
        postgres_conn.commit()

    except Exception as e:
        print(f'Error {e}')
        # print('Anything else that you feel is useful')
        postgres_conn.rollback()

    single_comm_list=[]
    db=client["Youtube_db"]
    collection=db["Youtubechannels"]


    for comm_data in collection.find({"Channel_information.ChannelName":Channelname_s},{"_id":0,}):
        for i in range(len(comm_data["Comment_information"])):            
                single_comm_list.append(comm_data["Comment_information"][i])
                s_com_df=pd.DataFrame(single_comm_list)


## insert values in commentsslist table

        
    for index,row in s_com_df.iterrows():
        insert_query='''insert into comments(     CommentId,
                                                    VideoId,
                                                    ChannelID,
                                                    CommentText,
                                                    CommentAuthor,
                                                    CommentPublished
                                            )                                       
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
                                        
        values=(row['CommentId'],                  
                    row['VideoId'], 
                    row['ChannelID'],
                    row['CommentText'],
                    row['CommentAuthor'],
                    row['CommentPublished']
                )



        cursor.execute(insert_query,values)
        
        postgres_conn.commit()
    
    return "process completed"

def mytables(singlechannel):
    ch=channel_details(singlechannel)
    pl=playlist_details(singlechannel)
    vi=video_details(singlechannel)
    com=comments_details(singlechannel)
    
    return 'full process completed'

# mytables("Shabarinath Premlal")

# Queries in sql

def myqueries(query):
    
    postgres_conn=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="sql",
                            database="youtube_db",
                            port="5432"
                        )
    cursor=postgres_conn.cursor() 

    if query=='What are the names of all the videos and their corresponding channels?':
        query1='select channelname, videoname from videoslist order by channelname'
        cursor.execute(query1)
        postgres_conn.commit()
        t1=cursor.fetchall()
        df=pd.DataFrame(t1,columns=["videoname","channelname"])
        
    elif query == 'Which channels have the most number of videos, and how many videos do they have?': 
        query2='''select channelname, videoscount from (
                        select
                            channelname,
                            count(videoname) as videoscount,
                            RANK () OVER ( 
                                ORDER BY count(videoname) DESC
                            ) rank 
                        from videoslist
                        group by channelname 
                        order by videoscount desc) where rank=1'''
        cursor.execute(query2)
        postgres_conn.commit()
        t2=cursor.fetchall()
        df=pd.DataFrame(t2,columns=["channelname","videoscount"])
        
    elif query=='What are the top 10 most viewed videos and their respective channels?':
        query3='select videoname,channelname,viewcount from videoslist order by viewcount desc limit 10'
        cursor.execute(query3)
        postgres_conn.commit()
        t3=cursor.fetchall()
        df=pd.DataFrame(t3,columns=["videoname","channelname","viewcount"])

    elif query=='How many comments were made on each video, and what are their corresponding video names?':
        query4='select videoid,count(commentid) as commentcount from comments group by videoid order by commentcount desc'
        cursor.execute(query4)
        postgres_conn.commit()
        t4=cursor.fetchall()
        df=pd.DataFrame(t4,columns=["videoid","commentcount"])
        
    elif query=='Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5='select channelname,videoname,likecount from videoslist order by likecount desc limit 1'
        cursor.execute(query5)
        postgres_conn.commit()
        t5=cursor.fetchall()
        df=pd.DataFrame(t5,columns=['channelname','videoname','likecount'])
        
    elif query=='What is the total number of likes for each video, and what are their corresponding video names?':
        query6='select videoname,likecount from videoslist'
        cursor.execute(query6)
        postgres_conn.commit()
        t6=cursor.fetchall()
        df=pd.DataFrame(t6,columns=['videoname','likecount'])
        
    elif query=='What is the total number of views for each channel, and what are their corresponding channel names?':
        query7='select channelname,sum(viewcount) as totalviews from videoslist group by channelname; '
        cursor.execute(query7)
        postgres_conn.commit()
        t7=cursor.fetchall()
        df=pd.DataFrame(t7,columns=['channelname','totalviews'])
        
    elif query=='What are the names of all the channels that have published videos in the year 2022?':
        query8="select channelname,videoname from videoslist where date_part('year', publishedat)='2022'"
        cursor.execute(query8)
        postgres_conn.commit()
        t8=cursor.fetchall()
        df=pd.DataFrame(t8,columns=['channelname','videoname'])
        
    elif query=='What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query9="select channelname,avg(duration) from videoslist group by channelname"
        cursor.execute(query9)
        postgres_conn.commit()
        t9=cursor.fetchall()
        df=pd.DataFrame(t9,columns=['channelname','avg'])
        
    elif query=='Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10="select channelname,videoname,max(commentcount) from videoslist group by channelname,videoname"
        cursor.execute(query10)
        postgres_conn.commit()
        t10=cursor.fetchall()
        df=pd.DataFrame(t10,columns=['channelname','videoname','max'])
        
    return df

def singlechanneldetails(Channelname_s):
    single_ch_list=[]
    db=client["Youtube_db"]
    collection=db["Youtubechannels"]
    for ch_data in collection.find({"Channel_information.ChannelName":Channelname_s},{"_id":0,}):
        single_ch_list.append(ch_data["Channel_information"])
    s_ch_df=pd.DataFrame(single_ch_list)
    
    return s_ch_df

def singleplaylist(Channelname_s):
    single_pl_list=[]
    db=client["Youtube_db"]
    collection=db["Youtubechannels"]
    for pl_data in collection.find({"Channel_information.ChannelName":Channelname_s},{"_id":0,}):
        single_pl_list.append(pl_data["Playlist_information"])
        s_pl_df=pd.DataFrame(single_pl_list)
        
    return s_pl_df

def singlevideos(Channelname_s):
    single_vi_list=[]
    db=client["Youtube_db"]
    collection=db["Youtubechannels"]
    for vi_data in collection.find({"Channel_information.ChannelName":Channelname_s},{"_id":0,}):
        for i in range(len(vi_data["Video_information"])):            
                single_vi_list.append(vi_data["Video_information"][i])
                s_vi_df=pd.DataFrame(single_vi_list)
                
    return s_vi_df

def singlecomments(Channelname_s):
    single_comm_list=[]
    db=client["Youtube_db"]
    collection=db["Youtubechannels"]


    for comm_data in collection.find({"Channel_information.ChannelName":Channelname_s},{"_id":0,}):
        for i in range(len(comm_data["Comment_information"])):            
                single_comm_list.append(comm_data["Comment_information"][i])
                s_com_df=pd.DataFrame(single_comm_list)
    return s_com_df

# mm=myqueries('What is the average duration of all videos in each channel, and what are their corresponding channel names?')
# print(mm)
