import streamlit as st
from youtubemain import *

st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
with st.sidebar:
    option=st.selectbox("select any one",
        ("get data", 'migrate data', 'query data','single channel data'))
try:
    st.write('You selected:', option)
    if option == 'get data':
        
        channel_id=st.text_input("Enter the channel ID")
        ch_li=[]
        db=client["Youtube_db"]
        collection=db["Youtubechannels"]
        for ch_data in collection.find({},{"_id":0,"Channel_information":1}):
            ch_li.append(ch_data["Channel_information"]["ChannelID"])
        if channel_id in ch_li:
            st.success("The channel already exist")
        else:
            insert=Channeldetails(str(channel_id))
            st.success("inserted successfully")

    elif option=="migrate data":
        ch_name=[]
        db=client["Youtube_db"]
        collection=db["Youtubechannels"]
        for ch_data in collection.find({},{"_id":0,"Channel_information":1}):
            ch_name.append(ch_data["Channel_information"]["ChannelName"])
        singlechannel = st.selectbox("select channel to migrate",ch_name)
        if st.button('migrate to sql'):
            try:
                table = mytables(str(singlechannel))
                st.success("channel inserted successfully in sql")
            except:
                st.success("channel already inserted")
                
    elif option=="query data":    
        ques=st.selectbox('Select the query',
                            ('What are the names of all the videos and their corresponding channels?',
                            'Which channels have the most number of videos, and how many videos do they have?',
                            'What are the top 10 most viewed videos and their respective channels?',
                            'How many comments were made on each video, and what are their corresponding video names?',
                            'Which videos have the highest number of likes, and what are their corresponding channel names?',
                            'What is the total number of likes for each video, and what are their corresponding video names?',
                            'What is the total number of views for each channel, and what are their corresponding channel names?',
                            'What are the names of all the channels that have published videos in the year 2022?',
                            'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                            'Which videos have the highest number of comments, and what are their corresponding channel names?'
                            ))  
        if st.button("show data"):  
            return_value=myqueries(str(ques))
            st.write(return_value)
            
    if option == "single channel data":
        ch_name=[]
        db=client["Youtube_db"]
        collection=db["Youtubechannels"]
        
        for ch_data in collection.find({},{"_id":0,"Channel_information":1}):
            ch_name.append(ch_data["Channel_information"]["ChannelName"])
        singlechannel = st.selectbox("select channel to show details",ch_name)
        showdata=st.selectbox("choose one",("Channel Details","Playlist Details","Video Details","Comments Details"))
        
        if showdata == "Channel Details":
            ch_details=singlechanneldetails(singlechannel)
            st.write(ch_details)
            
        if showdata == "Playlist Details":
            pl_details=singleplaylist(singlechannel)
            st.write(pl_details)
            
        if showdata == "Video Details":
            vi_details=singlevideos(singlechannel)
            st.write(vi_details)
            
        if showdata == "Comments Details":
            co_details=singlecomments(singlechannel)
            st.write(co_details)
            
except:
    pass
    


            
