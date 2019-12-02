#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sys 
import os  
import plotly.express as px
import plotly.graph_objects as go

argumentList = sys.argv 

def import_and_engineer_data():

    colnames = ['unix_time','message_id', 'person','recipients','topic','mode'] ### file has no column names, so adding here
    df = pd.read_csv(sys.argv[1],names=colnames, header=None) ## import data

    df['recipients_list'] = df['recipients'].str.split("|").to_list() ### break list of recipients into new list field

    df_simple = df[['unix_time','recipients_list']] ## narrow

    new_df  = df_simple.explode('recipients_list')
    sent_summary= df['person'].groupby(df['person']).count().reset_index(name="sent").sort_values(by='sent', ascending=False)  ##
r   eceived_summary = new_df['recipients_list'].groupby(new_df['recipients_list']).count().reset_index(name="received").sort_values(by='received', ascending=False)
    merged_results  =pd.merge(sent_summary, received_summary, left_on='person', right_on='recipients_list')  #summary list
    return merged_results
## Create summary of emails per sender  (for task 1)



def write_csv_output(input_dataframe):
    input_dataframe[['person','sent','received']].to_csv('enron_email_activity_summary.csv')  # write summary list to CSV file


def create_top_senders (input_dataframe):
# create list of top senders
    top_senders = merged_results.head(n=10)['person'].unique()
    y = df[df['person'].isin(top_senders)]  ## create new dataframe for data of top senders only
    y['month'] = pd.to_datetime(y['unix_time'], unit='ms')- pd.offsets.MonthBegin(1, normalize=True)  ## convert timestamp to first of month


# create data of recipients based on list of top senders

top_senders_received = new_df[new_df['recipients_list'].isin(top_senders)] 

top_senders_received['month'] = pd.to_datetime(top_senders_received['unix_time'], unit='ms')- pd.offsets.MonthBegin(1, normalize=True)  ## convert timestamp to first of month 
top_senders_received.head(n=10)



## create grouped summaries of sending and receiving activity per month

daily_sent_count = y[['person','month']].groupby(['person','month']).person.agg('count').reset_index(name = 'count')
daily_received_count = top_senders_received[['recipients_list','month']].groupby(['recipients_list','month']).recipients_list.agg('count').reset_index(name = 'count')






fig = px.line(daily_sent_count, x='month',y='count', color = 'person')

fig.update_layout(
    title=go.layout.Title(
        text="Number of Sent Emails for Top 10 Total Senders",
        x=0
    ),
    xaxis=go.layout.XAxis(
        title=go.layout.xaxis.Title(
            text="Date (grouped by Month)",
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="#7f7f7f"
            )
        )
    ),
    yaxis=go.layout.YAxis(
        title=go.layout.yaxis.Title(
            text="Number of Sent Emails in Month",
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="#7f7f7f"
            )
        )
    )
)

if not os.path.exists("images"):
    os.mkdir("images")
fig.write_image("images/Number_sent_emails.png")
fig.show()






import plotly.express as px
import plotly.graph_objects as go

fig = px.line(daily_received_count, x='month',y='count', color = 'recipients_list')

fig.update_layout(
    title=go.layout.Title(
        text="Number of Received Emails, Top 10 Senders",
        x=0
    ),
    xaxis=go.layout.XAxis(
        title=go.layout.xaxis.Title(
            text="Date (grouped by Month)",
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="#7f7f7f"
            )
        )
    ),
    yaxis=go.layout.YAxis(
        title=go.layout.yaxis.Title(
            text="Number of Received Emails in Month",
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="#7f7f7f"
            )
        )
    )
)





fig.write_image("images/Number_received_emails.png")
fig.show()


print ("Process Complete!  Output CSV is enron_email_activity_summary.csv, and output images are in \images folder.")


