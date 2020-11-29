import os
import pandas as pd
import tweepy


def get_tweet_ids():
    auth = tweepy.OAuthHandler(os.environ["KEY"], os.environ["KEY_SECRET"])
    auth.set_access_token(os.environ["TOKEN"], os.environ["TOKEN_SECRET"])

    api = tweepy.API(auth)

    LOCAL_ROOT = os.path.abspath("data") + os.sep
    import csv

    if not os.path.isfile(LOCAL_ROOT + "twitter_users.csv"):
        users  = {'name' : [], 'id' : []}
        with open(LOCAL_ROOT + 'handles.csv', newline='') as f:
            for row in csv.reader(f):
                try:
                    users['id'].append(api.get_user(row[0]).id_str)
                    users['name'].append(row[0])
                except Exception as e:
                    print(e)
                    print(row[0])
                    continue
        if len(users) != 0:
            df = pd.DataFrame(users, columns=["name", "id"])
            df.to_csv(LOCAL_ROOT + "twitter_users.csv")
        else:
           raise Exception("No user id found!")
    else:
        df = pd.read_csv(LOCAL_ROOT + "twitter_users.csv")

    user_ids = df["id"].apply(str).to_list()
    print(user_ids)