import os
import json
import random
import requests
import feedparser

from datetime import datetime

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build



# ==================================================
# XRACING CONFIG
# ==================================================


CHANNEL_ID = "UCW3oIxTLllTLK3Vy9jzdoEg"


RSS_URL = (
    "https://www.youtube.com/feeds/videos.xml?"
    f"channel_id={CHANNEL_ID}"
)



DATABASE = "database.json"

COMMENTS = "comments.json"


DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]



# ==================================================
# FILES
# ==================================================


def load_json(path):

    with open(
        path,
        encoding="utf-8"
    ) as f:

        return json.load(f)



def save_json(path,data):

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            data,
            f,
            indent=4,
            ensure_ascii=False
        )



# ==================================================
# DISCORD
# ==================================================


def send_discord(message):

    try:

        requests.post(
            DISCORD_WEBHOOK,
            json={
                "content":message
            },
            timeout=10
        )

    except Exception as e:

        print(
            "Discord erro:",
            e
        )



# ==================================================
# YOUTUBE AUTH
# ==================================================


def youtube_client():


    token=json.loads(
        os.environ["YOUTUBE_TOKEN"]
    )


    credentials = Credentials.from_authorized_user_info(

        token,

        [
        "https://www.googleapis.com/auth/youtube.force-ssl"
        ]

    )


    return build(
        "youtube",
        "v3",
        credentials=credentials
    )



# ==================================================
# FIND VIDEO
# ==================================================


def get_latest_video():


    feed = feedparser.parse(
        RSS_URL
    )


    if not feed.entries:

        return None



    item = feed.entries[0]


    return {

        "id":
            item.yt_videoid,


        "title":
            item.title,


        "url":
            f"https://youtube.com/watch?v={item.yt_videoid}"

    }



# ==================================================
# COMMENT
# ==================================================


def comment(video_id):


    youtube = youtube_client()


    text=random.choice(
        load_json(COMMENTS)
    )


    youtube.commentThreads().insert(

        part="snippet",

        body={

            "snippet":{

                "videoId":video_id,


                "topLevelComment":{

                    "snippet":{

                        "textOriginal":text

                    }

                }

            }

        }

    ).execute()



    return text



# ==================================================
# MAIN
# ==================================================


def main():


    video=get_latest_video()


    if not video:

        print(
            "Nenhum vídeo encontrado"
        )

        return



    database=load_json(
        DATABASE
    )



    if video["id"] in database["videos"]:

        print(
            "Vídeo já comentado"
        )

        return



    try:


        comentario = comment(
            video["id"]
        )



        database["videos"].append(
            video["id"]
        )


        save_json(
            DATABASE,
            database
        )



        send_discord(

f"""
🚀 **XRACING DETECTADO**

🎬 {video['title']}

🔗 {video['url']}

💬 Comentário enviado:

"{comentario}"

⏰ {datetime.now()}
"""
        )


        print(
            "Comentário enviado"
        )



    except Exception as error:


        send_discord(

f"""
❌ ERRO XRACING BOT

{error}

⏰ {datetime.now()}
"""
        )


        raise error





if __name__=="__main__":

    main()
