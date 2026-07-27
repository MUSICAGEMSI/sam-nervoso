import os
import json
import random
import requests
import feedparser

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build



CHANNEL_ID = "UCW3oIxTLllTLK3Vy9jzdoEg"


RSS_URL = (
    "https://www.youtube.com/feeds/videos.xml?"
    f"channel_id={CHANNEL_ID}"
)



def load(file):

    with open(file, encoding="utf-8") as f:
        return json.load(f)



def save(file,data):

    with open(
        file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            data,
            f,
            indent=4,
            ensure_ascii=False
        )



def discord(msg):

    requests.post(
        os.environ["DISCORD_WEBHOOK"],
        json={
            "content":msg
        },
        timeout=10
    )



def youtube():

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



def latest_video():

    feed=feedparser.parse(
        RSS_URL
    )


    if not feed.entries:
        return None


    video=feed.entries[0]


    return {

        "id":video.yt_videoid,

        "title":video.title

    }



def comment(video_id):

    api=youtube()


    comentarios=load(
        "comments.json"
    )


    texto=random.choice(
        comentarios
    )


    api.commentThreads().insert(

        part="snippet",

        body={

            "snippet":{

                "videoId":video_id,


                "topLevelComment":{

                    "snippet":{

                        "textOriginal":texto

                    }

                }

            }

        }

    ).execute()


    return texto



def main():


    video=latest_video()


    if not video:

        print("Nenhum vídeo")
        return



    database=load(
        "database.json"
    )


    if video["id"] in database["videos"]:

        print(
            "Já comentado"
        )

        return



    texto=comment(
        video["id"]
    )


    database["videos"].append(
        video["id"]
    )


    save(
        "database.json",
        database
    )



    discord(
f"""
🚀 XRACING BOT

🎬 {video['title']}

💬 Comentário:
{texto}

✅ Publicado!
"""
    )



if __name__=="__main__":
    main()
