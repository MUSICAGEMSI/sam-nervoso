import os
import random
import requests
import feedparser

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


CHANNEL_ID = "UCW3oIxTLllTLK3Vy9jzdoEg"


RSS = (
    "https://www.youtube.com/feeds/videos.xml?"
    f"channel_id={CHANNEL_ID}"
)


def youtube():

    token = os.environ["YOUTUBE_TOKEN"]

    creds = Credentials.from_authorized_user_info(
        eval(token),
        [
        "https://www.googleapis.com/auth/youtube.force-ssl"
        ]
    )

    return build(
        "youtube",
        "v3",
        credentials=creds
    )



def novo_video():

    feed = feedparser.parse(RSS)

    return feed.entries[0].yt_videoid



def comentar(video):

    api = youtube()


    with open("comments.txt") as f:
        comentarios=f.readlines()


    texto=random.choice(comentarios).strip()


    api.commentThreads().insert(

        part="snippet",

        body={

            "snippet":{

                "videoId":video,

                "topLevelComment":{

                    "snippet":{

                        "textOriginal":texto

                    }

                }

            }

        }

    ).execute()


    return texto



video=novo_video()


with open("last_video.txt") as f:
    antigo=f.read().strip()



if video != antigo:


    comentario=comentar(video)


    with open(
        "last_video.txt",
        "w"
    ) as f:

        f.write(video)



    requests.post(
        os.environ["DISCORD_WEBHOOK"],
        json={
            "content":
            f"🚀 Novo Xracing!\nComentário: {comentario}"
        }
    )

else:

    print(
        "Nada novo"
    )
