import os
import json
import random
import feedparser

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


CHANNEL_ID = "UCW3oIxTLllTLK3Vy9jzdoEg"


RSS_URL = (
    "https://www.youtube.com/feeds/videos.xml?"
    f"channel_id={CHANNEL_ID}"
)



def youtube():

    token = json.loads(
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

    feed = feedparser.parse(
        RSS_URL
    )


    if not feed.entries:

        return None


    video = feed.entries[0]


    return {
        "id": video.yt_videoid,
        "title": video.title
    }



def create_comment(video):

    comentarios = [

        f"🔥 Muito bom esse vídeo: {video['title']}! Parabéns pelo conteúdo.",

        "Excelente vídeo! Muito interessante acompanhar esse projeto 🚀",

        "Show demais! Continuem trazendo conteúdos assim 👏",

        "Parabéns pelo trabalho, ficou muito legal acompanhar essa evolução!"

    ]


    return random.choice(
        comentarios
    )



def publish_comment(video_id, texto):

    api = youtube()


    response = api.commentThreads().insert(

        part="snippet",

        body={

            "snippet": {

                "videoId": video_id,

                "topLevelComment": {

                    "snippet": {

                        "textOriginal": texto

                    }

                }

            }

        }

    ).execute()


    return response



def main():

    video = latest_video()


    if not video:

        print("Nenhum vídeo encontrado")
        return



    print(
        "Vídeo encontrado:"
    )

    print(
        video["title"]
    )


    comentario = create_comment(
        video
    )


    print()
    print(
        "Publicando comentário:"
    )

    print(
        comentario
    )


    resultado = publish_comment(
        video["id"],
        comentario
    )


    print()
    print(
        "✅ Comentário publicado!"
    )

    print(
        "ID comentário:",
        resultado["id"]
    )



if __name__ == "__main__":
    main()
