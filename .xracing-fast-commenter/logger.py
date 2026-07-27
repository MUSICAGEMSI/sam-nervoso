from datetime import datetime


def log(msg):

    with open(
        "bot.log",
        "a",
        encoding="utf8"
    ) as f:

        f.write(
            f"{datetime.now()} - {msg}\n"
        )
