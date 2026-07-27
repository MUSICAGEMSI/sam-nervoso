import requests


WEBHOOK = "https://discord.com/api/webhooks/1531359260175503420/VD_Kw5GpgvwX1hUXxgG81mSZn3I-t9v1FLu2wTl4V3asBRRCuex3dc4H_NeHoAPy2ArF"


requests.post(
    WEBHOOK,
    json={
        "content":
        "🚀 Teste XRacing Bot funcionando!"
    }
)
