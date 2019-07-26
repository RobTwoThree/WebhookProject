import logging
import time
from discord_webhook import DiscordWebhook, DiscordEmbed
from config import webhook_url, google_map_api_key

webhook = DiscordWebhook(url=webhook_url, username="PoGoSD Rocket Bot")

def notify(data):
    pokestop_id = data[0][0]
    pokestop_name = data[0][1]
    pokestop_url = data[0][2]
    pokestop_lat = data[0][3]
    pokestop_lon = data[0][4]
    pokestop_incident_start = data[0][5]
    pokestop_incident_expiration = data[0][6]

    pokestop_name = "**" + str(pokestop_name) + " Pokestop**"
    start_time = "**" + str(time.strftime('%I:%M %p',  time.localtime(pokestop_incident_start))) + "**"
    end_time = "**" + str(time.strftime('%I:%M %p', time.localtime(pokestop_incident_expiration))) + "**"
    map_url = "https://www.google.com/maps?q=" + str(pokestop_lat) + "," + str(pokestop_lon)
    map_image = "https://maps.googleapis.com/maps/api/staticmap?center=" + str(pokestop_lat) + "," + str(pokestop_lon) + "&zoom=13&size=300x300&maptype=roadmap&markers=color:red%7Clabel:R%7C" + str(pokestop_lat) + "," + str(pokestop_lon) + "&key=" + str(google_map_api_key)
    
    embed = DiscordEmbed(title=pokestop_name, description='Dark Pokestop has emerged!', color=16714240)

    # set author
    #embed.set_author(name='Author Name', url='author url', icon_url='author icon url')

    # set image
    embed.set_image(url=map_image)

    # set thumbnail
    embed.set_thumbnail(url=pokestop_url)

    # set footer
    embed.set_footer(text='PoGoSD')

    # set timestamp (default is now)
    embed.set_timestamp()

    # add fields to embed
    embed.add_embed_field(name='*Started:*', value=start_time)
    embed.add_embed_field(name='*Ends:*', value=end_time)
    embed.add_embed_field(name='Location:', value=map_url)

    webhook.add_embed(embed)
    
    webhook.execute()
    print("Discord notification sent for: " + str(pokestop_name) + " START: " + str(start_time) + " END: " + str(end_time))
    return 'Discord notification sent.', 200
