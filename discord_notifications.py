import logging

from discord_webhook import DiscordWebhook, DiscordEmbed
from config import webhook_url

webhook = DiscordWebhook(url=webhook_url, username="PoGoSD Rocket Bot")

def notify(data):
    pokestop_id = data[0][0]
    pokestop_name = data[0][1]
    pokestop_url = data[0][2]
    pokestop_lat = data[0][3]
    pokestop_lon = data[0][4]
    pokestop_incident_start = data[0][5]
    pokestop_incident_expiration = data[0][6]

    #start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pokestop_incident_start))
    #end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pokestop_incident_expiration))
    
    #print("START TIME: " + str(start_time))
    #print("END TIME: " + str(end_time))
    embed = DiscordEmbed(title=pokestop_name, description='Dark Pokestop has emerged!', color=242424)

    # set author
    #embed.set_author(name='Author Name', url='author url', icon_url='author icon url')

    # set image
    #embed.set_image(url='your image url')

    # set thumbnail
    embed.set_thumbnail(url=pokestop_url)

    # set footer
    embed.set_footer(text='PoGoSD')

    # set timestamp (default is now)
    embed.set_timestamp()

    # add fields to embed
    embed.add_embed_field(name='Started:', value='Time 1')
    embed.add_embed_field(name='Ends:', value='Time 2')

    webhook.add_embed(embed)
    
    webhook.execute()
    print("Discord notification sent.")
    return 'Discord notification sent.', 200
