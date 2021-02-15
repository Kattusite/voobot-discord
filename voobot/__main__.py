import os
from dotenv import load_dotenv

from .voobot import VooBot

def main():
    """ Read API key from env and run a client """
    load_dotenv()
    TOKEN = os.getenv('DISCORD_TOKEN')

    cmd_prefix = "+"

    voobot = VooBot(cmd_prefix)
    voobot.run(TOKEN)

if __name__ == '__main__':
    main()
