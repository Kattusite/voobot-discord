import discord
from discord.ext import commands
import yaml

import asyncio
import datetime
from collections import defaultdict
import glob
import os

import logging
logger = logging.getLogger(__name__)

CACHE_DIR = 'cache'

# How far back do we expect to find changes to entries already in the cache?
# The older of N messages ago, or T days ago.
REFETCH_LAST_N = 200
REFETCH_SINCE_DAYS = 7

class MessageData():
    def __init__(self, id: str, date: str, time: str, author: str, channel: str):
        self.id = id
        self.datetime = datetime.datetime.strptime(f'{date} {time}', '%Y-%m-%d %H:%M:%S.%f')
        self.author = author
        self.channel = channel

class ReactData():
    def __init__(self, emoji: str, sender: str, msg_id: str, msg_date: str,
                 msg_time: str, msg_author: str, channel: str):
        self.emoji = emoji
        self.sender = sender
        self.msg = MessageData(msg_id, msg_date, msg_time, msg_author, channel)

class EmojiStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        self.react_data = defaultdict(lambda: [])
        """ Maps channel name to List[ReactData] """

        # Read emoji history.

        # If the cache file exists, read from it
        # I expect entries to look like this:
        #   time,channel,emoji,sender,receiver

        # Search through available channels to find new messages
        # (those sent since the most recent message in cache)
        # NOTE: This assumes no reacts are added to old messages.
        #       Perhaps do min(1 week, oldest cached)
        # Something like:
        #           always check last 100 msgs, and messages newer than 24h
        #           but read all other results from cache

        # TODO: This also doesn't include messages that are themselves emoji, or that include emoji
        # These can be:
        #   time,channel,emoji,sender,

    def load_cache_file(self, filename):
        """ Load the cache file of the given name.
            Update self.react_data to reflect the new data.
        """
        if not os.path.isfile(filename):
            logger.warning(f'cache file {filename} does not exist!')
            return

        with open(filename, 'r', encoding='utf-8') as f:
            data = yaml.load(f)

        # Strip off the '.yaml'
        channel = filename[:-5]

        # Flatten each record so we can filter more easily
        # Yes, this is janky.
        rds = []
        for ymd, records in data.items():
            for r in records:
                for emoji, users in r['emoji'].items():
                    for u in users:
                        rd = ReactData(emoji=emoji,
                                       sender=u,
                                       msg_id=r['msg'],
                                       msg_date=ymd,
                                       msg_time=r['time'],
                                       msg_author=r['author'],
                                       channel=channel)
                        rds.append(rd)

        # WARNING HUGE BUG:
        # This does not handle merges correctly.
        # react_data needs to be a map of channel -> message-id -> ReactData, so
        # we can overwrite existing messages easily
        # NAH FUCK IT
        # TINYDB ALL THE WAY
        # STOP FUCKING HAND ROLLING DATABASES
        # YIKES


        # Already existing entries will need to be updated
        self.react_data[channel] += rds

    async def load_cache(self):
        """ Read the yaml files in the cache directory into memory, and use
            the data to populate self.react_data.
        """

        cache_filenames = glob.glob(os.path.join(CACHE_DIR, '*.yaml'))

        coros = [asyncio.to_thread(self.load_cache_file, fn) for fn in filename]
        await asyncio.gather(*coros)

    async def rescan_channel(self, ctx, channel):
        """ Rescan a given channel [TODO: or just a recent chunk of it] to
            populate its cache file with all reactions.
        """

        if not channel.permissions_for(ctx.guild.me).read_message_history:
            logger.warning(f'Bot not permitted to read_message_history in {channel.name}')
            return

        if not self.react_data:
            await self.load_cache()

        limit = None

        # If we've got some cache for this channel, find the Nth oldest message
        if channel.name in self.react_data:




        # TODO: Do some magic to figure out which history is "recent enough"


        records = defaultdict(lambda: [])
        """ A collection of records that look like this:
            yyyy-mm-dd: [
                {
                    "msg": msgid,
                    "author": userid,
                    "time": hh:mm:ss.f
                    "emoji": {
                        "emojistr": [userid, userid, userid],
                        "emojistr": [userid, userid, userid],
                    }
                },
            ]
        """
        async def insert_record(msg):
            ymd = msg.created_at.strftime("%Y-%m-%d")
            records[ymd].append({
                "msg": msg.id,
                "author": msg.author.id,
                "time": msg.created_at.strftime("%H:%M:%S.%f"),
                "reacts": {str(r): [user.id for user in await r.users().flatten()] for r in msg.reactions}
            })

        # Find all the reacts in the last `limit` messages
        logger.info(f'enumerating channel history for {channel.name}')
        async for msg in channel.history(limit=limit):
            if msg.reactions:
                await insert_record(msg)

        # This might slow the f out of the main thread, maybe use asyncio.to_thread()
        filename = os.path.join(CACHE_DIR, f'{channel.name}.yaml')
        logger.info(f'dumping {channel.name} to cache...')
        with open(filename, 'w', encoding='utf-8') as f:
            yaml.dump(dict(records), f, allow_unicode=True)
        logger.info(f'{channel.name} dump complete!')

        # await self.send_emoji_table(ctx, emojis, limit)

    @commands.command()
    async def rescan(self, ctx):
        logger.info('initiating rescan...')
        msg = await ctx.send("Rescanning channels. This might take a while...")

        with self.bot.progress_bar(msg):
            # await asyncio.sleep(5)
            scan_coros = [self.rescan_channel(ctx, c) for c in ctx.guild.text_channels]
            await asyncio.gather(*scan_coros)
        logger.info('done rescan')



    async def message_reactions(self, msg):
        # TODO: Why list of users and not just # occurrences - the latter isn't async...
        """ Process all the reactions on a given message.
            Returns the a dictionary mapping emoji str to user list
            {
                "reaction-str": ["UsernameFoo#1234", "UsernameBar#5678"],
                ...
            }
        """
        reactors = {}
        for react in msg.reactions:
            users = await react.users().flatten()
            s = str(react.emoji)
            reactors[s] = users

        return reactors

    # TODO
    # def lookup_emoji:
    # Lookup, in a combination of the cache and the message history,
    # the emoji that have been used recently..

    async def send_emoji_table(self, ctx, emojis, limit):
        """ Send an embed table mapping each emoji to its number of occurrences.
            Params:
                - emoji, dict:
                    {emoji-str: [user1, user2]}
        """

        embed=discord.Embed(title=f"Last {limit} messages", color=0xb14e4e)

        # Transform the str: userlist dict into a list of (str, usercount) tuples
        counts = [(emoji_str, len(users)) for emoji_str, users in emojis.items()]

        # And sort it by descending number of occurences...
        counts.sort(key=lambda x: x[1], reverse=True)
        for emoji_str, count in counts:
            embed.add_field(name=emoji_str, value=str(count))

        await ctx.send(embed=embed)

    @commands.command()
    async def hist(self, ctx, limit=250):

        emojis = defaultdict(lambda: [])

        # Find all the reacts in the last `limit` messages
        async for msg in ctx.channel.history(limit=limit):

            rs = await self.message_reactions(msg)

            for s, users in rs.items():
                emojis[s] += users

        print(dict(emojis))
        await self.send_emoji_table(ctx, emojis, limit)

    # please clap
    # assign a random react to the message


def setup(bot):
    bot.add_cog(EmojiStats(bot))
