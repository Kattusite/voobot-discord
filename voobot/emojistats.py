import discord
from discord.ext import commands

import tinydb
from tinydb import operations as dbops

import asyncio
import datetime
from collections import defaultdict
from operator import itemgetter
from os import path as op

import logging
logger = logging.getLogger(__name__)

CACHE_DIR = 'cache'

# How far back do we expect to find changes to entries already in the cache?
# The older of N messages ago, or T days ago.
RESCAN_LAST_N = 200
RESCAN_SINCE_DAYS = 7

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

def dttos(dt):
    """ Convert the provided datetime to a string. """
    return dt.strftime(DATETIME_FORMAT)

def stodt(s):
    """ Convert the provided string to a datetime. """
    return datetime.datetime.strptime(s, DATETIME_FORMAT)

class EmojiStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # This assumes the bot will only ever run on one server.
        self.db = tinydb.TinyDB(op.join(CACHE_DIR,'cache.json'),
                                encoding='utf-8',
                                indent=2,
                                ensure_ascii=False)

        self.cache = self.db.table('message_cache')
        self.sentinels = self.db.table('channel_sentinels')

    async def rescan_channel(self, ctx, channel):
        """ Rescan a given channel (or just a recent chunk of it) to
            populate self.cache with all sent messages with emoji or reacts.

            TODO: This doesn't yet include messages that are themselves emoji,
                  or that include emoji in the body
        """

        if not channel.permissions_for(ctx.guild.me).read_message_history:
            logger.warning(f'Bot not permitted to read_message_history in {channel.name}')
            return

        # Figure out how far back we'd like to invalidate the cache.
        # This will be the older of:
        # 1) RESCAN_SINCE_DAYS days ago, OR
        # 2) the datetime of the RESCAN_LAST_N'th most recent message in that channel.
            # BUG: This currently checks the Nth most recent message WITH A REACTION.
            #      That is wildly different than Nth most recent, which is a problem...
        # 2b) TODO: change it to: RESCAN_SINCE_DAYS before the last seen channel sentry.
        # TODO: This fails to account for the case where a channel has many many messages
        #       and almost no emoji, as we end up rescanning the entire list every time.
        #       This defeats the purpose of the cache :/
        # IDK what to do abt that other than just add sentries to the db.
        # e.g. db.insert({channel_sentry: channel_id, last_seen: datetime})

        Message = tinydb.Query()

        # New alg to fix all the bugs I had before
        """
            1.  Get sentinel from cache. timeA = sentinel.datetime.
                If no result, return None to rescan all.
                By definition, timeA is the most recent time for which we might
                possibly have any information, invalidated or otherwise.
            2.  Get last RESCAN_LAST_N messages before timeA, oldest first.
            3.  timeB = msg.datetime. Don't continue iterating.
            4.  timeC = timeA - timedelta(days=RESCAN_SINCE_DAYS)
            5.  Return min(timeB, timeC).
        """

        async def get_cache_invalidation_time():
            """ Determine how far back we'd like to invalidate the cache.

                TODO: Explain why this algorithm works, and why we need to invalidate the cache at all.
                        (to avoid dirty reads if reacts have been added since last scan)
                    Avoids the case of a channel with many messages, few reacts.
                    Assumes no "necro" reacting of very old messages, unless
                    those messages are the most recent in their channel.

                Returns:
                    - datetime.datetime, the time before which cache entries are
                      accepted as-is, and after which they are to be rescanned
                      from Discord.
            """
            # 1. Fetch sentinel time for this channel from cache.
            # This is the most recent datetime for which we have any data cached,
            # valid or otherwise.
            sentinel = self.sentinels.search(Message.channel == channel.id)
            if not sentinel:
                return None
            sentinel_time = stodt(sentinel['datetime'])

            # 2. Get time of the message RESCAN_LAST_N before the sentinel.
            async for msg in channel.history(limit=None, before=sentinel_time, oldest_first=True):
                timeA = msg.created_at
                break

            # 3.  Get time exactly RESCAN_SINCE_DAYS before the sentinel.
            timeB = sentinel_time - datetime.timedelta(days=RESCAN_SINCE_DAYS)

            # 4. Use whichever of the two times is earlier to minimize the
            #    risk of dirty cache entries.
            return min(timeA, timeB)

        rescan_since = await get_cache_invalidation_time()

        async def insert_record(msg):
            """ Insert a `Message` record into the cache. """
            self.cache.upsert({
                'datetime': msg.created_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
                'id':       msg.id,
                'channel':  channel.id,
                'author':   msg.author.id
            }, Message.id == msg.id)

            # Forcibly overwrite reacts to ensure deleted reacts are removed from cache.
            reacts = {str(r): [user.id for user in await r.users().flatten()] for r in msg.reactions}
            self.cache.update(dbops.set('reacts', reacts), Message.id == msg.id)

        # Find all the reacts in the last `limit` messages
        since_str = rescan_since.strftime("%Y-%m-%d %H:%M:%S.%f") if rescan_since else "forever"
        logger.info(f'Scanning channel history: {channel.name} since {since_str}')
        newest_msg = None
        async for msg in channel.history(limit=None, after=rescan_since, oldest_first=True):
            if msg.reactions:
                await insert_record(msg)
            newest_msg = msg

        # Store the newest message we've scanned in this channel as the sentinel.
        if newest_msg:
            # TODO: Searching for sentinels in the massive haystack of cache
            #       is needlessly slow. Just make a second table in the db,
            #       exclusively for sentinel records.
            #       For that matter, move messages to a table with a more apropos name than _default.
            # Time it first just to see what the perf impact really is..
            self.sentinels.upsert({
                'datetime': dttos(newest_msg.created_at),
                'channel':  channel.id
            }, Message.channel == channel.id)

        logger.info(f'{channel.name} scan complete!')

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
