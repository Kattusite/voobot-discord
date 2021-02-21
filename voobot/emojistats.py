import discord
from discord.ext import commands

import tinydb
from tinydb import operations as dbops

import asyncio
import datetime
from collections import defaultdict, deque
from operator import itemgetter
from os import path as op
import time

import logging
logger = logging.getLogger(__name__)

###########################################################
##                Constants and Helpers
###########################################################

CACHE_DIR = 'cache'

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
YMD_FORMAT = '%Y-%m-%d'

def dttos(dt, fmt=DATETIME_FORMAT):
    """ Convert the provided datetime to a string. """
    return dt.strftime(fmt)

def stodt(s, fmt=DATETIME_FORMAT):
    """ Convert the provided string to a datetime. """
    return datetime.datetime.strptime(s, fmt)


###########################################################
##                     EmojiStats
###########################################################

class EmojiStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # This assumes the bot will only ever run on one server.
        # It might work for several servers, but I haven't tested it.
        self.db = tinydb.TinyDB(op.join(CACHE_DIR,'cache.json'),
                                encoding='utf-8',
                                indent=2,
                                ensure_ascii=False)

        # TODO: Consider a new table mapping channel ID to server?
        self.cache = self.db.table('message_cache')
        self.channels = self.db.table('channels')

    def get_users_by_name(self, ctx, name):
        """ Return a list of users whose (nick)names are a full or partial match for name. """
        return [u for u in ctx.guild.members if name in u.name or name in u.nick]

    def get_channel_id_by_name(self, ctx, name):
        """ Return the name of the chaannel with the given id from the current ctx's guild. """

        Channel = tinydb.Query()
        channels = self.channels.search((Channel.guild == ctx.guild.id) &
                                        (Channel.name == channel_name))
        if not channels:
            err_msg = f'Could not find channel {channel_name} in guild {ctx.guild.id}'
            logger.warning(err_msg)
            raise KeyError(err_msg)
        if len(channels) > 1:
            logger,warning(f'Ambiguous match for channel {channel_name} in guild {ctx.guild.id}')
        return channels[0]['id']

    async def rescan_channel(self, ctx, channel, lookback_num=250,
                             lookback_time=datetime.timedelta(days=7),
                             force_sentinel=None):
        """
        Rescan the given channel to populate self.cache with all previously
        sent messages with emoji or reacts.

        To avoid excessive re-scanning of already-cached messages, we persist
        a "sentinel" datetime to the database indicating where to begin the next scan.

        Messages sent before the sentinel are considered final and will never be rescanned;
        messages sent after are considered tentative, and might have their cache entries
        invalidated (i.e. overwritten by newer data).

        The sentinel is chosen to be the earlier of:
        1) the datetime of the `lookback_num`'th most recent message, OR
        2) the datetime occurring `lookback_time` before the most recent message.

        WARNING: The sentinel is an imperfect heuristic; in particular it
                 assumes that messages will never be reacted to again once
                 they become old enough. (i.e. no "necro" reactions).

        TODO: This doesn't yet include messages that are themselves emoji,
              or that include emoji in the body

        Args:
            ctx: The context from which the rescan was requested.
            channel: The channel to be rescanned.
            lookback_num: How many messages to look back for the next sentinel.
            lookback_time: How much time before the most recent message to look
                           back for the next sentinel.

        Returns:
            None
        """
        start_time = time.time()

        if not channel.permissions_for(ctx.guild.me).read_message_history:
            logger.warning(f'Bot not permitted to read_message_history in {channel.name}')
            return

        # Find the last sentinel, if it exists
        Channel = tinydb.Query()
        sentinel_datetime = None
        if channels := self.channels.search(Channel.id == channel.id):
            assert len(channels) == 1
            sentinel_datetime = stodt(channels[0]['sentinel_datetime'])

        if force_sentinel is not None:
            sentinel_datetime = force_sentinel

        Message = tinydb.Query()
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

        since_str = "forever ago"
        if sentinel_datetime:
            since_str = sentinel_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
        logger.info(f'Scanning channel history: {channel.name} since {since_str}')

        # Find all the reacts in the channel since our last sentinel
        # Maintain a sliding window of the last RESCAN_LAST_N messages
        # so we can construct a new sentinel.
        newest_msgs = deque(maxlen=lookback_num)
        async for msg in channel.history(limit=None, after=sentinel_datetime, oldest_first=True):
            if msg.reactions:
                await insert_record(msg)
            newest_msgs.append(msg)

        # Select and persist a new sentinel
        if newest_msgs:
            nth_newest_datetime = newest_msgs[0].created_at
            newest_datetime = newest_msgs[-1].created_at
            sentinel_datetime = min(nth_newest_datetime, newest_datetime - lookback_time)

            self.channels.upsert({
                'sentinel_datetime': dttos(sentinel_datetime),
                'id': channel.id,
                'name': channel.name,
                'guild': ctx.guild.id
            }, Message.channel == channel.id)

        elapsed_time = time.time() - start_time
        logger.info(f'{channel.name} scan complete in {elapsed_time:.1f}s')

    @commands.command()
    async def rescan(self, ctx):
        if ctx.author.id != ctx.guild.owner_id:
            logger.info(f"user {ctx.author.name} tried to rescan...")
            await ctx.message.delete()
            return

        logger.info('initiating rescan...')
        msg = await ctx.send("Rescanning channels. This might take a while...")

        with self.bot.progress_bar(msg):
            # await asyncio.sleep(5)
            scan_coros = [self.rescan_channel(ctx, c) for c in ctx.guild.text_channels]
            await asyncio.gather(*scan_coros)
        logger.info('done rescan')

    def query_by_channel(self, ctx, channel_name):
        channel_id = self.get_channel_id_by_name(ctx, channel_name)
        return tinydb.Query().channel == channel_id

    def query_by_author(self, ctx, author):
        user_ids = [u.id for u in self.get_users_by_name(ctx, author)]
        return tinydb.Query().author.test(lambda uid: uid in user_ids)

    def query_by_reactor(self, ctx, reactor):
        user_ids = [u.id for u in self.get_users_by_name(ctx, reactor)]
        def test_reactor(reacts):
            for user_id in user_ids:
                if any([user_id in reactors for reactors in reacts]):
                    return True
            return False

    def query_by_react(self, react):
        def test_react(reacts):
            return any([react in r for r in reacts])
        return tinydb.Query().reacts.test(test_react)

    def query_by_before(self, before_date):
        test_before = lambda dt_str: stodt(dt_str) < stodt(before_date, fmt=YMD_FORMAT)
        return tinydb.Query().datetime.test(test_before)

    def query_by_after(self, after_date):
        test_after = lambda dt_str: stodt(dt_str) > stodt(after_date, fmt=YMD_FORMAT)
        return tinydb.Query().datetime.test(test_after)

    def query_message_cache(self, ctx, *args):
        """ Search the message cache with the given directives, and return a
            list of messages that match. """
        return []

    def collate_messages(self, ctx, messages, *args):
        """ Transform the list of messages provided according to the provided
            output directives. """
        return []

    @commands.command()
    async def hist(self, ctx, *args):
        """

        Directives:
            <cmd>:<value>[,<value>[...]]

        Params:
            args: a list of directives formatted as described above.
        """

        cmds = {
            'in': query_by_channel,
            'by': query_by_author,
            'emoji': query_by_emoji,
            'before': query_by_before,
            'after': query_by_after,
        }

        help_msg = """
        in:channel
        by:user
        msgby:user
        react:name
        before:yyyy-mm-dd
        after:yyyy-mm-dd
        as:table|graph|...
        """
        if 'help' in args:
            ctx.send(help_msg)
            return

        tests = []
        for cmd in cmds:
            pass

        ## msgs = query_message_cache(...)
        ## collated = collate_messages(...)
        ## display_results(collated)


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

    # @commands.command()
    # async def hist(self, ctx, limit=250):
    #
    #     emojis = defaultdict(lambda: [])
    #
    #     # Find all the reacts in the last `limit` messages
    #     async for msg in ctx.channel.history(limit=limit):
    #
    #         rs = await self.message_reactions(msg)
    #
    #         for s, users in rs.items():
    #             emojis[s] += users
    #
    #     print(dict(emojis))
    #     await self.send_emoji_table(ctx, emojis, limit)

    # please clap
    # assign a random react to the message


def setup(bot):
    bot.add_cog(EmojiStats(bot))
