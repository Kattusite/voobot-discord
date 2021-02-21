import discord
from discord.ext import commands

import tinydb
from tinydb import operations as dbops

import asyncio
import datetime
from collections import defaultdict, deque
import functools
import operator
import os
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
        self.db = tinydb.TinyDB(os.path.join(CACHE_DIR,'cache.json'),
                                encoding='utf-8',
                                indent=2,
                                ensure_ascii=False)

        # TODO: Consider a new table mapping channel ID to server?
        self.cache = self.db.table('message_cache')
        self.channels = self.db.table('channels')

    def get_users_by_name(self, ctx, name):
        """ Return a list of users whose (nick)names are a full or partial match for name. """
        # BUG: This just literally doesn't work at all for some reason.
        #       guild.members just shows the bot user???
        #       u.name = None ???
        #       same for nick ???
        logger.info(ctx.guild.members)
        return [u for u in ctx.guild.members if name.lower() in u.name.lower() or name.lower() in u.nick.lower()]

    def get_channel_id_by_name(self, ctx, channel_name):
        """ Return the ID of the channel with the name from the current ctx's guild. """

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
            if len(channels) != 1:
                logger.warning(f"Search for channel id {channel.id} expected 1 channel; yielded {channels}")
                return
            sentinel_datetime = stodt(channels[0]['sentinel_datetime'])

        if force_sentinel is not None:
            sentinel_datetime = force_sentinel

        async def insert_record(msg):
            """ Insert a `Message` record into the cache. """
            Message = tinydb.Query()
            self.cache.upsert({
                'id':       msg.id,
                'author':   msg.author.id,
                'channel':  channel.id,
                'datetime': msg.created_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
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
                'name': channel.name,
                'id': channel.id,
                'guild': ctx.guild.id,
                'sentinel_datetime': dttos(sentinel_datetime),
            }, Channel.id == channel.id)

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

    ###########################################################
    ##                     Querying
    ###########################################################

    """
    Each of the 'query_by_XXX' functions must accept (self, ctx, <str>) as arguments.

    Not all query_by functions have a use for the context, but some do, and dynamically
    figuring out which ones would add undue complexity, so we just pass it to each one
    and let the function process it as it likes.

    For similar reasons, the type of the final argument must be a string, which
    can then be decoded as a datetime, user object, etc. as desired by the query_by
    function.
    """

    def query_by_channel(self, ctx, channel_name):
        """ Return a tinydb query for messages sent to a specific channel. """
        channel_id = self.get_channel_id_by_name(ctx, channel_name)
        return tinydb.Query().channel == channel_id

    def query_by_author(self, ctx, author):
        """ Return a tinydb query for messages sent by a specific author. """
        user_ids = [u.id for u in self.get_users_by_name(ctx, author)]
        return tinydb.Query().author.test(lambda uid: uid in user_ids)

    def query_by_reactor(self, ctx, reactor):
        """ Return a tinydb query for messages reacted to by a specific user. """
        user_ids = [u.id for u in self.get_users_by_name(ctx, reactor)]
        def test_reactor(reacts):
            for user_id in user_ids:
                if any([user_id in reactors for reactors in reacts]):
                    return True
            return False

    def query_by_react(self, ctx, react):
        """ Return a tinydb query for messages reacted to with a specific react.
            Params:
                react, a substring of the discord reaction string we'd like to find.
                e.g. "pogg" would match against "<:poggers:0123456789>"
        """
        def test_react(reacts):
            return any([react in r for r in reacts])
        return tinydb.Query().reacts.test(test_react)

    def query_by_before(self, ctx, before_date):
        """ Return a tinydb query for messages sent before a specific date. """
        test_before = lambda dt_str: stodt(dt_str) < stodt(before_date, fmt=YMD_FORMAT)
        return tinydb.Query().datetime.test(test_before)

    def query_by_after(self, ctx, after_date):
        """ Return a tinydb query for messages sent after a specific date. """
        test_after = lambda dt_str: stodt(dt_str) > stodt(after_date, fmt=YMD_FORMAT)
        return tinydb.Query().datetime.test(test_after)

    def query_all(self):
        """ Return a tinydb query that matches every entry.

            This is used as the initializer for calls to reduce().
        """
        return tinydb.Query().id.exists()

    def query_message_cache(self, ctx, *args):
        """ Search the message cache with the given directives, and return a
            list of messages that match. """

        directives = {
            'in': self.query_by_channel,
            'by': self.query_by_reactor,
            'msgby': self.query_by_author,
            'react': self.query_by_react,
            'before': self.query_by_before,
            'after': self.query_by_after,
        }

        queries = []
        for arg in args:
            cmd_pcs = arg.split(":", 1)
            if len(cmd_pcs) != 2:
                logger.warning(f"Skipping unrecognized query directive: '{arg}'")
                continue

            cmd, val = cmd_pcs
            if cmd not in directives:
                logger.warning(f"Skipping unknown query command: '{cmd}'")
                continue
            query_func = directives[cmd]

            # `val` is allowed to be a comma-separated list, in which case we construct
            # one subquery for each val, and merge them together with the | operator.
            # (i.e. match any message that satisfies just one predicate)
            vals = val.split(",")
            subqueries = [query_func(ctx, v) for v in vals]
            query = functools.reduce(operator.or_, subqueries)
            queries.append(query)

        # Combine queries with & operator to match only those that satisfy all queries.
        # If queries is empty, query_all() acts as the default, matching all messages.
        merged_query = functools.reduce(operator.and_, queries, self.query_all())

        return self.cache.search(merged_query)

    def collate_messages(self, ctx, messages, *args):
        """ Transform the provided list of messages into a clean set of results that
            can be easily displayed, per the format requested by args.

            By default, this format will be a dictionary mapping emoji to their
            aggregate number of occurrences across the entire set of messages.
        """
        # TODO: Implement other collation formats.

        # Recall messages looks like this:
        # [ {..., 'reacts': {'<:poggers:12345>': [uid, uid, uid], '👍': [uid, uid, uid]}}, ... ]
        collated = defaultdict(lambda: 0)
        for msg in messages:
            for react, reactors in msg['reacts'].items():
                collated[react] += len(reactors)
        return collated

    async def display_emoji_stats(self, ctx, results, *args):
        """ Display a collection of results in a manner specified by args.

            By default, this will be an Embed table mapping emojis to their counts.
        """

        # TODO: Implement other display modes

        await self.send_emoji_table(ctx, results)

    @commands.command()
    async def hist(self, ctx, *args):
        """

        Directives:
            <cmd>:<value>[,<value>[...]]

        Distinct commands are combined with an AND relation, while the comma-separated
        values are combined with one another in OR relations.

        Params:
            args: a list of directives formatted as described above.
        """

        help_msg = """
Query directives:
    - in:channel
    - by:user
    - msgby:user
    - react:name/symbol
    - before:yyyy-mm-dd
    - after:yyyy-mm-dd

Output directives:
    - as:counts|bars|table|graph|...

Examples:
    - in:general,spam after:2020-01-01 before:2020-12-31
        Select from the general or spam channel in the year 2020.
    - msgby:Alice "by:Eve Dropper"
        Select messages by Alice with reactions by "Eve Dropper"
"""
        if 'help' in args:
            ctx.send(help_msg)
            return

        msgs = self.query_message_cache(ctx, *args)
        collated_msgs = self.collate_messages(ctx, msgs, *args)
        await self.display_emoji_stats(ctx, collated_msgs, *args)


    async def send_emoji_table(self, ctx, emojis):
        """ Send an embed table mapping each emoji to its number of occurrences.
            Params:
                - emoji, dict:
                    {emoji-str: 3}
        """

        # TODO: Make this embed less awful and sad.
        #       Make the Embed table a little prettier, give more information
        #       from args, like which params were specified, which channels
        #       were searched, what time the info is up-to-date as of, ...

        embed=discord.Embed(title=f"Results", color=0xb14e4e)

        # TODO: This is really jank, don't use a list of tuples.
        #       Use some sort of dict.items() magic instead, or some magic comprehension?
        # Convert dict to a list of tuples
        counts = list(emojis.items())

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
