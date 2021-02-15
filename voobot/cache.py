import discord
from discord.ext import commands

import tinydb
from tinydb import operations as dbops

import asyncio
import datetime
from collections import deque
import functools
import logging
import operator
import os
import time

from . import progressbar # Imported for its constants (TYPING, ...)

logger = logging.getLogger(__name__)

###########################################################
##                Constants and Helpers
###########################################################

CACHE_DIR = 'cache'

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
YMD_FORMAT = '%Y-%m-%d'

def dttos(dt, fmt=DATETIME_FORMAT):
    """ Convert the provided datetime to a string. """
    if dt is None:
        return None
    return dt.strftime(fmt)

def stodt(s, fmt=DATETIME_FORMAT):
    """ Convert the provided string to a datetime. """
    if not s:
        return None
    return datetime.datetime.strptime(s, fmt)


###########################################################
##                     Cache
###########################################################

class Cache(commands.Cog):
    """
    A Cache object implements the caching functionality for voobot in order
    to minimize the number of Discord API requests the bot must make.

    Callers should interface with this class through its defined instance
    methods, rather than by accessing its internal fields directly.

    Currently, the internal caching is provided by a TinyDB instance,
    backed by JSON files written to disk. However, this is a specific
    implementation detail, and it should not be relied upon as stable.
    """
    def __init__(self, bot):
        self.bot = bot
        self.bot.cache = self

        # This assumes the bot will only ever run on one server.
        # It might work for several servers, but I haven't tested it.
        self._db = tinydb.TinyDB(os.path.join(CACHE_DIR,'cache.json'),
                                encoding='utf-8',
                                indent=2,
                                ensure_ascii=False)

        # Load DB tables from disk, or initialize them if they don't exist.
        # Note: the 'reacted_messages' table only caches messages with reactions.
        #       This may change in the future.
        self._messages = self._db.table('reacted_messages')
        self._channels = self._db.table('channels')
        self._members = self._db.table('members')
        self._emoji = self._db.table('emoji')

    def get_members_by_name(self, ctx, name: str):
        """
        Return a list of members belonging to the guild of the provided context,
        and whose names or nicknames include the string `name`.

        Params:
            - ctx, the discord context in which to perform the lookup.
            - name, the string to match the name and/or nickname against.

        Returns:
            - List[Discord.Member], the matching members
        """
        # TODO: We don't invalidate member records, so weird things might
        #       happen if we try to look up a member who's left the guild.
        Member = tinydb.Query()

        def contains_string(needle):
            return lambda haystack: needle in haystack

        guild_matches = Member.guild == ctx.guild.id
        name_matches = Member.name.test(contains_string(name))
        nick_matches = Member.nick.test(contains_string(name))

        members = self._members.search(guild_matches & (name_matches | nick_matches))
        member_ids = set([u['id'] for u in members])

        # Now we have a list of cache entries (dict), but we need to map
        # these back to a list of discord.Member.
        return [u for u in ctx.guild.members if u.id in member_ids]

    def get_channel_id_by_name(self, ctx, channel_name):
        """ Return the ID of the channel with the name from the current ctx's guild. """

        Channel = tinydb.Query()
        channels = self._channels.search((Channel.guild == ctx.guild.id) &
                                        (Channel.name == channel_name))
        if not channels:
            err_msg = f'Could not find channel {channel_name} in guild {ctx.guild.id}'
            logger.warning(err_msg)
            raise KeyError(err_msg)
        if len(channels) > 1:
            logger,warning(f'Ambiguous match for channel {channel_name} in guild {ctx.guild.id}')
        return channels[0]['id']

    @commands.command()
    async def rescan(self, ctx):
        if ctx.author.id != ctx.guild.owner_id:
            logger.info(f"user {ctx.author.name} tried to rescan...")
            await ctx.message.delete()
            return

        progress_msgs = ["Rescanning. This might take a while..."]
        def progress_msg():
            return "\n   >  ".join(progress_msgs)

        logger.info('initiating rescan...')
        msg = await ctx.send(progress_msg())

        logger.info('scanning members...')
        self._rescan_members(ctx)

        logger.info('scanning channels...')
        with self.bot.progress_bar(msg, reacts=progressbar.TYPING):
            # await asyncio.sleep(5)
            scan_coros = [self._rescan_channel(ctx, c) for c in ctx.guild.text_channels]
            await asyncio.gather(*scan_coros)

        logger.info('done rescan')
        progress_msgs.append(r"All done! \\(^_^)/")
        await msg.edit(content=progress_msg())

    def _rescan_members(self, ctx):
        """ Rescan the members of the ctx's guild.

            NOTE: This rescan will update existing members, but will not flush
                or invalidate members who have since left the guild.
        """

        Member = tinydb.Query()
        for u in ctx.guild.members:
            self._members.upsert({
                'id':               u.id,
                'name':             u.name,
                'discriminator':    u.discriminator,
                'nick':             u.nick,
                'guild':            ctx.guild.id,
            }, Member.id == u.id)

    async def _rescan_channel(self,
                             ctx: discord.ext.commands.Context,
                             channel: discord.TextChannel,
                             lookback_num=250,
                             lookback_time=datetime.timedelta(days=7),
                             force_sentinel=None):
        """
        Rescan the given channel to populate the message cache with all previously
        sent messages with emoji or reacts.

        To avoid excessive re-scanning of already-cached messages, we persist
        a "sentinel" datetime to the database indicating where to begin the next scan.

        Messages sent before the sentinel are considered final and will never be rescanned;
        messages sent after are considered tentative, and might have their cache entries
        invalidated (i.e. overwritten by newer data).

        The sentinel is chosen to be the earlier of:
        1) the datetime of the `lookback_num`'th most recent message in the channel, OR
        2) the datetime occurring `lookback_time` before the most recent message in the channel.

        WARNING: The sentinel is an imperfect heuristic; in particular it
                 assumes that messages will never be reacted to again once
                 they become old enough. (i.e. no "necro" reactions).

        TODO: The rescan currently only detects reactions to messages.
              It will not detect messages that contain emoji in the body,
              although it would be nice to track these occurrences as well.

        Args:
            ctx: The context from which the rescan was requested.
            channel: The channel to be rescanned.
            lookback_num: How many messages to look back for the next sentinel.
            lookback_time: How much time before the most recent message to look
                           back for the next sentinel.
            force_sentinel: datetime.datetime, If provided, force a rescan up
                            to this point in the past.

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
        if channels := self._channels.search(Channel.id == channel.id):
            if len(channels) != 1:
                logger.warning(f"Search for channel id {channel.id} expected 1 channel; yielded {channels}")
                return
            sentinel_datetime = stodt(channels[0]['sentinel_datetime'])

        if force_sentinel is not None:
            sentinel_datetime = force_sentinel

        async def insert_message_record(msg):
            """ Insert a `Message` record into the cache. """
            Message = tinydb.Query()
            self._messages.upsert({
                'id':       msg.id,
                'author':   msg.author.id,
                'channel':  channel.id,
                'datetime': dttos(msg.created_at),
            }, Message.id == msg.id)

            # Forcibly overwrite reacts to ensure deleted reacts are removed from cache.
            reacts = {str(r): [user.id for user in await r.users().flatten()] for r in msg.reactions}
            self._messages.update(dbops.set('reacts', reacts), Message.id == msg.id)

        def insert_emoji_record(msg):
            """ Insert an `Emoji` record into the cache. """
            Emoji = tinydb.Query()
            for r in msg.reactions:
                record = {}
                # type(r) == Union[discord.Emoji, discord.PartialEmoji, str]
                if type(r.emoji) == str:
                    if len(r.emoji) > 2:
                        logger.warning(f'found over-long unicode emoji {r.emoji}')
                    # Build an integer representing the unicode code point.
                    id = 0
                    for i, c in enumerate(reversed(r.emoji)):
                        id |= ord(c)
                        if i < len(r.emoji) - 1:
                            id <<= 16

                    record['id']        = id
                    record['name']      = r.emoji
                    record['custom']    = False
                else:
                    record['id']            = r.emoji.id
                    record['name']          = r.emoji.name
                    record['custom']        = True
                    record['url']           = str(r.emoji.url)
                    record['discord_str']   = str(r.emoji)
                    record['created_at']    = dttos(r.emoji.created_at)
                self._emoji.upsert(record, Emoji.id == record['id'])

        since_str = "forever ago" if not sentinel_datetime else dttos(sentinel_datetime)
        logger.info(f'Scanning channel history: {channel.name} since {since_str}')

        # Find all the reacts in the channel since our last sentinel
        # Maintain a sliding window of the last RESCAN_LAST_N messages
        # so we can construct a new sentinel.
        newest_msgs = deque(maxlen=lookback_num)
        async for msg in channel.history(limit=None, after=sentinel_datetime, oldest_first=True):
            if msg.reactions:
                insert_emoji_record(msg)
                await insert_message_record(msg)
            newest_msgs.append(msg)

        # Select and persist a new sentinel
        if newest_msgs:
            nth_newest_datetime = newest_msgs[0].created_at
            newest_datetime = newest_msgs[-1].created_at
            sentinel_datetime = min(nth_newest_datetime, newest_datetime - lookback_time)

            self._channels.upsert({
                'name': channel.name,
                'id': channel.id,
                'guild': ctx.guild.id,
                'sentinel_datetime': dttos(sentinel_datetime),
            }, Channel.id == channel.id)

        elapsed_time = time.time() - start_time
        logger.info(f'{channel.name} scan complete in {elapsed_time:.1f}s')

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

    def query_by_channel(self, ctx, channel_name: str):
        """ Return a tinydb query for messages sent to a specific channel. """
        channel_id = self.get_channel_id_by_name(ctx, channel_name)
        return tinydb.Query().channel == channel_id

    def query_by_author(self, ctx, author: str):
        """ Return a tinydb query for messages sent by a specific author. """
        user_ids = [u.id for u in self.get_users_by_name(ctx, author)]
        return tinydb.Query().author.test(lambda uid: uid in user_ids)

    def query_by_reactor(self, ctx, reactor: str):
        """ Return a tinydb query for messages reacted to by a specific user.

            Note: This filters by message, so the output will contain a list of
            messages that definitely have reactions by the requested reactor,
            and may also have other unrelated reactions.
            This might not be the desired behavior for this function long-term.
        """
        user_ids = [u.id for u in self.get_users_by_name(ctx, reactor)]
        def test_reactor(reacts):
            for user_id in user_ids:
                if any([user_id in reactors for reactors in reacts.values()]):
                    return True
            return False
        return tinydb.Query().reacts.test(test_reactor)

    def query_by_react(self, ctx, react: str):
        """ Return a tinydb query for messages reacted to with a specific react.
            Params:
                react, a substring of the discord reaction string we'd like to find.
                e.g. "pogg" would match against "<:poggers:0123456789>"

            Note: This filters by *message*, so the output will contain a list of
                  messages that definitely have the requested reaction, and may
                  also have other unrelated reactions.
                  In other words, it can be used to capture co-occurences of reacts.
        """
        def test_react(reacts):
            return any([react in r for r in reacts])
        return tinydb.Query().reacts.test(test_react)

    def query_by_before(self, ctx, before_date: datetime.datetime):
        """ Return a tinydb query for messages sent before a specific date. """
        test_before = lambda dt_str: stodt(dt_str) < stodt(before_date, fmt=YMD_FORMAT)
        return tinydb.Query().datetime.test(test_before)

    def query_by_after(self, ctx, after_date: datetime.datetime):
        """ Return a tinydb query for messages sent after a specific date. """
        test_after = lambda dt_str: stodt(dt_str) > stodt(after_date, fmt=YMD_FORMAT)
        return tinydb.Query().datetime.test(test_after)

    def query_message_cache(self, ctx, *args):
        """ Search the message cache with the given directives,
            and return a list of messages that match. """

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
        # If queries is empty, noop() acts as the default, matching all messages.
        logger.info(f"querying with: {queries}")
        merged_query = functools.reduce(operator.and_, queries, tinydb.Query().noop())

        return self._messages.search(merged_query)


def setup(bot):
    bot.add_cog(Cache(bot))
