import discord
from discord.ext import commands

import asyncio
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

###########################################################
##                     EmojiStats
###########################################################

class EmojiStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.group()
    async def emoji(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid emoji command passed...')

    @emoji.command()
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

        msgs = self.bot.cache.query_message_cache(ctx, *args)
        collated_msgs = self.collate_messages(ctx, msgs, *args)
        await self.display_emoji_stats(ctx, collated_msgs, *args)

    def collate_messages(self, ctx, messages, *args, strict_matching=False):
        """ Transform the provided list of messages into a clean set of results that
            can be easily displayed, per the format requested by args.

            By default, this format will be a dictionary mapping emoji to their
            aggregate number of occurrences across the entire set of messages.
        """
        # TODO: If strict matching is true, filter out only the specific reacts
        #       on each message that were searched for by `args`,
        #       rather than including all the reacts on all messages that matched.
        #       (i.e. ignore/allow co-occurences.)
        #
        #       For example, if args includes "by:voobot", and strict_matching=True,
        #       the collated list should not include any reactions by any user besides
        #       voobot. Inversely, for strict_matching=False, the collated list could
        #       include reactions by other users, so long as those reactions were
        #       from messages that voobot also reacted to.

        # TODO: Implement other collation formats, besides just a dictionary
        #       mapping emoji to occurrence counts.
        #       e.g. Map users to their most frequent reactions.

        # messages looks like this:
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


    # please clap
    # assign a random react to the message


def setup(bot):
    bot.add_cog(EmojiStats(bot))
