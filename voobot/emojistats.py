import discord
from discord.ext import commands

from collections import defaultdict

class EmojiStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # Read emoji history.

        # If the cache file exists, read from it
        # I expect entries to look like this:
        #   time,channel,emoji,sender,receiver

        # Search through available channels to find new messages
        # (those sent since the most recent message in cache)
        # NOTE: This assumes no reacts are added to old messages.
        #       Perhaps do min(1 week, oldest cached)
        # Something like:
        #           always check last 100 msgs, and messages newer than a week
        #           but read all other results from cache


        # TODO: This also doesn't include messages that are themselves emoji, or that include emoji
        # These can be:
        #   time,channel,emoji,sender,

    # def read_from_cache()

    # def read_new_messages()

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
    async def hello(self, ctx):
        await ctx.send(f'Hello {ctx.author}')

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
