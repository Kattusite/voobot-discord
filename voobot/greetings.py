from discord.ext import commands

class Greetings(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command()
    async def hello(self, ctx):
        await ctx.send(f'Hello {ctx.author}')

    @commands.command()
    async def gnite(self, ctx):
        await ctx.send(f'Good night {ctx.author}')

def setup(bot):
    bot.add_cog(Greetings(bot))
