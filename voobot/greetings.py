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

    @commands.command()
    async def monke(self, ctx):
        msg = '🦍'
        await ctx.send(msg)

    @commands.command()
    async def monkw(self, ctx):
        await ctx.send('<:jajreen:691777761043284009>')

def setup(bot):
    bot.add_cog(Greetings(bot))
