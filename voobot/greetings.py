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
        if ctx.author.name == "JPirate":
            await ctx.send('🍓')
            return
        await ctx.send('🦍')

    @commands.command()
    async def monkw(self, ctx):
        await ctx.send(':jajreen:')

def setup(bot):
    bot.add_cog(Greetings(bot))
