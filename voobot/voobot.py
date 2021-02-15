# bot.py
# Building off https://realpython.com/how-to-make-a-discord-bot-python/#what-is-discord
# See also: https://discordpy.readthedocs.io/en/latest/ext/commands/commands.html
#           which would be useful potentially...

import discord
from discord.ext import commands

import asyncio
import logging
logging.basicConfig(level=logging.INFO)

from .emojistats import EmojiStats

logger = logging.getLogger(__name__)

class VooBot(commands.Bot):
    """ The discord Bot functionality. """

    def __init__(self, command_prefix):
        super().__init__(command_prefix)
        self.register_cogs()

    def register_cogs(self):
        print('foo')
        logger.info('Registering cogs...')
        self.add_cog(EmojiStats(self))


"""
    @self.client.event
    async def on_ready():
        logger.info(f'{client.user} has connected to Discord!')
        logger.info(f'{client.user} is connected to the following guilds:')

        for i, guild in enumerate(client.guilds):
            print(f'  - {guild.name:20s} (id: {guild.id})')
            if i >= 10:
                print(f'  - ... and {len(client.guilds-10)} more')
                break

    @self.client.event
    async def on_message(message):

        if message.content.startswith("+slot "):
            await handle_slot(message)

        if message.content.startswith("++slot"):
            await handle_slot_stats(message)

        if message.content.startswith("+crypto balance"):
            await handle_crypto_balance(message)

        if message.content.startswith("+crypto list"):
            await handle_crypto_list(message)
"""
"""
async def handle_slot(message):
    global slot_ratio_sum
    channel = message.channel
    sender = f'{message.author.name}'

    match = re.match("\+(?P<command>\w+) (?P<wager>\d+)(?P<pct>\%?)", message.content)
    gd = match.groupdict()
    game   = gd['command']
    isPercent = 'pct' in gd and gd['pct'] == '%'
    wager  = int(gd['wager'].replace(",", ""))

    # For now only check slot machine
    # if game != "slot":

    #     return
    # Now redundant because this function only runs for +slot commands

    print(f'Bet received: {wager} credits for {game}, by {sender}')
    # await message.channel.send(f"{len(bets)} bets submitted")

    # BUG: If two players concurrently roll slots,
    # it may not be guaranteed which player receives which
    # results, and so they might be unreliable
    def check(m):
        if m.author.name != "gambling bot":
            return False
        return True
        # embed_dict = get_embed_dict(m)
        # if not embed_dict or not 'fields' in embed_dict:
        #     return False
        # fields = embed_dict['fields']
        # print(fields[0]['name'], player)
        # if player not in fields[0]['name']:
        #     return False
        # return True

    msg = await client.wait_for('message', check=check)
    id = msg.id

    # Wait for bot to finish updating the message
    await asyncio.sleep(7)
    msg = await channel.fetch_message(id)
    if not msg.embeds or len(msg.embeds) == 0:
        return
    embed = msg.embeds[0]
    embed_dict = embed.to_dict()
    # print(embed_dict)

    stats = parse_slot_embed(embed_dict)
    stats['wager'] = float(wager)
    if stats['winnings'] > 0:
        stats['winnings'] -= wager
    stats['ratio'] = stats['winnings'] / stats['wager']

    # There's something wrong with the emoji in `roll` where they only
    # render as the unicode replacement character, so we'll have to omit them
    # for now
    del stats["roll"]
    print(stats)

    # if its a percentage bet, calculate how much money was held before,
    # and take `wager` percent of that
    if isPercent:
        #old_holdings = stats['holdings'] - stats['winnings']
        #stats['wager'] = old_holdings * (wager / 100)

        # BUG: winnings was computed using wager, which is not yet accurate.
        # so all of the numbers will still be wildly wrong
        with open("slot_pct.csv", "a") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([
                stats['player'],
                stats['wager'],
                stats['winnings'],
                stats['ratio'],
                stats['holdings'],
                stats['xp']
            ])
        return

    bets.append(stats)
    slot_ratio_sum += stats['ratio']

    # race condition?
    # does python allow concurrent opens of the same file?
    with open("slot.csv", "a") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([
            stats['player'],
            stats['wager'],
            stats['winnings'],
            stats['ratio'],
            stats['holdings'],
            stats['xp']
        ])


async def handle_slot_stats(message):
    n = len(bets)
    avg_ratio = 1 + (slot_ratio_sum / n)
    # await message.channel.send(f'Of {n} games played, the average payoff is {avg_ratio:.2f}x')

    # plot = plotter.plot_slot([b["ratio"] for b in bets])
    # await message.channel.send(
    #     f'Of {n} games played, the average payoff is {avg_ratio:.2f}x',
    #     file=discord.File(plot, 'slot_win_ratios.png')
    # )
    # plot.close()

    ratios = [b["ratio"] for b in bets]
    c = Counter(ratios)
    counts = c.most_common()
    counts.sort(key=lambda c: c[0])
    simplified = {val: utils.simplify(x/n) for (val, x) in counts}
    strs = {val: f"{simplified[val][0]} in {simplified[val][1]}" for (val, _) in counts}
    lines = [f"{val}x:\t\t {strs[val]} \t({(x/n)*100:.2f}%)" for (val, x) in counts]
    output = "\n".join(lines)

    embed=discord.Embed(
        title="Slot machine payout frequency",
        description=f"Of {n} games played, the average payoff is {avg_ratio:.2f}x",
        color=0xfbd309
    )
    embed.set_footer(text=output)
    await message.channel.send(embed=embed)
"""
