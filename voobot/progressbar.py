import asyncio

import logging
logger = logging.getLogger(__name__)


# By convention, the last symbol represents 100%, so we might repeat a bit..
MOONS = "🌑🌒🌓🌔🌕🌖🌗🌘🌑🌒🌓🌔🌕"

class ProgressBar():
    """ A progress bar attached to a message by iteratively applying reactions. """

    def __init__(self, bot, msg, delay=1, reacts=MOONS):
        """ Create a new progress bar attached to msg.
            Params:
                - msg: The Discord message to attach to
                - delay: Delay in seconds between updates
                - reacts: An iterable of emoji to react with in sequence
        """
        self.bot = bot
        self.msg = msg
        self.delay = delay
        self.reacts = reacts
        self._stopped = False

    async def _progress_bar(self):
        """ Update the progress bar, looping until stopped. """
        i = 0
        await self.msg.add_reaction(self.reacts[i])

        while not self._stopped:
            next = (i+1) % len(self.reacts)
            await asyncio.gather(
                self.msg.add_reaction(self.reacts[next]),
                self.msg.remove_reaction(self.reacts[i], self.bot.user))
            await asyncio.sleep(self.delay)
            i = next

        # At the end, add the final react
        await asyncio.gather(
            self.msg.add_reaction(self.reacts[-1]),
            self.msg.remove_reaction(self.reacts[i], self.bot.user))

    def __enter__(self):
        """ Spawn an a async task to update the progress bar repeatedly. """
        logger.debug('entering; spawning thread')
        task = asyncio.create_task(self._progress_bar())
        return task

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug('exiting; stopping')
        self._stopped = True
