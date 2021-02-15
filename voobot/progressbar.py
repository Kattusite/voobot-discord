import asyncio

import logging
logger = logging.getLogger(__name__)


# By convention, the last symbol represents 100%, so we might repeat a bit..
# TODO: Make these tuples (list, final_state) instead to allow less jank and better customizability
# QUESTION: Do we want to couple the "done" emoji and "loading" emoji, eg in a tuple,
#           or does it make more sense to specify it separately?
#           My suspicion is the latter, add a `final_state` param instead of making a tuple
MOONS = "🌑🌒🌓🌔🌕🌖🌗🌘"
FULL_MOON = "🌕"

TYPING = "typing"

class ProgressBar():
    """ A progress bar attached to a message by iteratively applying reactions. """

    def __init__(self, bot, msg, delay=1, reacts=MOONS, final_react=FULL_MOON):
        """ Create a new progress bar attached to msg.
            Params:
                - msg: The context to attach to.
                - delay: Delay in seconds between updates
                - reacts: An iterable of emoji to react with in sequence.
                - final_react: The emoji to react with upon completion.
            Note: if `reacts` == TYPING, this class mimics the functionality
                  of discord.py's channel.typing() context manager.
                  (i.e. "Voobot is typing...")
                  Presently, the final_react is ignored in this case,
                  but this behavior may change in future versions.
        """
        self.bot = bot
        self.msg = msg
        self.delay = delay
        self.reacts = reacts
        self._stopped = False
        self._typing_mgr = None # Unused unless we're using the channel.typing() manager.

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

        # If requested, don't use the emoji progress bar, and just use typing instead.
        if self.reacts == TYPING:
            logger.debug('using default typing() context manager...')
            self._typing_mgr = self.msg.channel.typing()
            return self._typing_mgr.__enter__()

        task = asyncio.create_task(self._progress_bar())
        return task

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug('exiting; stopping')
        self._stopped = True

        if self._typing_mgr:
            return self._typing_mgr.__exit__(exc_type, exc_val, exc_tb)
