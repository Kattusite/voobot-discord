import asyncio

class Counter():
    def __init__(self):
        self.stop = False

    async def count(self):
        while not self.stop:
            print('x')
            await asyncio.sleep(1)
        print('y')


    def __enter__(self):
        task = asyncio.create_task(self.count())
        return task

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop = True


async def count():
    for i in range(99999):
        print(i)
        await asyncio.sleep(1)

async def main():
    # task = asyncio.create_task(count())

    # with Counter():
    #     await asyncio.sleep(5)
    # task.cancel()

    scan_coros = [asyncio.to_thread(asyncio.sleep, i) for i in range(5)]
    await asyncio.gather(*scan_coros)

asyncio.run(main())
