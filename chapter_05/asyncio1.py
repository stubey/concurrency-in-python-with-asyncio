#!/usr/bin/env python

from datetime import datetime
import asyncio
from asyncio.events import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import List, Tuple

def count(count_to: int) -> Tuple[int, int]:
    counter = 0
    while counter < count_to:
        counter = counter + 1
    return counter, 999

async def main():
    with ProcessPoolExecutor() as process_pool:
        loop: AbstractEventLoop = asyncio.get_running_loop()
            
        nums = [100000001, 1, 3, 5, 22, 100000000]
        calls: List[partial[int]] = [partial(count, num) for num in nums]
            
        call_coros = []
        for call in calls:
            call_coros.append(loop.run_in_executor(process_pool, call))
        
        # Wait for all to complete
        # results = await asyncio.gather(*call_coros)
        # for result in results:
        #     print(f"{datetime.now()}: {result}")      

        # Process as available
        for finished_task in asyncio.as_completed(call_coros):
            counted, scum = await finished_task
            print(f"{datetime.now()}: {counted}")    
        
        
        
    
if __name__ == "__main__":
    asyncio.run(main())

