import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime


BATCH_SIZE = 18

async def fetch_files(request_session: aiohttp.ClientSession, request_url: str, output_file: str, params: str=None):
    ## Asynchronous function to fetch files and save in output dir
    # print(request_url)
    # print(request_session)
    print(output_file)
    async with request_session.get(request_url, params=params) as response:
        # print(response)
        with open(output_file, "wb") as file_writer:
            file_writer.write(await response.read())
            print("Write Success..!!")

async def process_request(urls: str):
    for i, val in enumerate(urls):
        print(i, val["url"])
    ## Process to run the urls in batches for API calls 
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_files(request_session=session, request_url=val["url"], output_file=val["output_filename"], params=val["params"])
            for i, val in enumerate(urls)
        ]
        await asyncio.gather(*tasks)

def create_batches(urls: list, batch_size: int):
    ## Creates a batches and yields the data
    for i in range(0, len(urls), batch_size):
        print(i, i + batch_size)
        yield urls[i: i + batch_size]
    
def download_request(urls: list):
    # Download request - Main block - Create Thread Pool Execution
    looper = asyncio.new_event_loop()
    asyncio.set_event_loop(looper)
    
    counter = 1
    for val in create_batches(urls, BATCH_SIZE):
        print(val)
        batch_order = f"batch_{counter}"
        looper.run_until_complete(process_request(val))
        counter += 1
        
    print(f"Total Batches: {counter}")

def main():
    output_dir = r".\async_download_url\src"
    urls = [
        {"url": "https://education.github.com/git-cheat-sheet-education.pdf", "params":None, "output_filename": os.path.join(output_dir, "filename_0_1.pdf")},
        {"url": "https://education.github.com/git-cheat-sheet-education.pdf", "params":None, "output_filename": os.path.join(output_dir, "filename_0_2.pdf")},
        {"url": "https://education.github.com/git-cheat-sheet-education.pdf", "params":None, "output_filename": os.path.join(output_dir, "filename_0_3.pdf")},
        ]
    
    print(datetime.now())
    with ThreadPoolExecutor() as exec:
        exec.submit(download_request, urls)
    print(datetime.now())        
        
if __name__ == "__main__":
    main()
