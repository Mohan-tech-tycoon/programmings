import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime



class AsyncAPIFileDownloader:
    BATCH_SIZE = 18
    @staticmethod
    async def fetch_files(request_session: aiohttp.ClientSession, request_url: str, output_file: str):
        ## Asynchronous function to fetch files and save in output dir
        # print(request_url)
        # print(request_session)
        print(output_file)
        async with request_session.get(request_url) as response:
            # print(response)
            with open(output_file, "wb") as file_writer:
                file_writer.write(await response.read())
                print("Write Success..!!")

    async def process_request(self, urls: list, batch_order: str, output_dir: str):
        ## Process to run the urls in batches for API calls 
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_files(request_session=session, request_url=url, output_file=os.path.join(output_dir, f"filename_{batch_order}_{i}.pdf"))
                for i, url in enumerate(urls)
            ]
            await asyncio.gather(*tasks)

    @staticmethod
    def create_batches(urls: list, batch_size: int):
        ## Creates a batches and yields the data
        for i in range(0, len(urls), batch_size):
            print(i, i + batch_size)
            yield urls[i: i + batch_size]
        
    def download_request(self, urls: list, output_dir: str):
        # Download request - Main block - Create Thread Pool Execution
        looper = asyncio.new_event_loop()
        asyncio.set_event_loop(looper)
        
        counter = 1
        for url in self.create_batches(urls, self.BATCH_SIZE):
            batch_order = f"batch_{counter}"
            looper.run_until_complete(self.process_request(url, batch_order, output_dir))
            counter += 1
            
        print(f"Total Batches: {counter}")

    """
    main() method is the sample for defining multiple files request
    """
    def run(self):
        urls = ["https://education.github.com/git-cheat-sheet-education.pdf"*i for i in range(1,100)]
        output_dir = os.path.join(os.path.dirname(__file__), "src")
        print(datetime.now())
        with ThreadPoolExecutor() as exec:
            exec.submit(self.download_request, urls, output_dir)
        print(datetime.now())        
        
if __name__ == "__main__":
    AsyncAPIFileDownloader().run()
