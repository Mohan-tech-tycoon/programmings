import asyncio


async def fetch_data(val):
    print("Fethcing Data")
    await asyncio.sleep(2)
    return {"result": val}


async def counter():
    for i in range(15):
        await asyncio.sleep(0.5)
        print(i)


async def main_test_1():
    await fetch_data(val = 1)
    await counter()


async def main_test_2():
    task1 =  asyncio.create_task(fetch_data(val = 1))
    task2 =  asyncio.create_task(counter())

    data: dict = await task1
    print(f"Task1 Result: {data}")


async def main_test_3():
    task1 =  asyncio.create_task(fetch_data(val = 1))
    task2 =  asyncio.create_task(counter())

    data: dict = await task1
    print(f"Task1 Result: {data}")

    await task2


if __name__ == "__main__":
    # asyncio.run(main_test_1())
    # asyncio.run(main_test_2())
    asyncio.run(main_test_3())



"""

------------------------------------------------
Arrived Results from the below test:
------------------------------------------------

Main Test 1:
------------------------
Fethcing Data
0
1
2
3
....
12
13
14


Main Test 2:
------------------------
Fethcing Data
0
1
2
Task1 Result: {'result': 1}


Main Test 3:
------------------------
Fethcing Data
0
1
2
Task1 Result: {'result': 1}
3
4
5
....
12
13
14

"""