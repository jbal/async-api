
import sys
import os
sys.path.insert(0, os.getcwd() + "/..")
print(sys.path)

from api.api import AsyncApi


base_url = "https://api2.realtor.ca/"
headers = {"referer": "https://www.realtor.ca/"}
params = {
  "ZoomLevel": 4,
  "LatitudeMax": 63.81644,
  "LongitudeMax": -57.96387,
  "LatitudeMin": 43.50872,
  "LongitudeMin": -140.66895,
  "Sort": "6-D",
  "PropertyTypeGroupID": 1,
  "TransactionTypeId": 2,
  "PropertySearchTypeId": 0,
  "Currency": "CAD",
  "IncludeHiddenListings": False,
  "RecordsPerPage": 12,
  "ApplicationId": 1,
  "CultureId": 1,
  "Version": 7.0,
  "CurrentPage": 1
}


class TestApi(AsyncApi):
    
    @AsyncApi.awaitable
    def test(self):

        async def _test(i):
            asyncio.sleep(1)
            print(i)
            return i
        
        tasks = []
        for i in range(20):
            tasks.append(_test(i))
        
        return tasks


async def main():
    # api  = AsyncApi(base_url, headers=headers)
    # out = await api.post("/Listing.svc/PropertySearch_Post", data=params)
    # print(out)

    api = TestApi(base_url="")
    for fut in api.test():
        result = await fut
        print(type(result))
        print(result)

import asyncio
asyncio.run(main())