import asyncio
import websockets
import json




async def main():
    base_url = "wss://stream.binance.com:9443/stream?streams="
    path = "config/initial_streams.json"
    with open(path) as f:
        data = json.load(f)
        streams = [asset.replace('/', '').lower() + "@kline_1m" for asset in data["assets"]]
        url = base_url + '/'.join(streams)  
    async with websockets.connect(url) as ws:
            msg = await ws.recv()
            data = json.loads(msg)
            k = data["data"]["k"]
            if k["x"]:
                print(data)
                print(data["stream"])
                for key, value in data["data"].items():
                    print(f"{key}: {value}")
                for key, value in data["data"]["k"].items():
                    print(f"{key}: {value}")
           
if __name__ == "__main__":
    asyncio.run(main())