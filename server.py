import asyncio
import os
import websockets

ROOMS = {}

async def handler(websocket):
    user_room = None
    user_name = None
    try:
        async for message in websocket:
            if isinstance(message, str):
                if message.startswith("JOIN:"):
                    parts = message.split(":", 2)
                    if len(parts) == 3:
                        _, room_id, name = parts
                        user_room = room_id
                        user_name = name
                        
                        if user_room not in ROOMS:
                            ROOMS[user_room] = {}
                        ROOMS[user_room][user_name] = websocket
                        print(f"[+] {user_name} вошел в {user_room} (Всего: {len(ROOMS[user_room])})")

                elif message.startswith("PING:"):
                    # Эхо пинга назад клиенту для вычисления задержки RTT
                    try:
                        await websocket.send(message.replace("PING:", "PONG:"))
                    except Exception:
                        pass

                elif message.startswith("CHAT:"):
                    # Текстовое сообщение: CHAT:room:sender:text
                    if user_room and user_room in ROOMS:
                        for peer_name, peer_ws in list(ROOMS[user_room].items()):
                            try:
                                await peer_ws.send(message)
                            except Exception:
                                pass

            elif isinstance(message, bytes):
                if user_room and user_room in ROOMS:
                    for peer_name, peer_ws in list(ROOMS[user_room].items()):
                        if peer_name != user_name:
                            try:
                                await peer_ws.send(message)
                            except Exception:
                                pass

    except Exception as e:
        print(f"[-] Исключение {user_name}: {e}")
    finally:
        if user_room and user_room in ROOMS:
            if user_name in ROOMS[user_room]:
                del ROOMS[user_room][user_name]
                print(f"[-] {user_name} покинул {user_room}")
            if not ROOMS[user_room]:
                del ROOMS[user_room]
                print(f"[x] Комната {user_room} удалена с сервера")

async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        ping_interval=20,
        ping_timeout=20,
        max_size=10 * 1024 * 1024,
        max_queue=128
    ):
        print(f"[*] Wave WebSocket Relay V3 запущен на порту {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
