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
                    _, room_id, name = message.split(":", 2)
                    user_room = room_id
                    user_name = name
                    
                    if user_room not in ROOMS:
                        ROOMS[user_room] = {}
                    ROOMS[user_room][user_name] = websocket
                    print(f"[+] {user_name} подключился к комнате {user_room}")

            elif isinstance(message, bytes):
                if user_room and user_room in ROOMS:
                    # Рассылаем всем участникам кроме отправителя
                    for peer_name, peer_ws in list(ROOMS[user_room].items()):
                        if peer_name != user_name and not peer_ws.closed:
                            try:
                                await peer_ws.send(message)
                            except Exception:
                                pass

    except Exception as e:
        print(f"[-] Ошибка клиента {user_name}: {e}")
    finally:
        if user_room and user_room in ROOMS and user_name in ROOMS[user_room]:
            del ROOMS[user_room][user_name]
            print(f"[-] {user_name} отключился от комнаты {user_room}")
            if not ROOMS[user_room]:
                del ROOMS[user_room]

async def main():
    port = int(os.environ.get("PORT", 8765))
    # Включаем ping_interval=20 и ping_timeout=20, чтобы Render не рвал связь
    async with websockets.serve(
        handler, 
        "0.0.0.0", 
        port,
        ping_interval=20,
        ping_timeout=20,
        max_size=10 * 1024 * 1024
    ):
        print(f"[*] Wave WebSocket Relay активен на порту {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
