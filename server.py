import asyncio
import os
import websockets

# Хранилище подключений по комнатам: { room_id: { username: websocket } }
ROOMS = {}

async def handler(websocket):
    user_room = None
    user_name = None
    try:
        async for message in websocket:
            # Если сообщение строковое — это служебная команда (регистрация в комнате)
            if isinstance(message, str):
                if message.startswith("JOIN:"):
                    _, room_id, name = message.split(":", 2)
                    user_room = room_id
                    user_name = name

                    if user_room not in ROOMS:
                        ROOMS[user_room] = {}
                    ROOMS[user_room][user_name] = websocket
                    print(f"[+] {user_name} вошел в комнату {user_room}")

            # Если сообщение бинарное (bytes) — это аудио-пакет
            elif isinstance(message, bytes):
                if user_room and user_room in ROOMS:
                    # Рассылаем аудио всем участникам комнаты, кроме отправителя
                    for peer_name, peer_ws in list(ROOMS[user_room].items()):
                        if peer_name != user_name and peer_ws.open:
                            try:
                                await peer_ws.send(message)
                            except Exception:
                                pass

    except Exception as e:
        print(f"[-] Ошибка клиента {user_name}: {e}")
    finally:
        if user_room and user_room in ROOMS and user_name in ROOMS[user_room]:
            del ROOMS[user_room][user_name]
            print(f"[-] {user_name} покинул комнату {user_room}")
            if not ROOMS[user_room]:
                del ROOMS[user_room]

async def main():
    # Render передает порт через системную переменную PORT
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"[*] Wave WebSocket Relay запущен на порту {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
