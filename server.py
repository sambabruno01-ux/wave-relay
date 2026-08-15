import asyncio
import os
import json
import time
import websockets

ROOMS = {}
CLEANUP_TASKS = {}

async def broadcast_user_list(room_id):
    if room_id not in ROOMS:
        return
    
    users_data = {
        u: {
            "ping": data.get("ping", 0),
            "status": "online"
        }
        for u, data in ROOMS[room_id]["users"].items()
    }
    
    payload = json.dumps({
        "type": "USER_LIST",
        "room": room_id,
        "users": users_data
    })
    
    for u, data in list(ROOMS[room_id]["users"].items()):
        try:
            await data["ws"].send(payload)
        except Exception:
            pass

async def schedule_room_cleanup(room_id, delay=45):
    await asyncio.sleep(delay)
    if room_id in ROOMS and len(ROOMS[room_id]["users"]) == 0:
        del ROOMS[room_id]
        print(f"[x] Комната {room_id} удалена по таймауту неактивности.")
    if room_id in CLEANUP_TASKS:
        del CLEANUP_TASKS[room_id]

async def handler(websocket):
    user_room = None
    user_name = None
    try:
        async for message in websocket:
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                    mtype = data.get("type")

                    if mtype == "CHECK_ROOM":
                        r_id = data.get("room", "").strip()
                        exists = r_id in ROOMS
                        await websocket.send(json.dumps({
                            "type": "ROOM_STATUS",
                            "room": r_id,
                            "exists": exists
                        }))

                    elif mtype == "JOIN":
                        r_id = data.get("room", "").strip()
                        name = data.get("user", "").strip()
                        pwd = data.get("password", "").strip()

                        if not r_id or not name:
                            continue

                        if r_id in ROOMS:
                            # Отменяем удаление, если кто-то заходит в существующую комнату
                            if r_id in CLEANUP_TASKS:
                                CLEANUP_TASKS[r_id].cancel()
                                del CLEANUP_TASKS[r_id]

                            if ROOMS[r_id]["password"] and ROOMS[r_id]["password"] != pwd:
                                await websocket.send(json.dumps({
                                    "type": "AUTH_ERROR",
                                    "msg": "Неверный пароль от комнаты!"
                                }))
                                continue
                        else:
                            ROOMS[r_id] = {
                                "password": pwd,
                                "users": {}
                            }
                            print(f"[*] Создана комната: {r_id}")

                        user_room = r_id
                        user_name = name
                        
                        ROOMS[user_room]["users"][user_name] = {
                            "ws": websocket,
                            "ping": 0,
                            "last_seen": time.time()
                        }

                        print(f"[+] {user_name} вошел в {user_room} (Всего: {len(ROOMS[user_room]['users'])})")
                        await websocket.send(json.dumps({
                            "type": "JOIN_OK",
                            "room": user_room,
                            "user": user_name
                        }))
                        await broadcast_user_list(user_room)

                    elif mtype == "PING":
                        ts = data.get("ts", time.time())
                        if user_room and user_room in ROOMS and user_name in ROOMS[user_room]["users"]:
                            ROOMS[user_room]["users"][user_name]["last_seen"] = time.time()
                        
                        await websocket.send(json.dumps({
                            "type": "PONG",
                            "ts": ts
                        }))

                    elif mtype == "REPORT_PING":
                        ping_ms = data.get("ping", 0)
                        if user_room and user_room in ROOMS and user_name in ROOMS[user_room]["users"]:
                            ROOMS[user_room]["users"][user_name]["ping"] = ping_ms
                            await broadcast_user_list(user_room)

                    elif mtype == "CHAT":
                        if user_room and user_room in ROOMS:
                            chat_payload = json.dumps({
                                "type": "CHAT",
                                "room": user_room,
                                "sender": user_name,
                                "text": data.get("text", "")
                            })
                            for peer_name, pdata in list(ROOMS[user_room]["users"].items()):
                                try:
                                    await pdata["ws"].send(chat_payload)
                                except Exception:
                                    pass

                except Exception as ex:
                    print(f"[!] JSON Error: {ex}")

            elif isinstance(message, bytes):
                if user_room and user_room in ROOMS:
                    if user_name in ROOMS[user_room]["users"]:
                        ROOMS[user_room]["users"][user_name]["last_seen"] = time.time()
                    
                    for peer_name, pdata in list(ROOMS[user_room]["users"].items()):
                        if peer_name != user_name:
                            try:
                                await pdata["ws"].send(message)
                            except Exception:
                                pass

    except Exception as e:
        print(f"[-] Отключение {user_name}: {e}")
    finally:
        if user_room and user_room in ROOMS:
            if user_name in ROOMS[user_room]["users"]:
                # Удаляем только если это сокет текущей сессии
                if ROOMS[user_room]["users"][user_name].get("ws") == websocket:
                    del ROOMS[user_room]["users"][user_name]
                    print(f"[-] {user_name} отключился от {user_room}")
            
            if not ROOMS[user_room]["users"]:
                if user_room not in CLEANUP_TASKS:
                    CLEANUP_TASKS[user_room] = asyncio.create_task(schedule_room_cleanup(user_room))
            else:
                await broadcast_user_list(user_room)

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
        print(f"[*] Wave Master Server запущен на порту {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
