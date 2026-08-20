import asyncio
import os
import json
import time
import websockets

ROOMS = {}
CLEANUP_TASKS = {}
ROOM_PERSIST_DAYS = 7
INACTIVITY_TIMEOUT = 45

async def broadcast_user_list(room_id):
    if room_id not in ROOMS:
        return
    
    users_data = {
        u: {
            "ping": data.get("ping", 0),
            "status": "online",
            "mic_muted": data.get("mic_muted", False),
            "deafened": data.get("deafened", False),
            "self_listen": data.get("self_listen", False),
            "is_soundpad": data.get("is_soundpad", False)
        }
        for u, data in ROOMS[room_id]["users"].items()
    }
    
    payload = json.dumps({
        "type": "USER_LIST",
        "room": room_id,
        "users": users_data,
        "reserved": ROOMS[room_id].get("reserved", False),
        "expire_at": ROOMS[room_id].get("expire_at", 0)
    })
    
    for u, data in list(ROOMS[room_id]["users"].items()):
        try:
            await data["ws"].send(payload)
        except Exception:
            pass

async def schedule_room_cleanup(room_id, delay=INACTIVITY_TIMEOUT):
    await asyncio.sleep(delay)
    if room_id in ROOMS:
        if ROOMS[room_id].get("reserved", False):
            if time.time() >= ROOMS[room_id].get("expire_at", 0):
                del ROOMS[room_id]
                print(f"[x] Reserved room {room_id} expired after 7 days of inactivity.")
        elif len(ROOMS[room_id]["users"]) == 0:
            del ROOMS[room_id]
            print(f"[x] Temporary room {room_id} deleted on inactivity.")
            
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
                        room_users = {}
                        is_reserved = False
                        if exists:
                            is_reserved = ROOMS[r_id].get("reserved", False)
                            room_users = {
                                u: {"ping": d.get("ping", 0)}
                                for u, d in ROOMS[r_id]["users"].items()
                            }
                        await websocket.send(json.dumps({
                            "type": "ROOM_STATUS",
                            "room": r_id,
                            "exists": exists,
                            "reserved": is_reserved,
                            "users": room_users
                        }))

                    elif mtype == "JOIN":
                        r_id = data.get("room", "").strip()
                        name = data.get("user", "").strip()
                        pwd = data.get("password", "").strip()
                        mic_muted = data.get("mic_muted", False)
                        deafened = data.get("deafened", False)
                        self_listen = data.get("self_listen", False)
                        wants_reserve = data.get("reserve", False)

                        if not r_id or not name:
                            continue

                        if r_id in ROOMS:
                            if r_id in CLEANUP_TASKS:
                                CLEANUP_TASKS[r_id].cancel()
                                del CLEANUP_TASKS[r_id]

                            if ROOMS[r_id]["password"] and ROOMS[r_id]["password"] != pwd:
                                await websocket.send(json.dumps({
                                    "type": "AUTH_ERROR",
                                    "msg": "Неверный пароль от комнаты!"
                                }))
                                continue
                            
                            if wants_reserve or ROOMS[r_id].get("reserved", False):
                                ROOMS[r_id]["reserved"] = True
                                ROOMS[r_id]["expire_at"] = time.time() + (ROOM_PERSIST_DAYS * 86400)
                        else:
                            expire_time = time.time() + (ROOM_PERSIST_DAYS * 86400) if wants_reserve else 0
                            ROOMS[r_id] = {
                                "password": pwd,
                                "reserved": wants_reserve,
                                "expire_at": expire_time,
                                "users": {}
                            }
                            print(f"[*] Создана комната: {r_id} (Резерв: {wants_reserve})")

                        user_room = r_id
                        user_name = name
                        
                        ROOMS[user_room]["users"][user_name] = {
                            "ws": websocket,
                            "ping": 0,
                            "mic_muted": mic_muted,
                            "deafened": deafened,
                            "self_listen": self_listen,
                            "is_soundpad": False,
                            "last_seen": time.time()
                        }

                        print(f"[+] {user_name} вошел в {user_room} (Всего: {len(ROOMS[user_room]['users'])})")
                        await websocket.send(json.dumps({
                            "type": "JOIN_OK",
                            "room": user_room,
                            "user": user_name,
                            "reserved": ROOMS[user_room]["reserved"]
                        }))
                        await broadcast_user_list(user_room)

                    elif mtype == "TOGGLE_RESERVE":
                        if user_room and user_room in ROOMS:
                            state = data.get("reserve", False)
                            ROOMS[user_room]["reserved"] = state
                            if state:
                                ROOMS[user_room]["expire_at"] = time.time() + (ROOM_PERSIST_DAYS * 86400)
                            await broadcast_user_list(user_room)

                    elif mtype == "UPDATE_STATE":
                        if user_room and user_room in ROOMS and user_name in ROOMS[user_room]["users"]:
                            if "mic_muted" in data:
                                ROOMS[user_room]["users"][user_name]["mic_muted"] = data["mic_muted"]
                            if "deafened" in data:
                                ROOMS[user_room]["users"][user_name]["deafened"] = data["deafened"]
                            if "self_listen" in data:
                                ROOMS[user_room]["users"][user_name]["self_listen"] = data["self_listen"]
                            if "is_soundpad" in data:
                                ROOMS[user_room]["users"][user_name]["is_soundpad"] = data["is_soundpad"]
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
                        if peer_name != user_name or pdata.get("self_listen", False):
                            try:
                                await pdata["ws"].send(message)
                            except Exception:
                                pass

    except Exception as e:
        print(f"[-] Отключение {user_name}: {e}")
    finally:
        if user_room and user_room in ROOMS:
            if user_name in ROOMS[user_room]["users"]:
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
        ping_interval=10,
        ping_timeout=10,
        max_size=10 * 1024 * 1024,
        max_queue=128
    ):
        print(f"[*] Wave Master Server запущен на порту {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())