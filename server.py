import asyncio
import os
import json
import time
import websockets

# ROOMS: { room_id: { "password": pwd, "users": { username: { "ws": ws, "last_seen": t, "ping": 0 } } } }
ROOMS = {}

async def broadcast_room_state(room_id):
    if room_id not in ROOMS:
        return
    
    users_info = {
        u: { "ping": data.get("ping", 0), "status": "online" }
        for u, data in ROOMS[room_id]["users"].items()
    }
    msg = json.dumps({
        "type": "USER_LIST",
        "room": room_id,
        "users": users_info
    })
    
    for u, data in list(ROOMS[room_id]["users"].items()):
        try:
            await data["ws"].send(msg)
        except Exception:
            pass

async def handler(websocket):
    user_room = None
    user_name = None
    try:
        async for message in websocket:
            # 1. ТЕКСТОВЫЕ КОМАНДЫ (JSON)
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                    mtype = data.get("type")

                    # Проверка статуса комнаты перед входом
                    if mtype == "CHECK_ROOM":
                        r_id = data.get("room", "").strip()
                        exists = r_id in ROOMS
                        await websocket.send(json.dumps({
                            "type": "ROOM_STATUS",
                            "room": r_id,
                            "exists": exists
                        }))

                    # Вход или создание комнаты с паролем
                    elif mtype == "JOIN":
                        r_id = data.get("room", "").strip()
                        name = data.get("user", "").strip()
                        pwd = data.get("password", "").strip()

                        if r_id in ROOMS:
                            if ROOMS[r_id]["password"] != pwd:
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
                            print(f"[*] Создана новая комната: {r_id}")

                        user_room = r_id
                        user_name = name
                        ROOMS[user_room]["users"][user_name] = {
                            "ws": websocket,
                            "last_seen": time.time(),
                            "ping": 0
                        }

                        print(f"[+] {user_name} вошел в {user_room} (Всего: {len(ROOMS[user_room]['users'])})")
                        await websocket.send(json.dumps({
                            "type": "JOIN_OK",
                            "room": user_room,
                            "user": user_name
                        }))
                        await broadcast_room_state(user_room)

                    # Пинг-Понг для измерения RTT и поддержания активности
                    elif mtype == "PING":
                        ts = data.get("ts", time.time())
                        if user_room and user_room in ROOMS and user_name in ROOMS[user_room]["users"]:
                            ROOMS[user_room]["users"][user_name]["last_seen"] = time.time()
                        
                        await websocket.send(json.dumps({
                            "type": "PONG",
                            "ts": ts
                        }))

                    # Отчет клиента о его пинге (для рассылки всем участникам)
                    elif mtype == "REPORT_PING":
                        ping_val = data.get("ping", 0)
                        if user_room and user_room in ROOMS and user_name in ROOMS[user_room]["users"]:
                            ROOMS[user_room]["users"][user_name]["ping"] = ping_val
                            await broadcast_room_state(user_room)

                    # Сообщение в чат
                    elif mtype == "CHAT":
                        if user_room and user_room in ROOMS:
                            chat_msg = json.dumps({
                                "type": "CHAT",
                                "room": user_room,
                                "sender": user_name,
                                "text": data.get("text", "")
                            })
                            for peer_name, pdata in list(ROOMS[user_room]["users"].items()):
                                try:
                                    await pdata["ws"].send(chat_msg)
                                except Exception:
                                    pass

                except Exception as ex:
                    print(f"[!] Ошибка разбора сообщения: {ex}")

            # 2. БИНАРНЫЕ АУДИО ПАКЕТЫ
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
        print(f"[-] Соединение закрыто для {user_name}: {e}")
    finally:
        if user_room and user_room in ROOMS:
            if user_name in ROOMS[user_room]["users"]:
                del ROOMS[user_room]["users"][user_name]
                print(f"[-] {user_name} удален из {user_room}")
            
            if not ROOMS[user_room]["users"]:
                del ROOMS[user_room]
                print(f"[x] Пустая комната {user_room} автоматически уничтожена!")
            else:
                await broadcast_room_state(user_room)

async def cleanup_dead_connections():
    """Фоновая автоочистка зависших клиентов и пустых комнат каждые 10 секунд"""
    while True:
        await asyncio.sleep(10)
        now = time.time()
        for r_id in list(ROOMS.keys()):
            for u_name in list(ROOMS[r_id]["users"].keys()):
                user_data = ROOMS[r_id]["users"][u_name]
                if now - user_data.get("last_seen", now) > 40:
                    print(f"[AutoKick] {u_name} не отвечает. Удаление из {r_id}")
                    try:
                        await user_data["ws"].close()
                    except Exception:
                        pass
                    del ROOMS[r_id]["users"][u_name]

            if not ROOMS[r_id]["users"]:
                del ROOMS[r_id]
                print(f"[AutoClean] Пустая комната {r_id} удалена!")
            else:
                await broadcast_room_state(r_id)

async def main():
    port = int(os.environ.get("PORT", 8765))
    asyncio.create_task(cleanup_dead_connections())
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        ping_interval=20,
        ping_timeout=20,
        max_size=10 * 1024 * 1024,
        max_queue=128
    ):
        print(f"[*] Wave Master Server V4 активен на порту {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
