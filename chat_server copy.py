#!/usr/bin/env python3
"""
Real-time Chat Application Server
A proof of concept WebSocket-based chat server with user authentication and message persistence.
"""

import asyncio
import websockets
import json
import sqlite3
import datetime
from typing import Dict, Set
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChatServer:
    def __init__(self):
        self.connected_users: Dict[websockets.WebSocketServerProtocol, str] = {}
        self.user_presence: Set[str] = set()
        self.init_database()

    def init_database(self):
        """Initialize SQLite database for message persistence"""
        self.conn = sqlite3.connect('chat.db', check_same_thread=False)
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                room TEXT DEFAULT 'general'
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
        logger.info("Database initialized successfully")

    async def register_user(self, websocket, username: str) -> bool:
        """Register a new user or authenticate existing user"""
        try:
            # Simple authentication - just check if username is valid
            if not username or len(username.strip()) < 2:
                await self.send_error(websocket, "Username must be at least 2 characters")
                return False

            username = username.strip()

            # Add user to database if not exists
            try:
                self.conn.execute("INSERT OR IGNORE INTO users (username) VALUES (?)", (username,))
                self.conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Database error: {e}")

            # Register user connection
            self.connected_users[websocket] = username
            self.user_presence.add(username)

            # Send authentication success
            await websocket.send(json.dumps({
                "type": "auth_success",
                "username": username,
                "message": f"Welcome {username}!"
            }))

            # Broadcast user joined
            await self.broadcast_user_event(username, "joined")

            # Send recent messages to new user
            await self.send_recent_messages(websocket)

            logger.info(f"User {username} connected")
            return True

        except Exception as e:
            logger.error(f"Error registering user: {e}")
            await self.send_error(websocket, "Authentication failed")
            return False

    async def send_error(self, websocket, message: str):
        """Send error message to client"""
        try:
            await websocket.send(json.dumps({
                "type": "error",
                "message": message
            }))
        except:
            pass

    async def send_recent_messages(self, websocket, limit: int = 10):
        """Send recent messages to newly connected user"""
        try:
            cursor = self.conn.execute(
                "SELECT username, message, timestamp FROM messages ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            )
            messages = cursor.fetchall()

            for username, message, timestamp in reversed(messages):
                await websocket.send(json.dumps({
                    "type": "message",
                    "username": username,
                    "message": message,
                    "timestamp": timestamp
                }))
        except Exception as e:
            logger.error(f"Error sending recent messages: {e}")

    async def handle_message(self, websocket, data: dict):
        """Handle incoming chat message"""
        try:
            username = self.connected_users.get(websocket)
            if not username:
                await self.send_error(websocket, "Not authenticated")
                return

            message = data.get("message", "").strip()
            if not message:
                return

            # Store message in database
            timestamp = datetime.datetime.now().isoformat()
            self.conn.execute(
                "INSERT INTO messages (username, message, timestamp) VALUES (?, ?, ?)",
                (username, message, timestamp)
            )
            self.conn.commit()

            # Broadcast message to all connected users
            message_data = {
                "type": "message",
                "username": username,
                "message": message,
                "timestamp": timestamp
            }

            await self.broadcast_to_all(json.dumps(message_data))
            logger.info(f"Message from {username}: {message}")

        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await self.send_error(websocket, "Failed to send message")

    async def handle_search(self, websocket, data: dict):
        """Handle message search requests - VULNERABLE TO SQL INJECTION"""
        try:
            username = self.connected_users.get(websocket)
            if not username:
                await self.send_error(websocket, "Not authenticated")
                return

            search_term = data.get("query", "").strip()
            if not search_term:
                await self.send_error(websocket, "Search query cannot be empty")
                return

            #TODO
            query = f"SELECT username, message, timestamp FROM messages WHERE message LIKE '%{search_term}%' ORDER BY timestamp DESC LIMIT 20"

            cursor = self.conn.execute(query)
            results = cursor.fetchall()

            search_results = []
            for username_result, message, timestamp in results:
                search_results.append({
                    "username": username_result,
                    "message": message,
                    "timestamp": timestamp
                })

            await websocket.send(json.dumps({
                "type": "search_results",
                "query": search_term,
                "results": search_results,
                "count": len(search_results)
            }))

            logger.info(f"Search performed by {username}: '{search_term}' - {len(search_results)} results")

        except Exception as e:
            logger.error(f"Error handling search: {e}")
            await self.send_error(websocket, "Search failed")

    async def broadcast_to_all(self, message: str):
        """Broadcast message to all connected users"""
        if not self.connected_users:
            return

        disconnected = []
        for websocket in self.connected_users:
            try:
                await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected.append(websocket)
            except Exception as e:
                logger.error(f"Error broadcasting to user: {e}")
                disconnected.append(websocket)

        # Clean up disconnected users
        for websocket in disconnected:
            await self.unregister_user(websocket)

    async def broadcast_user_event(self, username: str, event: str):
        """Broadcast user join/leave events"""
        event_data = {
            "type": "user_event",
            "username": username,
            "event": event,
            "online_users": list(self.user_presence)
        }
        await self.broadcast_to_all(json.dumps(event_data))

    async def unregister_user(self, websocket):
        """Unregister user when they disconnect"""
        username = self.connected_users.get(websocket)
        if username:
            self.user_presence.discard(username)
            del self.connected_users[websocket]
            await self.broadcast_user_event(username, "left")
            logger.info(f"User {username} disconnected")

    async def handle_client(self, websocket, path):
        """Handle new client connection"""
        logger.info(f"New connection from {websocket.remote_address}")

        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    message_type = data.get("type")

                    if message_type == "auth":
                        username = data.get("username")
                        await self.register_user(websocket, username)

                    elif message_type == "message":
                        await self.handle_message(websocket, data)

                    elif message_type == "ping":
                        await websocket.send(json.dumps({"type": "pong"}))

                    elif message_type == "search":
                        await self.handle_search(websocket, data)

                    else:
                        await self.send_error(websocket, f"Unknown message type: {message_type}")

                except json.JSONDecodeError:
                    await self.send_error(websocket, "Invalid JSON format")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await self.send_error(websocket, "Server error")

        except websockets.exceptions.ConnectionClosed:
            logger.info("Client disconnected")
        except Exception as e:
            logger.error(f"Error in client handler: {e}")
        finally:
            await self.unregister_user(websocket)

async def main():
    """Start the chat server"""
    chat_server = ChatServer()

    # Start WebSocket server
    server = await websockets.serve(
        chat_server.handle_client,
        "localhost",
        8765,
        ping_interval=20,
        ping_timeout=10
    )

    logger.info("Chat server started on ws://localhost:8765")
    logger.info("Server ready to accept connections...")

    # Keep server running
    await server.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    except Exception as e:
        logger.error(f"Server error: {e}")
