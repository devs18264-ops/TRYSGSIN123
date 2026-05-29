from json import loads, dumps
from time import sleep, time
from websocket import WebSocket
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests
import random
import os
from datetime import datetime
import aiohttp
import asyncio
from typing import Dict, List, Optional
import re

# === CONFIGURATION ===
CONFIG_FILE = "config.json"
TOKENS_FILE = "tokens.txt"
PACKS_FILE = "packs.txt"
INVALID_TOKENS_FILE = "invalid_tokens.txt"

# === DEFAULT CONFIG ===
default_config = {
    "guild_id": "",
    "vc_channels": {},  # token -> channel_id
    "auto_change_interval": 360,  # 6 hours in minutes
    "check_interval": 60,  # 1 hour in minutes
    "status": "none",
    "online_status": "online",  # online, idle, dnd, invisible
    "active_packs": []
}

class DiscordAccountManager:
    def __init__(self):
        self.tokens = []
        self.active_connections = {}
        self.token_packs = {}  # token -> current pack
        self.token_channels = {}  # token -> channel_id
        self.running = True
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=500)
        self.session = requests.Session()
        self.heartbeat_threads = {}  # Track heartbeat threads
        self.reconnect_attempts = {}  # Track reconnect attempts
        self.load_data()
        
    def load_data(self):
        """Load tokens and configuration"""
        # Load and validate tokens
        if os.path.exists(TOKENS_FILE):
            with open(TOKENS_FILE, 'r') as f:
                raw_tokens = [line.strip() for line in f if line.strip()]
            self.tokens = self.validate_tokens(raw_tokens)
        else:
            print(f"❌ {TOKENS_FILE} not found!")
            exit(1)
            
        # Load packs
        self.packs = self.load_packs()
        
        # Load config
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                config = loads(f.read())
                self.guild_id = config.get("guild_id", "")
                self.token_channels = config.get("vc_channels", {})
                self.auto_change_interval = config.get("auto_change_interval", 360)
                self.check_interval = config.get("check_interval", 60)
                self.status_config = config.get("status", "none")
                self.online_status = config.get("online_status", "online")
                self.active_packs = config.get("active_packs", [])
        else:
            self.guild_id = ""
            self.token_channels = {}
            self.auto_change_interval = 360
            self.check_interval = 60
            self.status_config = "none"
            self.online_status = "online"
            self.active_packs = []
            
    def validate_tokens(self, tokens):
        """Validate tokens and remove invalid ones"""
        valid_tokens = []
        invalid_tokens = []
        
        print("\n🔍 Validating tokens...")
        for token in tokens:
            headers = {"Authorization": token}
            try:
                response = requests.get("https://discord.com/api/v9/users/@me", headers=headers, timeout=10)
                if response.status_code == 200:
                    user_data = response.json()
                    print(f"✅ Valid token: {user_data.get('username', 'Unknown')}#{user_data.get('discriminator', '0000')}")
                    valid_tokens.append(token)
                else:
                    print(f"❌ Invalid token: {token[:20]}... (Status: {response.status_code})")
                    invalid_tokens.append(token)
            except Exception as e:
                print(f"❌ Invalid token: {token[:20]}... (Error: {str(e)})")
                invalid_tokens.append(token)
                
        # Save invalid tokens
        if invalid_tokens:
            with open(INVALID_TOKENS_FILE, 'a') as f:
                for token in invalid_tokens:
                    f.write(f"{token} - Removed on {datetime.now()}\n")
            
            # Update tokens.txt with only valid tokens
            with open(TOKENS_FILE, 'w') as f:
                for token in valid_tokens:
                    f.write(f"{token}\n")
                    
        print(f"\n📊 Token validation complete: {len(valid_tokens)} valid, {len(invalid_tokens)} invalid")
        return valid_tokens
        
    def load_packs(self):
        """Load avatar+bio+name packs from file"""
        packs = []
        if os.path.exists(PACKS_FILE):
            with open(PACKS_FILE, 'r') as f:
                content = f.read()
                # Parse packs separated by "---"
                pack_data = content.split("---")
                for data in pack_data:
                    if data.strip():
                        pack = {}
                        lines = data.strip().split('\n')
                        for line in lines:
                            if ':' in line:
                                key, value = line.split(':', 1)
                                pack[key.strip()] = value.strip()
                        if all(k in pack for k in ['name', 'bio', 'avatar_url']):
                            packs.append(pack)
        return packs
    
    def save_config(self):
        """Save current configuration"""
        config = {
            "guild_id": self.guild_id,
            "vc_channels": self.token_channels,
            "auto_change_interval": self.auto_change_interval,
            "check_interval": self.check_interval,
            "status": self.status_config,
            "online_status": self.online_status,
            "active_packs": self.active_packs
        }
        with open(CONFIG_FILE, 'w') as f:
            f.write(dumps(config, indent=2))
            
    def update_presence(self, ws, token):
        """Update user presence (online/idle/dnd)"""
        status_map = {
            "online": "online",
            "idle": "idle",
            "dnd": "dnd",
            "invisible": "invisible"
        }
        
        status = status_map.get(self.online_status, "online")
        
        presence_payload = {
            "op": 3,
            "d": {
                "since": None,
                "activities": [],
                "status": status,
                "afk": False
            }
        }
        
        try:
            ws.send(dumps(presence_payload))
            return True
        except:
            return False
            
    def keep_alive_mechanism(self, token, ws, heartbeat_interval):
        """Enhanced keep-alive mechanism to prevent disconnection"""
        last_heartbeat = time()
        last_presence_update = time()
        missed_heartbeats = 0
        max_missed = 3
        
        while self.running and token in self.active_connections:
            current_time = time()
            
            # Send heartbeat
            if current_time - last_heartbeat >= heartbeat_interval / 1000:
                try:
                    ws.send(dumps({"op": 1, "d": None}))
                    last_heartbeat = current_time
                    missed_heartbeats = 0
                except Exception as e:
                    missed_heartbeats += 1
                    print(f"⚠️ Heartbeat failed for {token[:20]}... ({missed_heartbeats}/{max_missed})")
                    if missed_heartbeats >= max_missed:
                        print(f"❌ Connection dead for {token[:20]}..., reconnecting...")
                        break
                        
            # Update presence every 30 seconds to stay active
            if current_time - last_presence_update >= 30:
                self.update_presence(ws, token)
                last_presence_update = current_time
                
            # Send voice keep-alive every 45 seconds
            try:
                ws.send(dumps({
                    "op": 18,
                    "d": {
                        "type": "guild",
                        "guild_id": self.guild_id,
                        "channel_id": self.token_channels.get(token),
                        "preferred_region": "singapore"
                    }
                }))
            except:
                pass
                
            sleep(5)  # Check every 5 seconds
            
    def update_discord_profile(self, token: str, name: str = None, bio: str = None, avatar_url: str = None):
        """Update Discord profile (username, bio, avatar)"""
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        
        # Update username
        if name:
            response = requests.patch(
                "https://discord.com/api/v9/users/@me",
                headers=headers,
                json={"username": name}
            )
            if response.status_code == 200:
                print(f"✅ Updated username for {token[:20]}... to {name}")
            else:
                print(f"❌ Failed to update username for {token[:20]}...: {response.text}")
                
        # Update bio
        if bio:
            response = requests.patch(
                "https://discord.com/api/v9/users/@me/profile",
                headers=headers,
                json={"bio": bio}
            )
            if response.status_code == 200:
                print(f"✅ Updated bio for {token[:20]}...")
            else:
                print(f"❌ Failed to update bio for {token[:20]}...")
                
        # Update avatar
        if avatar_url:
            # Download avatar
            avatar_response = requests.get(avatar_url)
            if avatar_response.status_code == 200:
                import base64
                avatar_base64 = base64.b64encode(avatar_response.content).decode('utf-8')
                avatar_data = f"data:image/{avatar_url.split('.')[-1]};base64,{avatar_base64}"
                
                response = requests.patch(
                    "https://discord.com/api/v9/users/@me",
                    headers=headers,
                    json={"avatar": avatar_data}
                )
                if response.status_code == 200:
                    print(f"✅ Updated avatar for {token[:20]}...")
                else:
                    print(f"❌ Failed to update avatar for {token[:20]}...")
                    
    def assign_random_pack(self, token: str):
        """Assign a random pack to a token"""
        if self.packs and self.active_packs:
            pack = random.choice(self.active_packs)
            self.token_packs[token] = pack
            self.update_discord_profile(
                token,
                name=pack.get('name'),
                bio=pack.get('bio'),
                avatar_url=pack.get('avatar_url')
            )
            return pack
        return None
        
    def rotate_all_packs(self):
        """Rotate packs for all tokens"""
        print(f"\n🔄 Rotating packs for all tokens at {datetime.now()}")
        for token in self.tokens:
            if token in self.active_connections:
                self.assign_random_pack(token)
                sleep(0.5)  # Rate limiting
                
    def check_vc_presence(self):
        """Check which accounts are in VC and reconnect if needed"""
        print(f"\n🔍 Checking VC presence at {datetime.now()}")
        for token in self.tokens:
            if token in self.active_connections:
                connection_data = self.active_connections.get(token)
                if connection_data and connection_data.get('ws'):
                    # Check if connection is still alive
                    try:
                        ws = connection_data['ws']
                        # Send a ping to check connection
                        ws.settimeout(1)
                        try:
                            ws.send(dumps({"op": 1, "d": None}))
                            print(f"✅ {token[:20]}... is in VC")
                        except:
                            print(f"⚠️ {token[:20]}... connection stale, reconnecting...")
                            self.reconnect_account(token)
                        ws.settimeout(None)
                    except:
                        print(f"⚠️ {token[:20]}... connection issue, reconnecting...")
                        self.reconnect_account(token)
                else:
                    print(f"⚠️ {token[:20]}... no WebSocket, reconnecting...")
                    self.reconnect_account(token)
            else:
                print(f"❌ {token[:20]}... not in VC, connecting...")
                self.reconnect_account(token)
                
    def reconnect_account(self, token: str):
        """Reconnect a specific account to VC with exponential backoff"""
        # Track reconnect attempts to prevent spam
        if token not in self.reconnect_attempts:
            self.reconnect_attempts[token] = 0
            
        # Exponential backoff
        delay = min(30, 2 ** self.reconnect_attempts[token])
        
        channel_id = self.token_channels.get(token)
        if channel_id:
            print(f"🔄 Reconnecting {token[:20]}... in {delay}s (attempt {self.reconnect_attempts[token] + 1})")
            sleep(delay)
            self.reconnect_attempts[token] += 1
            self.executor.submit(
                self.connect_and_join, 
                token, 
                channel_id
            )
            
    def connect_and_join(self, token: str, channel_id: str):
        """Connect a single account to voice channel with NEVER disconnecting"""
        mute = deaf = False
        if self.status_config == "mute":
            mute = True
        elif self.status_config == "deaf":
            deaf = True
        elif self.status_config == "mute+deaf":
            mute = deaf = True
            
        # Reset reconnect attempts on new connection
        self.reconnect_attempts[token] = 0
        
        while self.running:
            try:
                ws = WebSocket()
                ws.connect("wss://gateway.discord.gg/?v=9&encoding=json")
                hello = loads(ws.recv())
                heartbeat_interval = hello['d']['heartbeat_interval']
                
                # Identify with presence
                identify_payload = {
                    "op": 2,
                    "d": {
                        "token": token,
                        "properties": {
                            "$os": "windows",
                            "$browser": "Discord",
                            "$device": "desktop"
                        },
                        "presence": {
                            "status": self.online_status,
                            "since": 0,
                            "activities": [],
                            "afk": False
                        }
                    }
                }
                ws.send(dumps(identify_payload))
                
                sleep(1)  # Wait for identify to process
                
                # Join VC
                ws.send(dumps({
                    "op": 4,
                    "d": {
                        "guild_id": self.guild_id,
                        "channel_id": channel_id,
                        "self_mute": mute,
                        "self_deaf": deaf
                    }
                }))
                
                # Set preferred region and keep connection alive
                ws.send(dumps({
                    "op": 18,
                    "d": {
                        "type": "guild",
                        "guild_id": self.guild_id,
                        "channel_id": channel_id,
                        "preferred_region": "singapore"
                    }
                }))
                
                with self.lock:
                    self.active_connections[token] = {
                        'ws': ws,
                        'channel': channel_id,
                        'connected_at': time()
                    }
                    
                print(f"✅ Connected: {token[:20]}... to channel {channel_id} [Status: {self.online_status}]")
                
                # Apply pack if not assigned
                if token not in self.token_packs:
                    self.assign_random_pack(token)
                
                # Start keep-alive mechanism
                self.keep_alive_mechanism(token, ws, heartbeat_interval)
                        
            except Exception as e:
                print(f"❌ Connection failed for {token[:20]}...: {e}")
                # Don't exit, keep trying to reconnect
                sleep(5)
                continue
                
            # Cleanup and reconnect if loop breaks
            with self.lock:
                if token in self.active_connections:
                    del self.active_connections[token]
            try:
                ws.close()
            except:
                pass
            
            # Wait before reconnecting
            if self.running:
                delay = min(30, 2 ** self.reconnect_attempts.get(token, 0))
                print(f"🔄 Reconnecting {token[:20]}... in {delay}s")
                sleep(delay)
                self.reconnect_attempts[token] = self.reconnect_attempts.get(token, 0) + 1
            
    def leave_vc(self, token: str = None):
        """Make account(s) leave VC"""
        with self.lock:
            if token:
                tokens_to_leave = [token]
            else:
                tokens_to_leave = list(self.active_connections.keys())
                
            for t in tokens_to_leave:
                if t in self.active_connections:
                    try:
                        ws = self.active_connections[t]['ws']
                        ws.send(dumps({
                            "op": 4,
                            "d": {
                                "guild_id": self.guild_id,
                                "channel_id": None,
                                "self_mute": False,
                                "self_deaf": False
                            }
                        }))
                        ws.close()
                        del self.active_connections[t]
                        print(f"✅ {t[:20]}... left VC")
                    except Exception as e:
                        print(f"❌ Error leaving VC for {t[:20]}...: {e}")
                        
    def start_all(self):
        """Start all connections"""
        print(f"\n🚀 Starting connections for {len(self.tokens)} tokens...")
        for token in self.tokens:
            channel_id = self.token_channels.get(token)
            if channel_id:
                self.executor.submit(self.connect_and_join, token, channel_id)
                sleep(0.5)  # Delay between connections
            else:
                print(f"⚠️ No channel assigned for {token[:20]}...")
                
    def interactive_menu(self):
        """Interactive menu for managing the script"""
        while self.running:
            print("\n" + "="*60)
            print("🎮 DISCORD ACCOUNT MANAGER v2.0")
            print("="*60)
            print("1. 📊 View Status")
            print("2. 🔄 Change Account Profile (Single)")
            print("3. 🎲 Rotate All Packs")
            print("4. 📝 Change Account Bio (Single)")
            print("5. 🖼️ Change Account Avatar (Single)")
            print("6. 👤 Change Account Name (Single)")
            print("7. 📍 Change VC Channel (Single)")
            print("8. 🚪 Make Account Leave VC")
            print("9. 🔄 Reconnect Account")
            print("10. 📦 Manage Packs")
            print("11. ⚙️ Change Settings")
            print("12. 🔍 Manual VC Check")
            print("13. 💾 Save Configuration")
            print("14. 🎭 Change Online Status (Online/Idle/DND)")
            print("15. 🛑 Exit Script")
            print("="*60)
            
            choice = input("Select option: ").strip()
            
            if choice == "1":
                with self.lock:
                    print(f"\n📊 Status: {len(self.active_connections)}/{len(self.tokens)} accounts in VC")
                    print(f"🎭 Default status: {self.online_status}")
                    for token, data in list(self.active_connections.items())[:5]:
                        pack = self.token_packs.get(token, {})
                        uptime = int(time() - data['connected_at']) // 60
                        print(f"  • {token[:20]}... - Channel: {data['channel']} - Pack: {pack.get('name', 'None')} - Uptime: {uptime}m")
                    if len(self.active_connections) > 5:
                        print(f"  ... and {len(self.active_connections)-5} more")
                        
            elif choice == "2":
                self.change_single_profile()
                
            elif choice == "3":
                self.rotate_all_packs()
                
            elif choice == "4":
                self.change_single_bio()
                
            elif choice == "5":
                self.change_single_avatar()
                
            elif choice == "6":
                self.change_single_name()
                
            elif choice == "7":
                self.change_single_channel()
                
            elif choice == "8":
                token_id = input("Enter token (or part of token) to leave VC: ").strip()
                matching_tokens = [t for t in self.tokens if token_id in t]
                if matching_tokens:
                    self.leave_vc(matching_tokens[0])
                else:
                    print("❌ Token not found")
                    
            elif choice == "9":
                token_id = input("Enter token to reconnect: ").strip()
                matching_tokens = [t for t in self.tokens if token_id in t]
                if matching_tokens:
                    self.reconnect_account(matching_tokens[0])
                else:
                    print("❌ Token not found")
                    
            elif choice == "10":
                self.manage_packs()
                
            elif choice == "11":
                self.change_settings()
                
            elif choice == "12":
                self.check_vc_presence()
                
            elif choice == "13":
                self.save_config()
                print("✅ Configuration saved")
                
            elif choice == "14":
                self.change_online_status()
                
            elif choice == "15":
                print("🛑 Shutting down...")
                self.running = False
                self.leave_vc()
                self.executor.shutdown(wait=False)
                break
                
    def change_online_status(self):
        """Change online status for all accounts"""
        print("\n🎭 Available statuses:")
        print("1. Online (🟢)")
        print("2. Idle (🟠)")
        print("3. Do Not Disturb (🔴)")
        print("4. Invisible (⚫)")
        
        choice = input("Select status: ").strip()
        status_map = {
            "1": "online",
            "2": "idle",
            "3": "dnd",
            "4": "invisible"
        }
        
        if choice in status_map:
            self.online_status = status_map[choice]
            print(f"✅ Online status changed to: {self.online_status}")
            
            # Update presence for all connected accounts
            with self.lock:
                for token, data in self.active_connections.items():
                    ws = data.get('ws')
                    if ws:
                        self.update_presence(ws, token)
            print("✅ Presence updated for all connected accounts")
        else:
            print("❌ Invalid choice")
                
    def change_single_profile(self):
        """Change profile (name, bio, avatar) for a single account"""
        token_id = input("Enter token (or part of token): ").strip()
        matching_tokens = [t for t in self.tokens if token_id in t]
        if not matching_tokens:
            print("❌ Token not found")
            return
            
        token = matching_tokens[0]
        print("\nSelect pack or custom:")
        print("1. Use existing pack")
        print("2. Custom profile")
        
        choice = input("Choice: ").strip()
        
        if choice == "1" and self.packs:
            for i, pack in enumerate(self.packs):
                print(f"{i+1}. {pack.get('name', 'Unknown')}")
            pack_choice = int(input("Select pack: ")) - 1
            if 0 <= pack_choice < len(self.packs):
                pack = self.packs[pack_choice]
                self.token_packs[token] = pack
                self.update_discord_profile(
                    token,
                    name=pack.get('name'),
                    bio=pack.get('bio'),
                    avatar_url=pack.get('avatar_url')
                )
        else:
            name = input("New name (leave empty to skip): ").strip()
            bio = input("New bio (leave empty to skip): ").strip()
            avatar_url = input("Avatar URL (leave empty to skip): ").strip()
            self.update_discord_profile(token, name or None, bio or None, avatar_url or None)
            
    def change_single_bio(self):
        """Change bio for a single account"""
        token_id = input("Enter token (or part of token): ").strip()
        matching_tokens = [t for t in self.tokens if token_id in t]
        if matching_tokens:
            new_bio = input("Enter new bio: ").strip()
            self.update_discord_profile(matching_tokens[0], bio=new_bio)
            
    def change_single_avatar(self):
        """Change avatar for a single account"""
        token_id = input("Enter token (or part of token): ").strip()
        matching_tokens = [t for t in self.tokens if token_id in t]
        if matching_tokens:
            avatar_url = input("Enter avatar URL: ").strip()
            self.update_discord_profile(matching_tokens[0], avatar_url=avatar_url)
            
    def change_single_name(self):
        """Change name for a single account"""
        token_id = input("Enter token (or part of token): ").strip()
        matching_tokens = [t for t in self.tokens if token_id in t]
        if matching_tokens:
            new_name = input("Enter new name: ").strip()
            self.update_discord_profile(matching_tokens[0], name=new_name)
            
    def change_single_channel(self):
        """Change VC channel for a single account"""
        token_id = input("Enter token (or part of token): ").strip()
        matching_tokens = [t for t in self.tokens if token_id in t]
        if matching_tokens:
            token = matching_tokens[0]
            new_channel = input("Enter new channel ID: ").strip()
            self.token_channels[token] = new_channel
            print(f"✅ Channel updated for {token[:20]}... Reconnecting...")
            self.leave_vc(token)
            sleep(2)
            self.reconnect_account(token)
            
    def manage_packs(self):
        """Manage profile packs"""
        print("\n📦 PACK MANAGEMENT")
        print(f"Loaded packs: {len(self.packs)}")
        print(f"Active packs: {len(self.active_packs)}")
        print("\n1. View all packs")
        print("2. Toggle pack active/inactive")
        print("3. Add new pack")
        print("4. Reload packs from file")
        
        choice = input("Choice: ").strip()
        
        if choice == "1":
            for i, pack in enumerate(self.packs):
                active = "✅" if pack in self.active_packs else "❌"
                print(f"{active} {i+1}. Name: {pack.get('name', 'Unknown')}")
                print(f"   Bio: {pack.get('bio', '')[:50]}...")
                print(f"   Avatar: {pack.get('avatar_url', '')[:50]}...")
                print()
                
        elif choice == "2":
            for i, pack in enumerate(self.packs):
                print(f"{i+1}. {pack.get('name', 'Unknown')}")
            pack_choice = int(input("Select pack: ")) - 1
            if 0 <= pack_choice < len(self.packs):
                pack = self.packs[pack_choice]
                if pack in self.active_packs:
                    self.active_packs.remove(pack)
                    print(f"❌ Pack {pack.get('name')} deactivated")
                else:
                    self.active_packs.append(pack)
                    print(f"✅ Pack {pack.get('name')} activated")
                    
        elif choice == "3":
            name = input("Pack name: ").strip()
            bio = input("Bio: ").strip()
            avatar_url = input("Avatar URL: ").strip()
            new_pack = {"name": name, "bio": bio, "avatar_url": avatar_url}
            self.packs.append(new_pack)
            self.active_packs.append(new_pack)
            with open(PACKS_FILE, 'a') as f:
                f.write(f"\n---\nname:{name}\nbio:{bio}\navatar_url:{avatar_url}\n")
            print("✅ Pack added")
            
        elif choice == "4":
            self.packs = self.load_packs()
            print(f"✅ Reloaded {len(self.packs)} packs")
            
    def change_settings(self):
        """Change script settings"""
        print("\n⚙️ CURRENT SETTINGS")
        print(f"Guild ID: {self.guild_id}")
        print(f"Auto-change interval: {self.auto_change_interval} minutes")
        print(f"VC check interval: {self.check_interval} minutes")
        print(f"Voice Status: {self.status_config}")
        print(f"Online Status: {self.online_status}")
        
        print("\n1. Change guild ID")
        print("2. Change auto-change interval")
        print("3. Change check interval")
        print("4. Change voice status (mute/deaf)")
        
        choice = input("Choice: ").strip()
        
        if choice == "1":
            new_guild = input("Enter guild ID: ").strip()
            self.guild_id = new_guild
            print("✅ Guild ID updated")
            
        elif choice == "2":
            new_interval = int(input("Enter interval in minutes: ").strip())
            self.auto_change_interval = new_interval
            print(f"✅ Auto-change interval set to {new_interval} minutes")
            
        elif choice == "3":
            new_interval = int(input("Enter check interval in minutes: ").strip())
            self.check_interval = new_interval
            print(f"✅ Check interval set to {new_interval} minutes")
            
        elif choice == "4":
            print("Options: mute / deaf / mute+deaf / none")
            new_status = input("Enter voice status: ").strip().lower()
            if new_status in ["mute", "deaf", "mute+deaf", "none"]:
                self.status_config = new_status
                print("✅ Voice status updated (will apply on reconnect)")
                
    def run_automation(self):
        """Run automated tasks in background"""
        last_rotate = time()
        last_check = time()
        
        while self.running:
            current_time = time()
            
            # Rotate packs every X minutes
            if current_time - last_rotate >= self.auto_change_interval * 60:
                self.rotate_all_packs()
                last_rotate = current_time
                
            # Check VC presence every X minutes
            if current_time - last_check >= self.check_interval * 60:
                self.check_vc_presence()
                last_check = current_time
                
            sleep(30)  # Check every 30 seconds
                
    def setup_initial(self):
        """Initial setup - ask for guild and channel assignments"""
        if not self.guild_id:
            self.guild_id = input("Enter Guild ID: ").strip()
            
        print(f"\n📢 Setting up VC channels for {len(self.tokens)} tokens")
        print("You can assign channels individually or all to same channel")
        
        assign_choice = input("1. Assign all to same channel\n2. Assign individually\nChoice: ").strip()
        
        if assign_choice == "1":
            channel_id = input("Enter channel ID for all accounts: ").strip()
            for token in self.tokens:
                self.token_channels[token] = channel_id
        else:
            for token in self.tokens:
                channel_id = input(f"Channel ID for {token[:20]}...: ").strip()
                self.token_channels[token] = channel_id
                
        # Set online status
        print("\n🎭 Set default online status:")
        print("1. Online 🟢")
        print("2. Idle 🟠")
        print("3. Do Not Disturb 🔴")
        status_choice = input("Choice (default: 1): ").strip()
        if status_choice == "2":
            self.online_status = "idle"
        elif status_choice == "3":
            self.online_status = "dnd"
        else:
            self.online_status = "online"
            
        self.save_config()
        print("✅ Setup complete!")

# === MAIN EXECUTION ===
if __name__ == "__main__":
    print("🎮 Discord Account Manager v2.0")
    print("="*50)
    print("Features:")
    print("✅ Automatic token validation")
    print("✅ Never disconnects from VC")
    print("✅ Custom online status (Online/Idle/DND)")
    print("✅ Auto-reconnect with exponential backoff")
    print("✅ Profile pack system")
    print("="*50)
    
    manager = DiscordAccountManager()
    
    if not manager.guild_id or not manager.token_channels:
        manager.setup_initial()
        
    # Start background automation
    automation_thread = threading.Thread(target=manager.run_automation, daemon=True)
    automation_thread.start()
    
    # Start all connections
    manager.start_all()
    
    # Start interactive menu
    try:
        manager.interactive_menu()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
        manager.running = False
        manager.leave_vc()