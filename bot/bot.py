import os
import io
import asyncio
import re
import json
import sqlite3
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Optional
import psutil

import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks
from discord.ext import commands
from dotenv import load_dotenv
import requests
from datetime import datetime
from enum import Enum
import sqlite3
import random
import time

from discord.ui import View, button
from discord import ButtonStyle

from fastapi import FastAPI
import discord
from discord.ext import commands

from discord import Interaction
from discord import app_commands

# FastAPI 앱 생성
app = FastAPI()

intents = discord.Intents.default()
intents.members = True 
bot = commands.Bot(command_prefix="!", intents=intents) 


# =========================
# 데이터베이스
# =========================

conn = sqlite3.connect("economy.db")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS economy(
    user_id INTEGER PRIMARY KEY,
    money INTEGER DEFAULT 0,
    last_daily INTEGER DEFAULT 0,
    exp INTEGER DEFAULT 0,
    level INTEGER DEFAULT 1
)
""")

conn.commit()


def get_user(user_id):

    cur.execute("SELECT * FROM economy WHERE user_id=?", (user_id,))
    data = cur.fetchone()

    if data is None:
        cur.execute(
            "INSERT INTO economy (user_id,money,last_daily,exp,level) VALUES (?,0,0,0,1)",
            (user_id,)
        )
        conn.commit()
        return (user_id,0,0,0,1)

    return data

VERIFY_ROLE_ID = 1461636782176075831      # 🟢 인증자 역할 ID
UNVERIFY_ROLE_ID = 1478713261074550956     # 🔴 제거할 역할 ID (예: 미인증자)
ADMIN_LOG_CHANNEL_ID = 1468191799855026208 # 📋 관리자 로그 채널 ID 

# emoji = {"<:X_red:1479810084900044851>",
#          "<:_red:1479810110632099972>",
#          "<:Log_blue:1479810216597127224>",
#          "<:Chack_blue:1479810189434683402>",
#          "<:announce_blue:1479810147911205006>",
#          "<:verfired_green:1479810239619530752>"}

API_BASE = "https://web-api-production-69fc.up.railway.app" 

def is_already_verified(guild_id: int, user_id: int) -> bool:
    try:
        resp = requests.get(
            f"{API_BASE}/api/logs/verify",
            params={
                "guild_id": guild_id,
                "user_id": user_id,
                "limit": 1,
            },
            timeout=5,
        )
        if resp.status_code != 200:
            print("[WEB_CHECK_ERROR]", resp.status_code, resp.text)
            return False 

        data = resp.json()
        # 한 건이라도 있으면 이미 인증한 걸로 간주
        return len(data) > 0
    except Exception as e:
        print("[WEB_CHECK_EXCEPTION]", repr(e))
        return False


LOG_API_URL = "https://web-api-production-69fc.up.railway.app"  # 나중에 Railway 올리면 URL만 바꾸면 됨 


COMMANDS_DISABLED = False
DISABLED_COMMANDS = ["일괄닉네임변경", "장교역할"] 

DISABLED_COMMANDS = ["일괄닉네임변경", "장교역할"] 

DEVELOPER_ID = 1276176866440642561 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # ← 이 줄은 그대로 두고,
PROJECT_ROOT = os.path.dirname(BASE_DIR) 

env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path) 

OFFICER_ROLE_ID = 1477313558474920057
TARGET_ROLE_ID = 1461636782176075831 

TOKEN = str(os.getenv("DISCORD_TOKEN"))
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
OWNER_ID = int(os.getenv("OWNER_ID", "0")) 

RANK_API_URL_ROOT = "https://surprising-perfection-production-e015.up.railway.app"
print("DEBUG ROOT:", repr(RANK_API_URL_ROOT))
RANK_API_KEY = os.getenv("RANK_API_KEY") 

CREATOR_ROBLOX_NICK = "Sky_Lunarx"
CREATOR_ROBLOX_REAL = "Sky_Lunarx"
CREATOR_DISCORD_NAME = "Lunar" 

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN이 .env에 설정되어 있지 않습니다.") 

intents = discord.Intents.all() 

error_logs: list[dict] = []
MAX_LOGS = 50 

DB_PATH = os.path.join(BASE_DIR, "bot.db")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor() 

# ---------- DB 스키마 ----------
cursor.execute("""
CREATE TABLE IF NOT EXISTS transfer_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER,
    from_id INTEGER,
    to_id INTEGER,
    amount INTEGER,
    created_at TEXT
)
""")
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS mod_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER,
    user_id INTEGER,
    action TEXT,        -- 'warn', 'ban', 'kick', 'timeout', 'mute' 등
    moderator_id INTEGER,
    reason TEXT,
    created_at TEXT     -- ISO 또는 datetime('now')
)
""")
conn.commit()

# 유저별 경고 횟수
cursor.execute("""
CREATE TABLE IF NOT EXISTS warnings (
    guild_id INTEGER,
    user_id INTEGER,
    warns INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, user_id)
)
""")

# 경고 횟수별 자동 처벌 규칙
cursor.execute("""
CREATE TABLE IF NOT EXISTS punish_rules (
    guild_id INTEGER,
    warn_count INTEGER,
    punish_type TEXT,      -- 'ban', 'timeout', 'mute', 'kick'
    duration INTEGER,      -- 초 (timeout/mute일 때만 사용, 나머지는 NULL 가능)
    PRIMARY KEY (guild_id, warn_count)
)
""")
conn.commit()

cursor.execute(
    """CREATE TABLE IF NOT EXISTS rank_log_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER,
        log_data TEXT,
        created_at TEXT
    )"""
)
conn.commit() 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS senior_officer_settings(
        guild_id INTEGER PRIMARY KEY,
        senior_officer_role_id INTEGER
    )"""
)
conn.commit() 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS blacklist(
        guild_id INTEGER,
        group_id INTEGER,
        PRIMARY KEY(guild_id, group_id)
    )"""
)
conn.commit() 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS rank_log_settings(
        guild_id INTEGER PRIMARY KEY,
        channel_id INTEGER,
        enabled INTEGER DEFAULT 0
    )"""
)
conn.commit() 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS forced_verified(
        discord_id INTEGER,
        guild_id INTEGER,
        roblox_nick TEXT,
        roblox_user_id INTEGER,
        rank_role TEXT,
        PRIMARY KEY(discord_id, guild_id)
    )"""
)
conn.commit() 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS users(
        discord_id INTEGER,
        guild_id INTEGER,
        roblox_nick TEXT,
        roblox_user_id INTEGER,
        code TEXT,
        expire_time TEXT,
        verified INTEGER DEFAULT 0,
        PRIMARY KEY(discord_id, guild_id)
    )"""
) 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS stats(
        guild_id INTEGER PRIMARY KEY,
        verify_count INTEGER DEFAULT 0,
        force_count INTEGER DEFAULT 0,
        cancel_count INTEGER DEFAULT 0
    )"""
) 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS settings(
        guild_id INTEGER PRIMARY KEY,
        role_id INTEGER,
        status_channel_id INTEGER,
        admin_role_id TEXT
    )"""
) 

cursor.execute("""
CREATE TABLE IF NOT EXISTS logchannels (
    guildid   INTEGER,
    logtype   TEXT,
    channelid INTEGER,
    PRIMARY KEY (guildid, logtype)
)
""")
conn.commit() 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS officer_settings(
        guild_id INTEGER PRIMARY KEY,
        officer_role_id INTEGER
    )"""
)
conn.commit() 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS group_settings(
        guild_id INTEGER PRIMARY KEY,
        group_id INTEGER
    )"""
) 

cursor.execute(
    """CREATE TABLE IF NOT EXISTS rollback_settings(
        guild_id INTEGER PRIMARY KEY,
        auto_rollback INTEGER DEFAULT 1
    )"""
)
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS shop_items(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER,
    name TEXT,
    price INTEGER,
    type TEXT,          -- 'role', 'level', 'exp'
    role_id INTEGER,    -- type='role' 일 때만 사용
    level INTEGER,      -- type='level' 일 때만 사용
    exp INTEGER         -- type='exp' 일 때만 사용
)
""")
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS command_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER,
    user_id INTEGER,
    user_name TEXT,
    command_name TEXT,
    command_full TEXT,
    created_at TEXT
)
""")
conn.commit()

conn.commit() 

# ---------- 설정/권한 유틸 ---------- 

class CommandLogView(View):
    def __init__(self, pages: list[str]):
        super().__init__(timeout=60)
        self.pages = pages
        self.index = 0

    async def update(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="📜 명령어 로그",
            description=self.pages[self.index],
            color=discord.Color.blurple(),
        )
        embed.set_footer(text=f"페이지 {self.index+1}/{len(self.pages)}")
        await interaction.response.edit_message(embed=embed, view=self)

    @button(label="⬅ 이전", style=ButtonStyle.gray)
    async def prev(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.index > 0:
            self.index -= 1
        await self.update(interaction)

    @button(label="다음 ➡", style=ButtonStyle.gray)
    async def next(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.index < len(self.pages) - 1:
            self.index += 1
        await self.update(interaction)

def get_senior_officer_role_id(guild_id: int) -> Optional[int]:
    cursor.execute("SELECT senior_officer_role_id FROM senior_officer_settings WHERE guild_id=?", (guild_id,))
    row = cursor.fetchone()
    return row[0] if row else None 

def set_senior_officer_role_id(guild_id: int, role_id: int) -> None:
    cursor.execute(
        """INSERT OR REPLACE INTO senior_officer_settings(guild_id, senior_officer_role_id)
           VALUES(?, ?)""",
        (guild_id, role_id),
    )
    conn.commit() 

def check_is_officer(rank_num: int, rank_name: str) -> tuple[bool, bool]:
    """위관급, 영관급 여부 체크 - (is_junior_officer, is_senior_officer)"""
    # 위관급: 소위(20) ~ 중령(80)
    is_junior = 70 <= rank_num <= 120
    junior_keywords = ["Second Lieutenant", "First Lieutenant", "Captain", "Major", "Lieutenant Colonel", "소위", "중위", "대위", "소령", "중령"]
    if any(kw.lower() in rank_name.lower() for kw in junior_keywords):
        is_junior = True
    
    # 영관급 이상: 대령(100) ~ 대장(200) + 장성급 포함
    is_senior = 130 <= rank_num <= 170
    senior_keywords = [
        "Colonel", "Brigadier General", "Major General", "Lieutenant General", "General", 
        "대령", "준장", "소장", "중장", "대장", "원수"
    ]
    if any(kw.lower() in rank_name.lower() for kw in senior_keywords):
        is_senior = True
    
    return (is_junior, is_senior) 

LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
os.makedirs(LOG_DIR, exist_ok=True) 

def save_verification_log(discord_nick: str, roblox_nick: str):
    """인증 성공 시 로그 파일에 기록 + 콘솔에 같이 출력"""
    log_file = os.path.join(LOG_DIR, "verification_log.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] [{discord_nick}]: [{roblox_nick}]" 

    try:
        # 파일에 저장 (Volume용)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n") 

        # Deploy Logs 에도 출력
        print("[VERIFY_LOG]", line)
        print("/인증 로블닉:{}")
    except Exception as e:
        print(f"로그 저장 실패: {e}") 

def set_guild_group_id(guild_id: int, group_id: int) -> None:
    cursor.execute(
        """
        INSERT INTO group_settings(guild_id, group_id)
        VALUES(?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET group_id=excluded.group_id
        """,
        (guild_id, group_id),
    )
    conn.commit()


def get_guild_role_id(guild_id: int) -> Optional[int]:
    cursor.execute("SELECT role_id FROM settings WHERE guild_id=?", (guild_id,))
    row = cursor.fetchone()
    return row[0] if row else None


def set_guild_role_id(guild_id: int, role_id: int) -> None:
    cursor.execute(
        """
        INSERT INTO settings(guild_id, role_id)
        VALUES(?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET role_id=excluded.role_id
        """,
        (guild_id, role_id),
    )
    conn.commit()

async def send_admin_log(
    guild: discord.Guild,
    title: str,
    description: str | None = None,
    color: discord.Color = discord.Color.blurple(),
    fields: list[tuple[str, str, bool]] | None = None,  # (name, value, inline)
):
    log_ch_id = get_log_channel(guild.id, "admin")
    if not log_ch_id:
        return

    channel = guild.get_channel(log_ch_id) or await guild.fetch_channel(log_ch_id)
    if not channel:
        return

    embed = discord.Embed(title=title, color=color, description=description)
    if fields:
        for name, value, inline in fields:
            embed.add_field(name=name, value=value, inline=inline)

    embed.set_footer(text="관리자 로그")
    await channel.send(embed=embed)

def set_log_channel(guild_id: int, log_type: str, channel_id: int | None):
    if channel_id is None:
        cursor.execute(
            "DELETE FROM logchannels WHERE guildid=? AND logtype=?",
            (guild_id, log_type),
        )
    else:
        cursor.execute(
            """
            INSERT INTO logchannels(guildid, logtype, channelid)
            VALUES (?, ?, ?)
            ON CONFLICT(guildid, logtype)
            DO UPDATE SET channelid=excluded.channelid
            """,
            (guild_id, log_type, channel_id),
        )
    conn.commit() 

def get_log_channel(guild_id: int, log_type: str) -> int | None:
    cursor.execute(
        "SELECT channelid FROM logchannels WHERE guildid=? AND logtype=?",
        (guild_id, log_type),
    )
    row = cursor.fetchone()
    return row[0] if row else None 

def get_guild_admin_role_ids(guild_id: int) -> list[int]:
    cursor.execute("SELECT admin_role_id FROM settings WHERE guild_id=?", (guild_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return []
    try:
        import json 

        if isinstance(row[0], str):
            return list(map(int, json.loads(row[0])))
        return [int(row[0])]
    except Exception:
        return []


def set_guild_admin_role_ids(guild_id: int, role_ids: list[int]) -> None:
    import json 

    value = json.dumps(role_ids)
    cursor.execute(
        """
        INSERT INTO settings(guild_id, admin_role_id)
        VALUES(?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET admin_role_id=excluded.admin_role_id
        """,
        (guild_id, value),
    )
    conn.commit()


def is_owner(user: discord.abc.User | discord.Member) -> bool:
    if OWNER_ID <= 0:
        return False
    return int(user.id) == int(OWNER_ID)


def is_admin(member: discord.Member) -> bool:
    # 1) 제작자
    if is_owner(member):
        return True 

    # 2) 서버 관리자 권한
    try:
        if member.guild_permissions.administrator:
            return True
    except AttributeError:
        return False 

    # 3) 설정된 관리자 역할
    guild = member.guild
    if guild is None:
        return False 

    admin_ids = get_guild_admin_role_ids(guild.id)
    if not admin_ids:
        return False 

    member_role_ids = {r.id for r in member.roles}
    if any(rid in member_role_ids for rid in admin_ids):
        return True 

    return False 

def _rank_api_headers():
    return {
        "Content-Type": "application/json",
        "X-API-KEY": RANK_API_KEY,
    } 

def add_error_log(error_msg: str) -> None:
    error_logs.append({"timestamp": datetime.now(timezone.utc), "message": error_msg})
    if len(error_logs) > MAX_LOGS:
        error_logs.pop(0)


def generate_code() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=8)) 

# ---------- Roblox API ---------- 

ROBLOX_USERNAME_API = "https://users.roblox.com/v1/usernames/users"
ROBLOX_USER_API = "https://users.roblox.com/v1/users/{userId}"


async def roblox_get_user_id_by_username(username: str) -> Optional[int]:
    payload = {"usernames": [username], "excludeBannedUsers": True}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                ROBLOX_USERNAME_API,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                results = data.get("data", [])
                return results[0].get("id") if results else None
        except Exception as e:
            add_error_log(f"roblox_get_user_id: {repr(e)}")
            return None 

async def roblox_get_user_groups(user_id: int) -> list[int]:
    """사용자가 속한 Roblox 그룹 ID 목록을 반환합니다."""
    url = f"https://groups.roblox.com/v2/users/{user_id}/groups/roles"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    print(
                        f"DEBUG: Roblox API error for user {user_id}: "
                        f"status {resp.status}"
                    )
                    return [] 

                data = await resp.json()
                print(f"DEBUG: Roblox API response for {user_id}: {data}") 

                groups = data.get("data", [])
                group_ids = [
                    g.get("group", {}).get("id")
                    for g in groups
                    if g.get("group")
                ]
                print(f"DEBUG: Extracted group_ids: {group_ids}")
                return group_ids
        except Exception as e:
            add_error_log(f"roblox_get_user_groups: {repr(e)}")
            print(f"DEBUG: Exception in roblox_get_user_groups: {e}")
            return [] 
        
async def apply_punishment(
    guild: discord.Guild,
    member: discord.Member,
    punish_type: str,
    duration: int | None,
    executor: discord.Member | discord.User,
    reason: str | None,
):
    reason_text = reason or f"자동 처벌 (by {executor})"

    if punish_type == "ban":
        await guild.ban(member, reason=reason_text)
    elif punish_type == "kick":
        await guild.kick(member, reason=reason_text)
    elif punish_type == "timeout":
        if duration and duration > 0:
            await member.timeout(timedelta(seconds=duration), reason=reason_text)
    elif punish_type == "mute":
        # 미리 뮤트 역할을 정해놓고 여기서 add_roles
        mute_role_id = ...  # 설정값에서 가져오기
        mute_role = guild.get_role(mute_role_id)
        if mute_role:
            await member.add_roles(mute_role, reason=reason_text)

async def roblox_get_description_by_user_id(user_id: int) -> Optional[str]:
    url = ROBLOX_USER_API.format(userId=user_id)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data.get("description")
        except Exception as e:
            add_error_log(f"roblox_get_description: {repr(e)}")
            return None
        
def get_officer_role_id(guild_id: int) -> Optional[int]:
    cursor.execute("SELECT officer_role_id FROM officer_settings WHERE guild_id=?", (guild_id,))
    row = cursor.fetchone()
    return row[0] if row else None 

def set_officer_role_id(guild_id: int, role_id: int) -> None:
    cursor.execute(
        """INSERT OR REPLACE INTO officer_settings(guild_id, officer_role_id)
           VALUES(?, ?)""",
        (guild_id, role_id),
    )
    conn.commit()


# ---------- 인증 View ---------- 

class VerifyView(discord.ui.View):
    def __init__(self, code: str, expire_time: datetime, guild_id: int):
        super().__init__(timeout=300)
        self.code = code
        self.expire_time = expire_time
        self.guild_id = guild_id 

# ---------- View 클래스 ----------
def send_log_to_web(guild_id: int, user_id: int, action: str, detail: str):
    try:
        resp = requests.post(
            "https://web-api-production-69fc.up.railway.app/api/log",  # ← /api/log 로 변경
            json={
                "guild_id": guild_id,
                "user_id": user_id,
                "action": action,
                "detail": detail,
            },
            timeout=5,
        )
        print("[WEB_LOG]", resp.status_code, resp.text)
    except Exception as e:
        print("[WEB_LOG_ERROR]", repr(e))


class VerifyView(discord.ui.View):
    def __init__(
        self,
        code: str,
        expiretime: datetime,
        guildid: int,
        roblox_nick: str,
        roblox_user_id: int,
    ):
        super().__init__(timeout=300)
        self.code = code
        self.expiretime = expiretime
        self.guildid = guildid
        self.roblox_nick = roblox_nick
        self.roblox_user_id = roblox_user_id

    @discord.ui.button(label="인증하기", style=discord.ButtonStyle.green)
    async def verifybutton(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction is None:
            return

        try:
            # 0) 길드 확보
            guild: Optional[discord.Guild] = interaction.guild or bot.get_guild(self.guildid)
            if guild is None:
                print(
                    f"[WEB_LOG_ERROR_VERIFY_BUTTON] guild is None, "
                    f"user={interaction.user} guild_id={self.guildid}"
                )
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "길드를 찾을 수 없습니다. 서버에서 다시 /인증 해 주세요.",
                        ephemeral=True,
                    )
                return

            # 1) 만료 체크
            if datetime.now() > self.expiretime:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "인증 코드가 만료되었습니다. 다시 /인증 명령을 사용해 주세요.",
                        ephemeral=True,
                    )
                return

            # 2) Roblox 프로필 설명에서 코드 확인
            description = await roblox_get_description_by_user_id(self.roblox_user_id)
            if description is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "Roblox 프로필 설명을 가져오지 못했습니다. 잠시 후 다시 시도해 주세요.",
                        ephemeral=True,
                    )
                return

            if self.code not in description:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "Roblox 프로필 설명에 인증 코드가 없습니다. 설명에 코드를 넣고 다시 시도해 주세요.",
                        ephemeral=True,
                    )
                return

            # 3) 역할 부여 + 관리자 로그
            config_role_id = get_guild_role_id(guild.id)

            KST = timezone(timedelta(hours=9))
            now_kst = datetime.now(KST)

            member = guild.get_member(interaction.user.id)
            if member is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "서버에서 회원 정보를 찾을 수 없습니다.",
                        ephemeral=True,
                    )
                return

            verify_role = guild.get_role(VERIFY_ROLE_ID)
            unverify_role = guild.get_role(UNVERIFY_ROLE_ID)
            log_channel = guild.get_channel(ADMIN_LOG_CHANNEL_ID)

            if verify_role is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "인증 역할을 찾을 수 없습니다. 관리자에게 문의해 주세요.",
                        ephemeral=True,
                    )
                return

            # 이미 인증된 경우 중복 방지
            if verify_role in member.roles:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "이미 인증된 상태입니다.",
                        ephemeral=True,
                    )
                return

            account_created = member.created_at.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

            # 🔴 기존 역할 제거
            if unverify_role and unverify_role in member.roles:
                await member.remove_roles(unverify_role)

                if log_channel:
                    embed_remove = discord.Embed(
                        title="🔴 역할 제거",
                        color=discord.Color.red(),
                        timestamp=now_kst
                    )

                    if guild.icon:
                        embed_remove.set_thumbnail(url=guild.icon.url)

                    embed_remove.add_field(
                        name="디스코드",
                        value=(
                            f"{member.mention}\n"
                            f"{member.name}\n"
                            f"ID: {member.id}\n"
                            f"계정 생성일: {account_created}"
                        ),
                        inline=False
                    )

                    embed_remove.add_field(
                        name="로블록스",
                        value=f"{self.roblox_nick}",
                        inline=False
                    )

                    embed_remove.add_field(
                        name="역할",
                        value=f"{unverify_role.mention}",
                        inline=False
                    )

                    embed_remove.add_field(
                        name="실행자",
                        value=f"{interaction.user.mention}",
                        inline=False
                    )

                    embed_remove.set_footer(text="Made by Lunar | KST(UTC+9)")

                    await log_channel.send(embed=embed_remove)

            # 🟢 인증 역할 추가
            await member.add_roles(verify_role)

            if log_channel:
                embed_add = discord.Embed(
                    title="🟢 역할 추가",
                    color=discord.Color.green(),
                    timestamp=now_kst
                )

                if guild.icon:
                    embed_add.set_thumbnail(url=guild.icon.url)

                embed_add.add_field(
                    name="디스코드",
                    value=(
                        f"{member.mention}\n"
                        f"{member.name}\n"
                        f"ID: {member.id}\n"
                        f"계정 생성일: {account_created}"
                    ),
                    inline=False
                )

                embed_add.add_field(
                    name="로블록스",
                    value=f"{self.roblox_nick}",
                    inline=False
                )

                embed_add.add_field(
                    name="역할",
                    value=f"{verify_role.mention}",
                    inline=False
                )

                embed_add.add_field(
                    name="실행자",
                    value=f"{interaction.user.mention}",
                    inline=False
                )

                embed_add.set_footer(text="Made by Lunar | KST(UTC+9)")

                await log_channel.send(embed=embed_add)

            # 4) (선택) 랭크 API / 닉네임 변경 블럭 완전히 제거됨

            # 5) 파일/콘솔 로그
            try:
                save_verification_log(member.name, self.roblox_nick)
            except Exception as e:
                print("[VERIFY_LOG_ERROR]", e)

            # 6) 웹 로그
            send_log_to_web(
                guild_id=guild.id,
                user_id=interaction.user.id,
                action="verify_success",
                detail=f"{self.roblox_nick} ({self.roblox_user_id})",
            )

            # 7) 인증 성공 로그 embed
            try:
                log_ch_id = get_log_channel(guild.id, "verify")
                if log_ch_id:
                    log_ch = guild.get_channel(log_ch_id) or await guild.fetch_channel(log_ch_id)
                    if log_ch:
                        success_embed = make_verify_embed(
                            VerifyLogType.SUCCESS,
                            user=member,
                            roblox_nick=self.roblox_nick,
                            group_rank=None,          # ← rankname 대신 None
                            account_age_days=None,
                            new_nick=member.nick,     # 실제 현재 닉 그대로
                            at_time=datetime.now(),
                        )
                        await log_ch.send(embed=success_embed)
            except Exception as e:
                print("[VERIFY_SUCCESS_LOG_ERROR]", repr(e))

            # 8) 유저 응답
            if not interaction.response.is_done():
                await interaction.response.send_message("인증이 완료되었습니다!", ephemeral=True)

        except Exception as e:
            add_error_log(f"verifybutton: {repr(e)}")
            print("[WEB_LOG_ERROR_VERIFY_BUTTON]", repr(e))
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "인증 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
                    ephemeral=True,
                )


# ---------- 클래스 ----------
class VerifyLogType(str, Enum):
    REQUEST = "request"
    SUCCESS = "success"
    NO_GROUP = "no_group"
    INVALID_NICK = "invalid_nick" 

class RankLogType(str, Enum):
    PROMOTE = "promote"
    DEMOTE = "demote" 

class RankSummaryType(str, Enum):
    BULK_PROMOTE = "bulk_promote"
    BULK_DEMOTE = "bulk_demote"

class ShopView(View):
    def __init__(self, guild: discord.Guild, items: list[tuple]):
        super().__init__(timeout=60)
        self.guild = guild
        self.items = items  # [(name, price, type, role_id, level, exp), ...]
        self.index = 0      # 현재 페이지
        self.per_page = 10

    def make_page_embed(self) -> discord.Embed:
        start = self.index * self.per_page
        end = start + self.per_page
        chunk = self.items[start:end]

        lines = []
        for name, price, itype, role_id, level_val, exp_val in chunk:
            extra = ""
            if itype == "role" and role_id:
                role = self.guild.get_role(role_id)
                if role:
                    extra = f" → 역할: {role.mention}"
            elif itype == "level" and level_val is not None:
                extra = f" → 레벨 +{level_val}"
            elif itype == "exp" and exp_val is not None:
                extra = f" → 경험치 +{exp_val}"

            lines.append(f"• `{name}` | 가격: `{price}` | 타입: `{itype}`{extra}")

        desc = "\n".join(lines) if lines else "이 페이지에는 아이템이 없습니다."

        embed = discord.Embed(
            title=f"🛒 아이템 상점 (페이지 {self.index+1}/{self.max_page})",
            description=desc,
            color=discord.Color.blurple(),
        )
        return embed

    @property
    def max_page(self) -> int:
        # 페이지 수 (0-based index → 길이)
        return max(1, (len(self.items) + self.per_page - 1) // self.per_page)

    async def update(self, interaction: discord.Interaction):
        embed = self.make_page_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @button(label="⬅ 이전", style=ButtonStyle.gray)
    async def prev(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.index > 0:
            self.index -= 1
        await self.update(interaction)

    @button(label="다음 ➡", style=ButtonStyle.gray)
    async def next(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.index < self.max_page - 1:
            self.index += 1
        await self.update(interaction)

# ---------- 엠베드 ----------
def make_verify_embed(
    log_type: VerifyLogType,
    *,
    user: discord.abc.User | discord.Member | None = None,
    roblox_nick: str | None = None,
    group_rank: str | None = None,
    account_age_days: int | None = None,
    code: str | None = None,
    new_nick: str | None = None,
    group_id: int | None = None,
    input_nick: str | None = None,
    fail_reason: str | None = None,
    at_time: datetime | None = None,
) -> discord.Embed:
    at_time = at_time or datetime.now() 

    if log_type is VerifyLogType.REQUEST:
        embed = discord.Embed(
            title="✅ 인증 요청",
            color=discord.Color.blurple(),
            description="새로운 인증 코드 발급",
        )
        if user:
            embed.add_field(name="유저", value=user.mention, inline=False)
        if roblox_nick:
            embed.add_field(name="로블록스", value=f"`{roblox_nick}`", inline=True)
        if group_rank:
            embed.add_field(name="그룹 랭크", value=group_rank, inline=True)
        if account_age_days is not None:
            embed.add_field(name="계정 나이", value=f"{account_age_days}일", inline=True)
        if code:
            embed.add_field(name="인증 코드", value=f"`{code}`", inline=True) 

    elif log_type is VerifyLogType.SUCCESS:
        embed = discord.Embed(
            title="<:verfired_green:1479810239619530752> 인증 성공",
            color=discord.Color.green(),
            description="새로운 유저가 인증을 완료했습니다.",
        )
        if user:
            embed.add_field(name="유저", value=user.mention, inline=False)
        if roblox_nick:
            embed.add_field(name="로블록스", value=f"`{roblox_nick}`", inline=True)
        if group_rank:
            embed.add_field(name="그룹 랭크", value=group_rank, inline=True)
        if account_age_days is not None:
            embed.add_field(name="계정 나이", value=f"{account_age_days}일", inline=True)
        if new_nick:
            embed.add_field(name="새 닉네임", value=f"`{new_nick}`", inline=False)
        embed.add_field(
            name="인증 시각",
            value=at_time.strftime("%Y년 %m월 %d일 %A %p %I:%M"),
            inline=False,
        ) 

    elif log_type is VerifyLogType.NO_GROUP:
        embed = discord.Embed(
            title="<:_red:1479810110632099972> 그룹 미가입",
            color=discord.Color.orange(),
            description="그룹 미가입 상태로 인증 실패",
        )
        if user:
            embed.add_field(name="유저", value=user.mention, inline=False)
        if roblox_nick:
            embed.add_field(name="로블록스", value=f"`{roblox_nick}`", inline=True)
        if group_id is not None:
            embed.add_field(name="그룹 ID", value=str(group_id), inline=True) 

    elif log_type is VerifyLogType.INVALID_NICK:
        embed = discord.Embed(
            title="<:_red:1479810110632099972> 인증 실패",
            color=discord.Color.red(),
            description="존재하지 않는 로블록스 닉네임",
        )
        if user:
            embed.add_field(name="유저", value=user.mention, inline=False)
        if input_nick:
            embed.add_field(name="입력한 닉네임", value=f"`{input_nick}`", inline=True)
        embed.add_field(
            name="실패 사유",
            value=fail_reason or "사용자를 찾을 수 없음",
            inline=False,
        )
    else:
        embed = discord.Embed(title="알 수 없는 로그 타입", color=discord.Color.dark_grey()) 

    embed.set_footer(text="Made By Lunar")
    return embed 

def make_rank_log_embed(
    log_type: RankLogType,
    *,
    target_name: str,
    old_rank: str,
    new_rank: str,
    executor: discord.abc.User | discord.Member | None = None,
) -> discord.Embed:
    if log_type is RankLogType.DEMOTE:
        title = "⬇️ 강등"
        desc = "멤버가 강등되었습니다."
        color = discord.Color.red()
    else:
        title = "⬆️ 승진"
        desc = "멤버가 승진되었습니다."
        color = discord.Color.green() 

    embed = discord.Embed(title=title, description=desc, color=color) 

    embed.add_field(name="대상", value=f"`{target_name}`", inline=False)
    embed.add_field(name="이전 랭크", value=old_rank, inline=True)
    embed.add_field(name="새 랭크", value=new_rank, inline=True) 

    if executor:
        embed.add_field(name="실행자", value=executor.mention, inline=False) 

    embed.set_footer(text="Made By Lunar")
    return embed 

def make_bulk_rank_summary_embed(
    summary_type: RankSummaryType,
    *,
    role_name: str,
    total: int,
    success: int,
    failed: int,
    executor: discord.abc.User | discord.Member | None = None,
) -> discord.Embed:
    if summary_type is RankSummaryType.BULK_PROMOTE:
        title = "<:Chack_blue:1479810189434683402> 일괄 승진 완료"
        color = discord.Color.green()
        desc = "여러 멤버 승진 작업이 완료되었습니다."
    else:
        title = "<:Chack_blue:1479810189434683402> 일괄 강등 완료"
        color = discord.Color.red()
        desc = "여러 멤버 강등 작업이 완료되었습니다." 

    embed = discord.Embed(title=title, description=desc, color=color)
    embed.add_field(name="변경 역할", value=f"`{role_name}`", inline=False)
    embed.add_field(name="총 처리", value=f"{total}명", inline=True)
    embed.add_field(name="<:Chack_blue:1479810189434683402> 성공", value=f"{success}명", inline=True)
    embed.add_field(name="<:X_red:1479810084900044851> 실패", value=f"{failed}명", inline=True) 

    if executor:
        embed.add_field(name="실행자", value=executor.mention, inline=False) 

    embed.set_footer(text="Made By Lunar")
    return embed
# ---------- 슬래시 명령어 ---------- 
# 인증
@bot.tree.command(name="인증", description="로블록스 계정 인증을 시작합니다.")
@app_commands.describe(로블닉="로블록스 닉네임")
async def verify(interaction: discord.Interaction, 로블닉: str):
    await interaction.response.defer(ephemeral=True)


    print(
        f"/인증 로블닉:{로블닉} "
        f"(user={interaction.user} id={interaction.user.id})"
    )

    if is_already_verified(interaction.guild.id, interaction.user.id):
        await interaction.followup.send(
            "이미 인증된 사용자입니다. (웹 로그 기준)",
            ephemeral=True,
        )
        return

    user_id = await roblox_get_user_id_by_username(로블닉)
    if not user_id:
        await interaction.followup.send(
            "해당 닉네임의 로블록스 계정을 찾을 수 없습니다.",
            ephemeral=True,
        )
        return
    

    cursor.execute(
        "SELECT group_id FROM blacklist WHERE guild_id=?",
        (interaction.guild.id,),
    )
    blacklist_groups = {row[0] for row in cursor.fetchall()}
    if blacklist_groups:
        

        user_groups = await roblox_get_user_groups(user_id)
        blocked_groups = [g for g in user_groups if g in blacklist_groups]
        if blocked_groups:
            await interaction.followup.send(
                "❌ 블랙리스트된 그룹에 속해 있어서 인증할 수 없습니다.\n"
                f"차단된 그룹: {', '.join(map(str, blocked_groups))}",
                ephemeral=True,
            )
            return

    code = generate_code()
    expire_time = datetime.now() + timedelta(minutes=5)

    # DM용 안내 embed
    dm_embed = discord.Embed(
        title="로블록스 인증",
        color=discord.Color.blue(),
    )
    dm_embed.description = (
        f"> Roblox: `{로블닉}` (ID: `{user_id}`)\n"
        f"> 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        "1️⃣ Roblox 프로필로 이동\n"
        "2️⃣ 설명란에 코드 입력\n"
        "3️⃣ '인증하기' 버튼 클릭\n\n"
        f"🔐 코드: `{code}`\n"
        "⏱ 남은 시간: 5분\n\n"
        "Made by Lunar"
    )

    view = VerifyView(
        code=code,
        expiretime=expire_time,
        guildid=interaction.guild.id,
        roblox_nick=로블닉,
        roblox_user_id=user_id,
    )

    # ✅ 인증 요청 로그 채널로 전송
    try:
        log_ch_id = get_log_channel(interaction.guild.id, "verify")
        if log_ch_id:
            log_ch = interaction.guild.get_channel(log_ch_id) or await interaction.guild.fetch_channel(log_ch_id)
            if log_ch:
                req_embed = make_verify_embed(
                    VerifyLogType.REQUEST,
                    user=interaction.user,
                    roblox_nick=로블닉,
                    code=code,
                )
                await log_ch.send(embed=req_embed)
    except Exception as e:
        print("[VERIFY_REQUEST_LOG_ERROR]", repr(e))

    # DM 전송
    try:
        await interaction.user.send(embed=dm_embed, view=view)
        await interaction.followup.send("📩 DM을 확인해주세요.", ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send(
            "DM 전송에 실패했습니다. DM 수신을 허용하고 다시 시도해주세요.",
            ephemeral=True,
        )

@bot.tree.command(name="일괄강제인증", description="현재 서버의 모든 미인증자를 강제인증 처리합니다. (제작자 전용)")
async def bulk_force_verify(interaction: discord.Interaction):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    # 🔐 제작자(OWNER_ID)만 허용
    if not is_owner(interaction.user):
        await interaction.response.send_message("제작자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    # 로그 채널
    log_channel_id = get_log_channel(guild.id, "verify")
    log_channel: discord.TextChannel | None = guild.get_channel(log_channel_id) if log_channel_id else None

    # 대상 멤버 (봇 제외)
    members: list[discord.Member] = [m for m in guild.members if not m.bot]

    # 미인증자 필터 (웹 API 사용 안 함)
    verified_ids: set[int] = set()
    loop = asyncio.get_running_loop()

    def _check_one(user_id: int) -> bool:
        # is_already_verified가 웹 안 쓰도록 바꿨다면 그대로, 아니면 여기서 DB-only 버전 사용
        return is_already_verified(guild.id, user_id)

    async def check_verified(m: discord.Member):
        is_verified = await loop.run_in_executor(None, _check_one, m.id)
        if is_verified:
            verified_ids.add(m.id)

    await asyncio.gather(*(check_verified(m) for m in members))

    targets = [m for m in members if m.id not in verified_ids]

    total = len(targets)
    success = 0
    fail = 0

    # 진행 상황 엠베드
    if log_channel:
        embed = discord.Embed(
            title="<:Chack_blue:1479810189434683402> 일괄 강제인증 시작",
            description=f"대상 인원: {total}명",
            color=discord.Color.orange()
        )
        embed.add_field(name="<:Chack_blue:1479810189434683402> 성공", value=str(success))
        embed.add_field(name="<:X_red:1479810084900044851> 실패", value=str(fail))
        embed.set_footer(text=f"요청자: {interaction.user} ({interaction.user.id})")
        progress_msg = await log_channel.send(embed=embed)
    else:
        progress_msg = None

    for idx, member in enumerate(targets, start=1):
        try:
            verify_role = guild.get_role(VERIFY_ROLE_ID)
            unverify_role = guild.get_role(UNVERIFY_ROLE_ID)

            if verify_role and verify_role in member.roles:
                continue

            cursor.execute(
                """
                INSERT OR REPLACE INTO forced_verified(discord_id, guild_id, roblox_nick, roblox_user_id, rank_role)
                VALUES(?, ?, ?, ?, ?)
                """,
                (member.id, guild.id, None, None, "forced")
            )
            conn.commit()

            if verify_role:
                await member.add_roles(verify_role, reason="일괄 강제인증")
            if unverify_role and unverify_role in member.roles:
                await member.remove_roles(unverify_role, reason="일괄 강제인증")

            send_log_to_web(
                guild_id=guild.id,
                user_id=member.id,
                action="force_verify_bulk",
                detail=f"일괄 강제인증 처리 (요청자: {interaction.user.id})"
            )

            success += 1

        except Exception as e:
            fail += 1
            add_error_log(f"bulk_force_verify: {repr(e)}")

        # 게이트웨이 지연 방지용 약간의 양보
        if idx % 20 == 0:
            await asyncio.sleep(0)

        if progress_msg and (idx % 10 == 0 or idx == total):
            progress_embed = discord.Embed(
                title="일괄 강제인증 진행 중",
                description=f"{idx}/{total}명 처리 완료",
                color=discord.Color.blurple()
            )
            progress_embed.add_field(name="성공", value=str(success))
            progress_embed.add_field(name="실패", value=str(fail))
            progress_embed.set_footer(text=f"요청자: {interaction.user} ({interaction.user.id})")
            try:
                await progress_msg.edit(embed=progress_embed)
            except discord.NotFound:
                progress_msg = None

    # stats 업데이트
    cursor.execute(
        """
        INSERT INTO stats(guild_id, verify_count, force_count, cancel_count)
        VALUES(?, 0, ?, 0)
        ON CONFLICT(guild_id) DO UPDATE SET force_count = stats.force_count + ?
        """,
        (guild.id, success, success)
    )
    conn.commit()

    result_text = (
        f"대상: {total}명\n"
        f"<:Chack_blue:1479810189434683402> 성공: {success}명\n"
        f"<:X_red:1479810084900044851>  실패: {fail}명"
    )

    # 최종 응답 (Unknown Message 방지)
    try:
        if interaction.response.is_done():
            await interaction.edit_original_response(content=result_text)
        else:
            await interaction.response.send_message(result_text)
    except discord.NotFound:
        pass

    if log_channel:
        final_embed = discord.Embed(
            title="<:Chack_blue:1479810189434683402> 일괄 강제인증 완료",
            description=result_text,
            color=discord.Color.green()
        )
        final_embed.set_footer(text=f"요청자: {interaction.user} ({interaction.user.id})")
        await log_channel.send(embed=final_embed)

@bot.tree.command(name="돈추가", description="유저에게 돈을 추가합니다. (관리자)")
@app_commands.describe(
    유저="돈을 받을 유저",
    금액="추가할 금액"
)
async def add_money(
    interaction: discord.Interaction,
    유저: discord.Member,
    금액: int
):

    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    if 금액 <= 0:
        await interaction.response.send_message("금액은 1 이상이어야 합니다.", ephemeral=True)
        return

    # 유저 생성 (없으면 생성)
    user = get_user(유저.id)

    # 돈 추가
    cur.execute(
        "UPDATE economy SET money = money + ? WHERE user_id=?",
        (금액, 유저.id)
    )
    conn.commit()

    await interaction.response.send_message(
        f"💰 {유저.mention}에게 `{금액}`원을 추가했습니다."
    )

@bot.tree.command(name="강제인증해제", description="특정 유저의 강제인증을 해제합니다. (관리자)")
@app_commands.describe(
    user="강제인증을 해제할 디스코드 유저"
)
async def force_unverify(
    interaction: discord.Interaction,
    user: discord.User,
):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("길드에서만 사용할 수 있습니다.", ephemeral=True)
        return

    member = guild.get_member(user.id)
    if member is None:
        await interaction.followup.send("해당 유저를 서버에서 찾을 수 없습니다.", ephemeral=True)
        return

    verify_role = guild.get_role(VERIFY_ROLE_ID)
    unverify_role = guild.get_role(UNVERIFY_ROLE_ID)

    # DB에서 강제인증 기록 삭제
    cursor.execute(
        "DELETE FROM forced_verified WHERE discord_id = ? AND guild_id = ?",
        (member.id, guild.id),
    )
    conn.commit()

    # 역할 롤백
    try:
        if verify_role and verify_role in member.roles:
            await member.remove_roles(verify_role, reason="강제인증 해제")
        if unverify_role and unverify_role not in member.roles:
            await member.add_roles(unverify_role, reason="강제인증 해제")
    except Exception as e:
        add_error_log(f"force_unverify_roles: {repr(e)}")
        await interaction.followup.send(f"역할 변경 중 오류 발생: {e}", ephemeral=True)
        return

    # 웹 로그 (기존)
    send_log_to_web(
        guild_id=guild.id,
        user_id=member.id,
        action="force_unverify",
        detail=f"강제인증 해제 (요청자: {interaction.user.id})",
    )

    # stats.cancel_count 증가
    cursor.execute(
        """
        INSERT INTO stats(guild_id, verify_count, force_count, cancel_count)
        VALUES(?, 0, 0, 1)
        ON CONFLICT(guild_id) DO UPDATE SET cancel_count = stats.cancel_count + 1
        """,
        (guild.id,),
    )
    conn.commit()

    await interaction.followup.send(
        f"{member.mention} 님의 강제인증을 해제했습니다.",
        ephemeral=True,
    )

    # ✅ 1) 강제인증 전용 로그 채널에 임베드
    force_log_ch_id = get_log_channel(guild.id, "force_verify")
    if force_log_ch_id:
        force_log_ch = guild.get_channel(force_log_ch_id) or await guild.fetch_channel(force_log_ch_id)
        if force_log_ch:
            embed = discord.Embed(
                title="<:_red:1479810110632099972> 강제인증 해제",
                color=discord.Color.red(),
                description="관리자가 강제인증을 해제했습니다.",
            )
            embed.add_field(
                name="대상 유저",
                value=f"{member.mention} (`{member.id}`)",
                inline=False,
            )
            embed.add_field(
                name="실행자",
                value=f"{interaction.user.mention} (`{interaction.user.id}`)",
                inline=False,
            )
            embed.set_footer(text="강제인증 로그")
            await force_log_ch.send(embed=embed)

    # ✅ 2) 관리자 로그 채널에도 임베드 (send_admin_log 쓴다면)
    if guild:
        await send_admin_log(
            guild,
            title="🔴 강제인증 해제",
            description="관리자가 강제인증을 해제했습니다.",
            color=discord.Color.red(),
            fields=[
                ("대상 유저", f"{member.mention} (`{member.id}`)", False),
                ("실행자", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
            ],
        )

@bot.tree.command(name="강제인증", description="유저를 강제로 인증 처리합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(
    user="Discord 유저 멘션",
    roblox_nick="Roblox 닉네임"
)
async def force_verify(interaction: discord.Interaction, user: discord.User, roblox_nick: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    await interaction.response.defer(ephemeral=True)
    
    user_id = await roblox_get_user_id_by_username(roblox_nick)
    if not user_id:
        await interaction.followup.send(
            f"해당 닉네임의 로블록스 계정을 찾을 수 없습니다.",
            ephemeral=True,
        )
        return 

    # users 테이블에 verified=1로 저장
    cursor.execute(
        """INSERT OR REPLACE INTO users(discord_id, guild_id, roblox_nick, roblox_user_id, code, expire_time, verified)
           VALUES(?, ?, ?, ?, ?, ?, 1)""",
        (user.id, interaction.guild.id, roblox_nick, user_id, "forced", datetime.now().isoformat()),
    )
    conn.commit() 

    # 강제인증 로그 기록
    try:
        save_verification_log(user.name, roblox_nick)
    except:
        pass 

    # 인증 역할 부여
    role_id = get_guild_role_id(interaction.guild.id)
    member = interaction.guild.get_member(user.id)
    
    if role_id and member:
        role = interaction.guild.get_role(role_id)
        if role:
            try:
                await member.add_roles(role)
            except:
                pass 

    # 현재 랭크 조회 및 닉네임 변경
    try:
        resp = requests.post(
            f"{RANK_API_URL_ROOT}/bulk-status",
            json={"usernames": [roblox_nick]},
            headers=_rank_api_headers(),
            timeout=15,
        )
        
        rank_name = "?"
        rank_num = 0
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            if results and results[0].get("success"):
                role_info = results[0].get("role", {})
                rank_name = role_info.get("name", "?")
                rank_num = role_info.get("rank", 0)

        is_junior, is_senior = check_is_officer(rank_num, rank_name)
        
        officer_role_id = get_officer_role_id(interaction.guild.id)
        if officer_role_id and is_junior:
            officer_role = interaction.guild.get_role(officer_role_id)
            if officer_role and member:
                await member.add_roles(officer_role)
        
        senior_officer_role_id = get_senior_officer_role_id(interaction.guild.id)
        if senior_officer_role_id and is_senior:
            senior_officer_role = interaction.guild.get_role(senior_officer_role_id)
            if senior_officer_role and member:
                await member.add_roles(senior_officer_role)
        
    except Exception as e:
        print(f"강제인증 추가 처리 실패: {e}") 

    embed = discord.Embed(
        title="강제인증 완료",
        color=discord.Color.green(),
        description=f"{user.mention} 을(를) {roblox_nick}로 인증 처리했습니다."
    )
    send_log_to_web(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        action="verify_success",
        detail=f"{roblox_nick} ({user_id})",
    ) 

    await interaction.followup.send(embed=embed, ephemeral=True)

    # 관리자 로그에도 기록
    guild = interaction.guild
    if guild:
        await send_admin_log(
            guild,
            title="<:verfired_green:1479810239619530752> 강제인증 실행",
            description="관리자가 유저를 강제인증 처리했습니다.",
            color=discord.Color.green(),
            fields=[
                ("대상 유저", f"{user.mention} (`{user.id}`)", False),
                ("로블록스 닉네임", f"`{roblox_nick}`", False),
                ("실행자", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
            ],
        )

@bot.tree.command(name="공지", description="인증된 모든 유저에게 공지 전송")
@app_commands.describe(
    message="공지 내용",
    color="embed 색상"
)
@app_commands.choices(color=[
    app_commands.Choice(name="파란색", value="blue"),
    app_commands.Choice(name="초록색", value="green"),
    app_commands.Choice(name="빨간색", value="red"),
    app_commands.Choice(name="주황색", value="orange"),
    app_commands.Choice(name="보라색", value="purple"),
    app_commands.Choice(name="금색", value="gold"),
])
async def announce(interaction: discord.Interaction, message: str, color: app_commands.Choice[str] = None):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return
    
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    
    # 색상
    colors = {
        "blue": discord.Color.blue(),
        "green": discord.Color.green(),
        "red": discord.Color.red(),
        "orange": discord.Color.orange(),
        "purple": discord.Color.purple(),
        "gold": discord.Color.gold(),
    }
    selected = colors.get(color.value if color else "blue", discord.Color.blue())
    
    # 인증 유저
    cursor.execute("SELECT DISTINCT discordid FROM users WHERE guildid=? AND verified=1", (guild.id,))
    verified = [r[0] for r in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT discordid FROM forcedverified WHERE guildid=?", (guild.id,))
    forced = [r[0] for r in cursor.fetchall()]
    
    all_users = list(set(verified + forced))
    
    if not all_users:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return
    
    # Embed
    embed = discord.Embed(
        title="📢 공지사항",
        description=message,
        color=selected,
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text=f"발신: {interaction.user.name}")
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    
    # 공지 채널
    ch_id = get_log_channel(guild.id, "announce")
    if ch_id:
        ch = guild.get_channel(ch_id) or await guild.fetch_channel(ch_id)
        if ch:
            await ch.send(embed=embed)
    
    # DM
    success = fail = 0
    for uid in all_users:
        try:
            u = await bot.fetch_user(uid)
            await u.send(embed=embed)
            success += 1
            await asyncio.sleep(0.3)
        except:
            fail += 1
    
    await interaction.followup.send(
        f"✅ 공지 완료\n• 대상: {len(all_users)}명\n• 성공: {success}명\n• 실패: {fail}명",
        ephemeral=True
    )

@bot.tree.command(name="인증로그보기", description="인증 기록을 확인합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(최근="최근 N개 (기본 20)")
async def view_verification_log(interaction: discord.Interaction, 최근: int = 20):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    await interaction.response.defer(ephemeral=True) 

    try:
        resp = requests.get(
            f"{API_BASE}/api/logs/verify",
            params={
                "guild_id": interaction.guild.id,
                "user_id": interaction.user.id,  # or 특정 유저만, 전체면 이 줄 빼기
                "limit": 최근,
            },
            timeout=5,
        )
        if resp.status_code != 200:
            await interaction.followup.send(
                f"웹 로그 조회 실패: {resp.status_code} {resp.text}",
                ephemeral=True,
            )
            return 

        data = resp.json()
        if not data:
            await interaction.followup.send("인증 로그가 없습니다.", ephemeral=True)
            return 

        # 문자열로 포맷
        lines = [
            f"{i+1}. [{item['created_at']}] {item['detail']} (user_id={item['user_id']})"
            for i, item in enumerate(data)
        ]
        msg = "\n".join(lines) 

        embed = discord.Embed(
            title="인증 로그 (웹)",
            description=f"```\n{msg[:1900]}\n```",
            color=discord.Color.blue(),
        )
        embed.set_footer(text=f"최근 {len(data)}개") 

        await interaction.followup.send(embed=embed, ephemeral=True) 

    except Exception as e:
        await interaction.followup.send(f"로그 읽기 실패: {e}", ephemeral=True) 

@bot.tree.command(name="인증통계", description="서버 인증 통계를 보여줍니다.")
async def verify_stats(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message(
            "길드에서만 사용 가능합니다.",
            ephemeral=True,
        )
        return 

    member = interaction.user
    if not (is_owner(member) or is_admin(member)):
        await interaction.response.send_message(
            "관리자 또는 제작자만 사용할 수 있습니다.",
            ephemeral=True,
        )
        return 

    await interaction.response.defer(ephemeral=True) 

    # ----------------- 서버 멤버 가져오기 -----------------
    members: list[discord.Member] = [m for m in guild.members if not m.bot] 

    # ----------------- API 체크 (인증 여부) -----------------
    verified_ids: set[int] = set()
    loop = asyncio.get_running_loop() 

    def _check_one(user_id: int) -> bool:
        # sync 함수는 run_in_executor 안에서만 호출
        return is_already_verified(guild.id, user_id) 

    async def check_verified(m: discord.Member):
        is_verified = await loop.run_in_executor(None, _check_one, m.id)
        if is_verified:
            verified_ids.add(m.id) 

    # 여러 코루틴을 한 번에 실행
    await asyncio.gather(*(check_verified(m) for m in members)) 

    # ----------------- 멤버 객체 기준으로 분류 -----------------
    verified_members = [m for m in members if m.id in verified_ids]
    not_verified_members = [m for m in members if m.id not in verified_ids] 

    total_members = len(members)
    verified_count = len(verified_members)
    not_verified_count = len(not_verified_members) 

    verified_pct = round(verified_count / total_members * 100, 2) if total_members else 0
    not_verified_pct = round(not_verified_count / total_members * 100, 2) if total_members else 0 

    # ----------------- Embed Chunking -----------------
    def chunk_lines(title: str, members_list: list[discord.Member]):
        chunks = []
        chunk_size = 20  # 한 Embed에 최대 20명씩
        for i in range(0, len(members_list), chunk_size):
            chunk = members_list[i:i+chunk_size]
            lines = [f"{m.display_name} ({m.id})" for m in chunk]
            chunks.append(f"**{title}**\n" + "\n".join(lines))
        return chunks
        
@bot.tree.command(name="역할목록", description="서버 역할과 봇 역할을 10개씩 출력합니다.(관리자)")
async def role_all(interaction: discord.Interaction): 

    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용 가능합니다.", ephemeral=True)
        return 

    await interaction.response.defer(ephemeral=True) 

    # ---------- 1️⃣ 서버 전체 역할 ----------
    roles = interaction.guild.roles[::-1]
    roles = [r for r in roles if r.name != "@everyone"] 

    if roles:
        chunks = [roles[i:i+10] for i in range(0, len(roles), 10)] 

        for idx, chunk in enumerate(chunks, start=1):
            embed = discord.Embed(
                title=f"서버 역할 목록 (총 {len(roles)}개) ({idx}/{len(chunks)})",
                color=discord.Color.blue()
            ) 

            desc = ""
            for role in chunk:
                desc += f"{role.mention} | `{role.id}`\n" 

            embed.description = desc
            await interaction.followup.send(embed=embed, ephemeral=True) 

    # ---------- 2️⃣ 봇 역할 ----------
    bot_member = interaction.guild.get_member(bot.user.id)
    bot_roles = bot_member.roles[::-1]
    bot_roles = [r for r in bot_roles if r.name != "@everyone"] 

    if bot_roles:
        chunks = [bot_roles[i:i+10] for i in range(0, len(bot_roles), 10)] 

        for idx, chunk in enumerate(chunks, start=1):
            embed = discord.Embed(
                title=f"봇 역할 목록 (총 {len(bot_roles)}개) ({idx}/{len(chunks)})",
                color=discord.Color.green()
            ) 

            desc = ""
            for role in chunk:
                desc += f"{role.mention} | `{role.id}`\n" 

            embed.description = desc
            await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.followup.send("봇은 역할이 없습니다.", ephemeral=True)
# 설정
@bot.tree.command(name="관리자지정", description="관리자 역할 추가/제거 (개발자 전용)")
@app_commands.describe(
    역할="추가할 관리자 역할",
    모드="add = 추가 / remove = 제거 / reset = 전체초기화"
)
@app_commands.choices(
    모드=[
        app_commands.Choice(name="add", value="add"),
        app_commands.Choice(name="remove", value="remove"),
        app_commands.Choice(name="reset", value="reset"),
    ]
)
async def set_admin_roles(
    interaction: discord.Interaction,
    역할: Optional[discord.Role],
    모드: app_commands.Choice[str],
):
    if not is_owner(interaction.user.id):
        await interaction.response.send_message(
            "개발자만 사용할 수 있습니다.", ephemeral=True
        )
        return 

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "길드에서만 사용할 수 있습니다.", ephemeral=True
        )
        return 

    current_roles = set(get_guild_admin_role_ids(guild.id)) 

    # reset
    if 모드.value == "reset":
        set_guild_admin_role_ids(guild.id, [])
        await interaction.response.send_message(
            "관리자 역할을 전부 초기화했습니다.", ephemeral=True
        )
        return 

    if 역할 is None:
        await interaction.response.send_message(
            "역할을 선택해주세요.", ephemeral=True
        )
        return 

    bot_member = guild.me
    if bot_member.top_role <= 역할:
        await interaction.response.send_message(
            "봇보다 높은 역할은 설정할 수 없습니다.", ephemeral=True
        )
        return 

    if 모드.value == "add":
        current_roles.add(역할.id)
        set_guild_admin_role_ids(guild.id, list(current_roles))
        await interaction.response.send_message(
            f"{역할.mention} 을(를) 관리자 역할로 추가했습니다.",
            ephemeral=True
        ) 

    elif 모드.value == "remove":
        if 역할.id in current_roles:
            current_roles.remove(역할.id)
            set_guild_admin_role_ids(guild.id, list(current_roles))
            await interaction.response.send_message(
                f"{역할.mention} 을(를) 관리자 역할에서 제거했습니다.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "해당 역할은 관리자 목록에 없습니다.",
                ephemeral=True
    )
# 로그
@bot.tree.command(name="명령어로그", description="명령어 사용 기록을 확인합니다. (관리자)")
@app_commands.describe(페이지크기="한 페이지에 표시할 개수 (기본 10)")
async def command_logs(
    interaction: discord.Interaction,
    페이지크기: int = 10,
):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    # 최신 순으로 200개 정도까지만
    cursor.execute(
        """
        SELECT id, user_name, user_id, command_name, command_full, created_at
        FROM command_logs
        WHERE guild_id=?
        ORDER BY id DESC
        LIMIT 200
        """,
        (guild.id,),
    )
    rows = cursor.fetchall()
    if not rows:
        await interaction.response.send_message("로그가 없습니다.", ephemeral=True)
        return

    # 문자열로 가공
    lines = []
    for log_id, user_name, user_id, cmd_name, full, created_at in rows:
        lines.append(
            f"{log_id}. [{created_at}] /{cmd_name} - {user_name} ({user_id})\n"
            f"    ⤷ {full}"
        )

    # 페이지 나누기
    pages: list[str] = []
    for i in range(0, len(lines), 페이지크기):
        chunk = lines[i:i+페이지크기]
        pages.append("\n".join(chunk))

    view = CommandLogView(pages)
    first_embed = discord.Embed(
        title="📜 명령어 로그",
        description=pages[0],
        color=discord.Color.blurple(),
    )
    first_embed.set_footer(text=f"페이지 1/{len(pages)}")

    await interaction.response.send_message(
        embed=first_embed,
        view=view,
        ephemeral=True,
    )

# 그룹
@bot.tree.command(name="명단", description="Roblox 그룹 역할 리스트를 보여줍니다.")
async def list_roles(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    if not RANK_API_URL_ROOT or not RANK_API_KEY:
        await interaction.response.send_message(
            "랭킹 서버 설정이 되어 있지 않습니다.", ephemeral=True
        )
        return 

    await interaction.response.defer(ephemeral=True) 

    try:
        resp = requests.get(
            f"{RANK_API_URL_ROOT}/roles",
            headers=_rank_api_headers(),
            timeout=15,
        )
        if resp.status_code != 200:
            await interaction.followup.send(
                f"역할 목록 불러오기 실패 (HTTP {resp.status_code}): {resp.text}",
                ephemeral=True,
            )
            return 

        roles = resp.json()  # [{ name, rank, id }, ...]
        total = len(roles) 

        if not roles:
            await interaction.followup.send("역할이 없습니다.", ephemeral=True)
            return 

        # 한 embed당 최대 10개 정도씩
        PER_EMBED = 10
        embeds: list[discord.Embed] = [] 

        for i in range(0, total, PER_EMBED):
            chunk = roles[i:i + PER_EMBED] 

            embed = discord.Embed(
                title="Roblox 그룹 역할 리스트",
                description=f"{i + 1} ~ {min(i + PER_EMBED, total)} / {total}개",
                colour=discord.Colour.blurple(),
            )
            # 전체 개수는 footer에
            embed.set_footer(text=f"총 역할 개수: {total}개") 

            for r in chunk:
                name = r.get("name", "?")
                rank = r.get("rank", "?")
                role_id = r.get("id", "?") 

                # name/field 형식은 취향대로
                embed.add_field(
                    name=name,
                    value=f"rank: `{rank}` / id: `{role_id}`",
                    inline=False,
                ) 

            embeds.append(embed) 

        # 여러 embed 한 번에 전송
        await interaction.followup.send(embeds=embeds, ephemeral=True) 

    except Exception as e:
        await interaction.followup.send(
            f"역할 목록 중 에러 발생: {e}",
            ephemeral=True,
        ) 

@bot.tree.command(name="승진", description="Roblox 그룹 랭크를 특정 역할로 변경합니다. (관리자)")
@app_commands.describe(
    username="Roblox 본닉",
    role_name="그룹 역할 이름",
)
async def promote_cmd(
    interaction: discord.Interaction,
    username: str,
    role_name: str,
):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    if not RANK_API_URL_ROOT or not RANK_API_KEY:
        await interaction.response.send_message(
            "랭킹 서버 설정이 되어 있지 않습니다.", ephemeral=True
        )
        return 

    await interaction.response.defer(ephemeral=True) 

    try:
        payload = {"username": username, "rank": role_name}
        resp = requests.post(
            f"{RANK_API_URL_ROOT}/rank",
            json=payload,
            headers=_rank_api_headers(),
            timeout=15,
        ) 

        if resp.status_code == 200:
            data = resp.json()
            new_role = data.get("newRole", {})  # { name, rank }
            old_role = data.get("oldRole", {})  # 백엔드에서 같이 주면 사용 

            old_rank_str = f"{old_role.get('name','?')} (Rank {old_role.get('rank','?')})"
            new_rank_str = f"{new_role.get('name','?')} (Rank {new_role.get('rank','?')})" 

            await interaction.followup.send(
                f"`{username}` 님을 역할 `{role_name}` 으로 변경했습니다.\n"
                f"실제 반영: {new_rank_str}",
                ephemeral=True,
            ) 

            # 🔵 그룹변경 로그 채널로 embed 전송
            guild = interaction.guild
            if guild:
                log_channel_id = get_log_channel(guild.id, "group_change")
                if log_channel_id:
                    try:
                        log_ch = guild.get_channel(log_channel_id) or await guild.fetch_channel(log_channel_id)
                        if log_ch:
                            embed = make_rank_log_embed(
                                RankLogType.PROMOTE,
                                target_name=username,
                                old_rank=old_rank_str,
                                new_rank=new_rank_str,
                                executor=interaction.user,
                            )
                            await log_ch.send(embed=embed)
                    except Exception as e:
                        print("[RANK_PROMOTE_LOG_ERROR]", repr(e)) 

        else:
            await interaction.followup.send(
                f"승진 실패 (HTTP {resp.status_code}): {resp.text}",
                ephemeral=True,
            )
    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)


@bot.tree.command(name="강등", description="Roblox 그룹 랭크를 특정 역할로 변경합니다. (관리자)")
@app_commands.describe(
    username="Roblox 본닉",
    role_name="그룹 역할 이름",
)
async def demote_to_role_cmd(
    interaction: discord.Interaction,
    username: str,
    role_name: str,
):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    if not RANK_API_URL_ROOT or not RANK_API_KEY:
        await interaction.response.send_message(
            "랭킹 서버 설정이 되어 있지 않습니다.", ephemeral=True
        )
        return 

    await interaction.response.defer(ephemeral=True) 

    try:
        payload = {"username": username, "rank": role_name} 

        resp = requests.post(
            f"{RANK_API_URL_ROOT}/rank",
            json=payload,
            headers=_rank_api_headers(),
            timeout=30,
    )
    
        if resp.status_code == 200:
            data = resp.json()
            new_role = data.get("newRole", {})
            old_role = data.get("oldRole", {}) 

            old_rank_str = f"{old_role.get('name','?')} (Rank {old_role.get('rank','?')})"
            new_rank_str = f"{new_role.get('name','?')} (Rank {new_role.get('rank','?')})" 

            await interaction.followup.send(
                f"`{username}` 님을 역할 `{role_name}` 으로 변경했습니다.\n"
                f"실제 반영: {new_rank_str}",
                ephemeral=True,
            ) 

            guild = interaction.guild
            if guild:
                log_channel_id = get_log_channel(guild.id, "group_change")
                if log_channel_id:
                    try:
                        log_ch = guild.get_channel(log_channel_id) or await guild.fetch_channel(log_channel_id)
                        if log_ch:
                            embed = make_rank_log_embed(
                                RankLogType.DEMOTE,
                                target_name=username,
                                old_rank=old_rank_str,
                                new_rank=new_rank_str,
                                executor=interaction.user,
                            )
                            await log_ch.send(embed=embed)
                    except Exception as e:
                        print("[RANK_DEMOTE_LOG_ERROR]", repr(e)) 

        else:
            await interaction.followup.send(
                f"강등 실패 (HTTP {resp.status_code}): {resp.text}",
                ephemeral=True,
            )
    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)


@bot.tree.command(name="일괄승진", description="인증된 모든 유저를 특정 역할로 승진합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(role_name="변경할 그룹 역할 이름 또는 숫자")
async def bulk_promote_to_role(interaction: discord.Interaction, role_name: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    if not RANK_API_URL_ROOT or not RANK_API_KEY:
        await interaction.response.send_message(
            "랭킹 서버 설정이 되어 있지 않습니다.", ephemeral=True
        )
        return 

    await interaction.response.defer(ephemeral=True) 

    # 인증된 유저 목록
    cursor.execute(
        "SELECT roblox_nick FROM users WHERE guild_id=? AND verified=1",
        (interaction.guild.id,),
    )
    verified_users = [row[0] for row in cursor.fetchall() if row[0]] 

    cursor.execute(
        "SELECT roblox_nick FROM forced_verified WHERE guild_id=?",
        (interaction.guild.id,),
    )
    forced_excluded = {row[0] for row in cursor.fetchall() if row[0]} 

    all_users = [u for u in verified_users if u not in forced_excluded] 

    if not all_users:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return 

    total = len(all_users) 

    if total > 1000:
        await interaction.followup.send(
            f"{total}명 처리 예정 (약 {total // 60}분 소요)\n처리 시작합니다...",
            ephemeral=True,
        ) 

    BATCH_SIZE = 100
    all_results: list[dict] = [] 

    for i in range(0, total, BATCH_SIZE):
        batch = all_users[i:i + BATCH_SIZE] 

        try:
            payload = {"usernames": batch, "rank": role_name}
            resp = requests.post(
                f"{RANK_API_URL_ROOT}/bulk-promote-to-role",
                json=payload,
                headers=_rank_api_headers(),
                timeout=120,
            ) 

            if resp.status_code == 200:
                data = resp.json()
                all_results.extend(data.get("results", [])) 

            if (i + BATCH_SIZE) % 1000 == 0:
                await interaction.followup.send(
                    f"진행 중... {min(i + BATCH_SIZE, total)}/{total}명",
                    ephemeral=True,
                ) 

            await asyncio.sleep(1) 

        except Exception as e:
            print(f"Batch {i} error: {e}")
            continue 

    success_cnt = len([r for r in all_results if r.get("success")])
    fail_cnt = len([r for r in all_results if not r.get("success")]) 

    summary = make_bulk_rank_summary_embed(
        RankSummaryType.BULK_PROMOTE,
        role_name=role_name,
        total=total,
        success=success_cnt,
        failed=fail_cnt,
        executor=interaction.user,
    )
    await interaction.followup.send(embed=summary, ephemeral=True) 

    # 선택: 그룹변경 로그 채널에도 요약 남기기
    log_ch_id = get_log_channel(interaction.guild.id, "group_change")
    if log_ch_id:
        ch = interaction.guild.get_channel(log_ch_id) or await interaction.guild.fetch_channel(log_ch_id)
        if ch:
            await ch.send(embed=summary) 

@bot.tree.command(name="일괄강등", description="인증된 모든 유저를 특정 역할로 변경합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(role_name="변경할 그룹 역할 이름 또는 숫자")
async def bulk_demote_to_role(interaction: discord.Interaction, role_name: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    if not RANK_API_URL_ROOT or not RANK_API_KEY:
        await interaction.response.send_message(
            "랭킹 서버 설정이 되어 있지 않습니다.", ephemeral=True
        )
        return 

    await interaction.response.defer(ephemeral=True) 

    cursor.execute(
        "SELECT roblox_nick FROM users WHERE guild_id=? AND verified=1",
        (interaction.guild.id,),
    )
    verified_users = [row[0] for row in cursor.fetchall() if row[0]] 

    cursor.execute(
        "SELECT roblox_nick FROM forced_verified WHERE guild_id=?",
        (interaction.guild.id,),
    )
    forced_excluded = {row[0] for row in cursor.fetchall() if row[0]} 

    all_users = [u for u in verified_users if u not in forced_excluded] 

    if not all_users:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return 

    total = len(all_users) 

    if total > 1000:
        await interaction.followup.send(
            f"{total}명 처리 예정 (약 {total // 60}분 소요)\n처리 시작합니다...",
            ephemeral=True,
        ) 

    BATCH_SIZE = 100
    all_results: list[dict] = [] 

    for i in range(0, total, BATCH_SIZE):
        batch = all_users[i:i + BATCH_SIZE] 

        try:
            payload = {"usernames": batch, "rank": role_name}
            resp = requests.post(
                f"{RANK_API_URL_ROOT}/bulk-demote-to-role",
                json=payload,
                headers=_rank_api_headers(),
                timeout=120,
            ) 

            if resp.status_code == 200:
                data = resp.json()
                all_results.extend(data.get("results", [])) 

            if (i + BATCH_SIZE) % 1000 == 0:
                await interaction.followup.send(
                    f"진행 중... {min(i + BATCH_SIZE, total)}/{total}명",
                    ephemeral=True,
                ) 

            await asyncio.sleep(1) 

        except Exception as e:
            print(f"Batch {i} error: {e}")
            continue 

    success_cnt = len([r for r in all_results if r.get("success")])
    fail_cnt = len([r for r in all_results if not r.get("success")]) 

    summary = make_bulk_rank_summary_embed(
        RankSummaryType.BULK_DEMOTE,
        role_name=role_name,
        total=total,
        success=success_cnt,
        failed=fail_cnt,
        executor=interaction.user,
    )
    await interaction.followup.send(embed=summary, ephemeral=True) 

    # 선택: 그룹변경 로그 채널에도 요약 남기기
    log_ch_id = get_log_channel(interaction.guild.id, "group_change")
    if log_ch_id:
        ch = interaction.guild.get_channel(log_ch_id) or await interaction.guild.fetch_channel(log_ch_id)
        if ch:
            await ch.send(embed=summary)
# 관리
@bot.tree.command(name="동기화", description="슬래시 명령어를 동기화합니다.")
async def sync_commands(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    await interaction.response.defer(ephemeral=True)
    try:
        if interaction.guild:
            synced = await bot.tree.sync(guild=interaction.guild)
            msg = f"{interaction.guild.name}({interaction.guild.id}) 길드에 {len(synced)}개 명령어 동기화 완료"
        else:
            synced = await bot.tree.sync()
            msg = f"전역에 {len(synced)}개 명령어 동기화 완료" 

        await interaction.followup.send(msg, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"동기화 중 오류: {e}", ephemeral=True) 
# 경고
@bot.tree.command(name="처벌추가", description="경고 횟수에 따른 처벌 규칙을 추가합니다. (관리자)")
@app_commands.describe(
    경고횟수="이 횟수에 도달하면 처벌 적용",
    처벌="적용할 처벌 종류",
    기간="타임아웃/뮤트일 때 지속 시간 (초 단위)"
)
@app_commands.choices(
    처벌=[
        app_commands.Choice(name="밴", value="ban"),
        app_commands.Choice(name="타임아웃", value="timeout"),
        app_commands.Choice(name="뮤트", value="mute"),
        app_commands.Choice(name="킥", value="kick"),
    ]
)
async def add_punish_rule(
    interaction: discord.Interaction,
    경고횟수: int,
    처벌: app_commands.Choice[str],
    기간: int | None = None,
):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    punish_type = 처벌.value  # 'ban' / 'timeout' / 'mute' / 'kick'

    if punish_type in ("timeout", "mute") and (기간 is None or 기간 <= 0):
        await interaction.followup.send(
            "타임아웃/뮤트 처벌은 기간(초)을 1 이상으로 입력해야 합니다.",
            ephemeral=True,
        )
        return

    # DB에 저장 (있으면 덮어쓰기)
    cursor.execute(
        """
        INSERT INTO punish_rules(guild_id, warn_count, punish_type, duration)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(guild_id, warn_count) DO UPDATE SET
            punish_type=excluded.punish_type,
            duration=excluded.duration
        """,
        (guild.id, 경고횟수, punish_type, 기간),
    )
    conn.commit()

    desc = f"경고 `{경고횟수}` 회 도달 시 `{punish_type}` 처벌을 적용합니다."
    if punish_type in ("timeout", "mute"):
        desc += f"\n기간: `{기간}` 초"

    embed = discord.Embed(
        title="✅ 처벌 규칙 추가/수정",
        color=discord.Color.green(),
        description=desc,
    )
    embed.add_field(
        name="설정자",
        value=f"{interaction.user.mention} (`{interaction.user.id}`)",
        inline=False,
    )

    await interaction.followup.send(embed=embed, ephemeral=True)

    await send_admin_log(
        guild,
        title="✅ 처벌 규칙 추가/수정",
        description="경고 횟수에 따른 자동 처벌 규칙이 설정되었습니다.",
        color=discord.Color.green(),
        fields=[
            ("경고 횟수", f"`{경고횟수}` 회", True),
            ("처벌", f"`{punish_type}`", True),
            (
                "기간",
                f"`{기간}` 초" if punish_type in ("timeout", "mute") else "해당 없음",
                True,
            ),
            ("설정자", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
        ],
    )

@bot.tree.command(name="경고", description="유저에게 경고를 1회 부여합니다. (관리자)")
@app_commands.describe(
    user="경고를 줄 유저",
    이유="경고 사유"
)
async def warn(
    interaction: discord.Interaction,
    user: discord.Member,
    이유: str | None = None,
):
    # 관리자 체크
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    # 자기 자신 경고 방지 (원하면 제거해도 됨)
    if user.id == interaction.user.id:
        await interaction.followup.send("자기 자신에게는 경고를 줄 수 없습니다.", ephemeral=True)
        return

    # 1) 현재 경고 횟수 조회
    cursor.execute(
        "SELECT warns FROM warnings WHERE guild_id=? AND user_id=?",
        (guild.id, user.id),
    )
    row = cursor.fetchone()
    current_warns = row[0] if row else 0

    # 2) +1 해서 저장
    new_warns = current_warns + 1
    cursor.execute(
        """
        INSERT INTO warnings(guild_id, user_id, warns)
        VALUES(?, ?, ?)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET warns=excluded.warns
        """,
        (guild.id, user.id, new_warns),
    )
    conn.commit()

    # 3) mod_logs에 기록 (재부팅 후에도 남는 제재 내역)
    cursor.execute(
        """
        INSERT INTO mod_logs(guild_id, user_id, action, moderator_id, reason, created_at)
        VALUES(?, ?, ?, ?, ?, datetime('now'))
        """,
        (guild.id, user.id, "warn", interaction.user.id, 이유 or None),
    )
    conn.commit()

    # 4) 유저에게 피드백 임베드
    user_embed = discord.Embed(
        title="⚠️ 경고 부여",
        color=discord.Color.orange(),
        description=f"{user.mention} 님에게 경고 1회가 부여되었습니다.",
    )
    user_embed.add_field(name="현재 경고 횟수", value=f"`{new_warns}` 회", inline=True)
    user_embed.add_field(name="사유", value=이유 or "사유 없음", inline=False)
    user_embed.add_field(
        name="실행자",
        value=f"{interaction.user.mention} (`{interaction.user.id}`)",
        inline=False,
    )

    await interaction.followup.send(embed=user_embed, ephemeral=True)

    # 5) 관리자 로그 채널에 임베드
    await send_admin_log(
        guild,
        title="⚠️ 경고 부여",
        description="관리자가 유저에게 경고를 부여했습니다.",
        color=discord.Color.orange(),
        fields=[
            ("대상 유저", f"{user.mention} (`{user.id}`)", False),
            ("현재 경고 횟수", f"`{new_warns}` 회", True),
            ("사유", 이유 or "사유 없음", False),
            ("실행자", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
        ],
    )

    # 6) (선택) 경고 횟수에 따른 자동 처벌 규칙 적용
    #    punish_rules 테이블을 사용한다면 여기에서 SELECT 후 apply_punishment 호출
    # cursor.execute(
    #     "SELECT punish_type, duration FROM punish_rules WHERE guild_id=? AND warn_count=?",
    #     (guild.id, new_warns),
    # )
    # rule = cursor.fetchone()
    # if rule:
    #     punish_type, duration = rule
    #     await apply_punishment(guild, user, punish_type, duration, interaction.user, 이유)

# @bot.tree.command(
#     name="일괄닉네임변경",
#     description="인증된 유저의 닉네임을 [랭크] 본닉 형식으로 변경합니다. (관리자)"
# )
# @app_commands.guilds(discord.Object(id=GUILD_ID)) 

# async def bulk_nickname_change(interaction: discord.Interaction):
#     if not is_admin(interaction.user):
#         await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
#         return 

#     await interaction.response.defer(ephemeral=True) 

#     try:
#         # 인증된 유저 목록
#         cursor.execute(
#             "SELECT discord_id, roblox_nick FROM users WHERE guild_id=? AND verified=1",
#             (interaction.guild.id,),
#         )
#         users_data = cursor.fetchall() 

#         if not users_data:
#             await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
#             return 

#         # 모든 유저의 현재 랭크 조회
#         usernames = [row[1] for row in users_data] 

#         resp = requests.post(
#             f"{RANK_API_URL_ROOT}/bulk-status",
#             json={"usernames": usernames},
#             headers=_rank_api_headers(),
#             timeout=60,
#         ) 

#         if resp.status_code != 200:
#             await interaction.followup.send(
#                 f"랭크 조회 실패 (HTTP {resp.status_code})", ephemeral=True
#             )
#             return 

#         data = resp.json() 

#         # username -> rank_name 매핑
#         rank_map = {}
#         for r in data.get("results", []):
#             if r.get("success"):
#                 role_info = r.get("role", {}) or {}
#                 rank_map[r["username"]] = role_info.get("name", "?") 

#         updated = 0
#         failed = 0 

#         for discord_id, roblox_nick in users_data:
#             try:
#                 member = interaction.guild.get_member(discord_id)
#                 if not member:
#                     failed += 1
#                     continue 

#                 rank_name = rank_map.get(roblox_nick, "?") or "?" 

#                 # ROKA | 육군 → 육군
#                 if " | " in rank_name:
#                     rank_name = rank_name.split(" | ")[-1] 

#                 new_nick = f"[{rank_name}] {roblox_nick}" 

#                 if len(new_nick) > 32:
#                     new_nick = new_nick[:32] 

#                 await member.edit(nick=new_nick)
#                 updated += 1 

#             except Exception as e:
#                 print(f"닉네임 변경 실패 {roblox_nick}: {e}")
#                 failed += 1 

#         embed = discord.Embed(
#             title="일괄 닉네임 변경 완료",
#             color=discord.Color.blue(),
#         )
#         embed.add_field(name="성공", value=str(updated), inline=True)
#         embed.add_field(name="실패", value=str(failed), inline=True)
#         embed.add_field(name="형식", value="[랭크] 로블 본닉", inline=False)
#         await interaction.followup.send(embed=embed, ephemeral=True) 

#     except Exception as e:
#         await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True) 

@bot.tree.command(name="로그채널지정", description="로그 채널을 설정합니다. (관리자)")
@app_commands.describe(
    인증="인증 로그 채널",
    그룹변경="그룹변경 로그 채널",
    관리자="관리자 로그 채널",
    보안="보안 로그 채널",
    개발자="개발자 로그 채널",
    아이템="아이템 구매 로그 채널",
    공지="공지 로그 채널"
)
async def set_log_channels(
    interaction: discord.Interaction,
    인증: discord.TextChannel | None = None,
    그룹변경: discord.TextChannel | None = None,
    관리자: discord.TextChannel | None = None,
    보안: discord.TextChannel | None = None,
    개발자: discord.TextChannel | None = None,
    아이템: discord.TextChannel | None = None,
    공지: discord.TextChannel | None = None,
):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    changed = []

    if 인증 is not None:
        set_log_channel(guild.id, "verify", 인증.id)
        changed.append(f"인증: {인증.mention}")

    if 그룹변경 is not None:
        set_log_channel(guild.id, "group_change", 그룹변경.id)
        changed.append(f"그룹변경: {그룹변경.mention}")

    if 관리자 is not None:
        set_log_channel(guild.id, "admin", 관리자.id)
        changed.append(f"관리자: {관리자.mention}")

    if 보안 is not None:
        set_log_channel(guild.id, "security", 보안.id)
        changed.append(f"보안: {보안.mention}")

    if 개발자 is not None:
        set_log_channel(guild.id, "dev", 개발자.id)
        changed.append(f"개발자: {개발자.mention}")

    if 아이템 is not None:
        set_log_channel(guild.id, "item", 아이템.id)
        changed.append(f"아이템: {아이템.mention}")

    if 공지 is not None:
        set_log_channel(guild.id, "announce", 공지.id)
        changed.append(f"공지: {공지.mention}")

    if not changed:
        await interaction.response.send_message(
            "변경된 채널이 없습니다. 최소 한 개 이상 지정해 주세요.",
            ephemeral=True
        )
        return

    msg = "다음 로그 채널이 설정되었습니다:\n" + "\n".join(changed)

    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="블랙리스트", description="블랙리스트 그룹을 관리합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(
    group_id="Roblox 그룹 ID",
    action="add (추가) 또는 remove (제거)",
)
async def manage_blacklist(interaction: discord.Interaction, group_id: int, action: str = "add"):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    if action.lower() == "add":
        try:
            cursor.execute(
                "INSERT INTO blacklist(guild_id, group_id) VALUES(?, ?)",
                (interaction.guild.id, group_id),
            )
            conn.commit()
            await interaction.response.send_message(
                f" 그룹 ID `{group_id}` 을(를) 블랙리스트에 추가했습니다.", ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(f"추가 실패: {e}", ephemeral=True)
    else:
        cursor.execute(
            "DELETE FROM blacklist WHERE guild_id=? AND group_id=?",
            (interaction.guild.id, group_id),
        )
        conn.commit()
        await interaction.response.send_message(
            f" 그룹 ID `{group_id}` 을(를) 블랙리스트에서 제거했습니다.", ephemeral=True
        ) 

@bot.tree.command(name="블랙리스트목록", description="블랙리스트 그룹 목록을 봅니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
async def view_blacklist(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return 

    cursor.execute("SELECT group_id FROM blacklist WHERE guild_id=?", (interaction.guild.id,))
    rows = cursor.fetchall() 

    embed = discord.Embed(title="블랙리스트 그룹", color=discord.Color.red()) 

    if not rows:
        embed.description = "블랙리스트에 그룹이 없습니다."
    else:
        group_ids = [str(row[0]) for row in rows]
        embed.description = "\n".join(group_ids) 

    await interaction.response.send_message(embed=embed, ephemeral=True) 

# @bot.tree.command(name="역할전체변경", description="모든 유저의 역할을 한 역할로 통일합니다. (위험)")
# async def set_all_role(interaction: discord.Interaction):
#     guild = interaction.guild
#     if guild.id != GUILD_ID:
#         await interaction.response.send_message("이 명령어는 지정된 서버에서만 사용할 수 있습니다.", ephemeral=True)
#         return 

#     target_role = guild.get_role(TARGET_ROLE_ID)
#     if not target_role:
#         await interaction.response.send_message("대상 역할을 찾을 수 없습니다.", ephemeral=True)
#         return 

#     await interaction.response.send_message("모든 멤버 역할 변경 시작...", ephemeral=True) 

#     success = 0
#     failed = 0
#     skipped = 0 

#     for member in guild.members:
#         # 봇은 스킵
#         if member.bot:
#             continue 

#         # 봇 위상보다 높은/같은 멤버는 어차피 못 건드리니 스킵[web:80]
#         if guild.me.top_role <= member.top_role:
#             skipped += 1
#             continue 

#         try:
#             # @everyone 역할은 항상 첫 번째, 제거하면 안 됨[web:58]
#             everyone = member.roles[0]
#             new_roles = [everyone, target_role] 

#             await member.edit(roles=new_roles)
#             success += 1 

#             # 레이트리밋 완화용 (인원 많으면 조절)
#             await asyncio.sleep(0.3) 

#         except discord.Forbidden:
#             # 권한 부족(역할 위상 등) → 그 멤버만 예외
#             print(f"{member} 권한 부족으로 스킵")
#             failed += 1
#         except Exception as e:
#             print(f"{member} 역할 변경 실패: {e}")
#             failed += 1 

#     await interaction.followup.send(
#         f"역할 변경 완료\n"
#         f"성공: {success}명\n"
#         f"실패: {failed}명\n"
#         f"위상/조건으로 스킵: {skipped}명",
#         ephemeral=True
#     )

# -- 경제 명령어 --
# =========================
# EXP 시스템
# =========================

xp_cooldown = {}

@bot.event
async def on_message(message):

    if message.author.bot:
        return

    user_id = message.author.id
    now = time.time()

    if user_id in xp_cooldown:
        if now - xp_cooldown[user_id] < 30:
            return

    xp_cooldown[user_id] = now

    user = get_user(user_id)

    exp = user[3]
    level = user[4]

    gain = random.randint(10,20)
    exp += gain

    need = 50 + (level * 25)

    if exp >= need:

        level += 1
        exp -= need

        reward = level * 50

        cur.execute(
            "UPDATE economy SET money = money + ? WHERE user_id=?",
            (reward, user_id)
        )

    cur.execute(
        "UPDATE economy SET exp=?, level=? WHERE user_id=?",
        (exp, level, user_id)
    )

    conn.commit()


# =========================
# 슬래시 명령어
# =========================

@bot.tree.command(name="돈", description="24시간마다 돈 받기")
async def daily(interaction: discord.Interaction):

    user = get_user(interaction.user.id)
    now = int(time.time())

    if now - user[2] < 86400:

        remain = 86400 - (now - user[2])
        h = remain // 3600
        m = (remain % 3600) // 60

        await interaction.response.send_message(
            f"⏳ {h}시간 {m}분 후 다시 받을 수 있습니다."
        )
        return

    reward = random.randint(100,300)

    cur.execute(
        "UPDATE economy SET money = money + ?, last_daily=? WHERE user_id=?",
        (reward, now, interaction.user.id)
    )

    conn.commit()

    await interaction.response.send_message(
        f"💰 {reward}원을 받았습니다!"
    )


# =========================
# 도박
# =========================

@bot.tree.command(name="도박", description="돈을 걸고 도박합니다")
@app_commands.describe(amount="도박 금액")
async def gamble(interaction: discord.Interaction, amount: int):

    user = get_user(interaction.user.id)

    if amount <= 0:
        await interaction.response.send_message("금액 오류")
        return

    if user[1] < amount:
        await interaction.response.send_message("돈이 부족합니다")
        return

    r = random.random()

    if r <= 0.50:
        # 패배
        cur.execute(
            "UPDATE economy SET money = money - ? WHERE user_id=?",
            (amount, interaction.user.id)
        )
        conn.commit()

        await interaction.response.send_message(
            f"💀 도박 실패\n잃은 돈 : {amount}"
        )
        return

    elif r <= 0.85:
        multi = 2
    elif r <= 0.95:
        multi = 3
    elif r <= 0.99:
        multi = 4
    else:
        multi = 5

    win = amount * multi

    cur.execute(
        "UPDATE economy SET money = money + ? WHERE user_id=?",
        (win, interaction.user.id)
    )

    conn.commit()

    await interaction.response.send_message(
        f"🎰 도박 성공!\n배율 : x{multi}\n획득 : {win}"
    )
    
# =========================
# 아이템샵
# =========================
@bot.tree.command(name="아이템샵", description="서버 아이템을 보여줍니다.")
async def shop(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    cursor.execute(
        """
        SELECT name, price, type, role_id, level, exp
        FROM shop_items
        WHERE guild_id=?
        ORDER BY price ASC
        """,
        (guild.id,),
    )
    items = cursor.fetchall()
    if not items:
        await interaction.followup.send("상점에 등록된 아이템이 없습니다.", ephemeral=True)
        return

    view = ShopView(guild, items)
    embed = view.make_page_embed()
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)

# =========================
# 구매
# =========================
@bot.tree.command(name="구매", description="상점 아이템을 구매합니다.")
@app_commands.describe(이름="구매할 아이템 이름")
async def buy(interaction: discord.Interaction, 이름: str):
    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    member = interaction.user
    if guild is None:
        await interaction.followup.send("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    # 아이템 조회
    cursor.execute(
        """
        SELECT price, type, role_id, level, exp
        FROM shop_items
        WHERE guild_id=? AND name=?
        """,
        (guild.id, 이름),
    )
    row = cursor.fetchone()
    if not row:
        await interaction.followup.send("해당 이름의 아이템이 없습니다.", ephemeral=True)
        return

    price, item_type, role_id, level_val, exp_val = row

    # 유저 경제 정보
    user = get_user(member.id)  # (user_id, money, last_daily, exp, level)
    _, money, _, cur_exp, cur_level = user

    if money < price:
        await interaction.followup.send("잔액이 부족합니다.", ephemeral=True)
        return

    # 돈 차감
    new_money = money - price
    cur.execute(
        "UPDATE economy SET money=? WHERE user_id=?",
        (new_money, member.id),
    )

    detail = ""

    # 타입별 지급
    if item_type == "role":
        if role_id:
            role = guild.get_role(role_id)
            if role:
                await guild.get_member(member.id).add_roles(role, reason="아이템 구매")
                detail = f"역할 {role.mention} 지급 완료."
            else:
                detail = "역할을 찾을 수 없습니다."
        else:
            detail = "이 아이템에는 역할 ID가 설정되어 있지 않습니다."

    elif item_type == "level":
        if level_val is not None:
            add_level = int(level_val)
            new_level = cur_level + add_level
            cur.execute(
                "UPDATE economy SET level=? WHERE user_id=?",
                (new_level, member.id),
            )
            detail = f"레벨 {add_level} 상승! (현재 레벨: {new_level})"
        else:
            detail = "이 아이템에는 레벨 값이 설정되어 있지 않습니다."

    elif item_type == "exp":
        if exp_val is not None:
            add_exp = int(exp_val)
            new_exp = cur_exp + add_exp
            cur.execute(
                "UPDATE economy SET exp=? WHERE user_id=?",
                (new_exp, member.id),
            )
            detail = f"경험치 {add_exp} 획득! (현재 경험치: {new_exp})"
        else:
            detail = "이 아이템에는 경험치 값이 설정되어 있지 않습니다."

    else:
        await interaction.followup.send("알 수 없는 아이템 타입입니다.", ephemeral=True)
        return

    conn.commit()

    # 유저에게 응답
    user_embed = discord.Embed(
        title="✅ 아이템 구매 완료",
        color=discord.Color.green(),
        description=(
            f"아이템: `{이름}`\n"
            f"가격: `{price}`\n"
            f"잔액: `{new_money}`\n\n"
            f"{detail}"
        ),
    )
    await interaction.followup.send(embed=user_embed, ephemeral=True)

    # 아이템 로그 채널에 파란색 embed
    log_ch_id = get_log_channel(guild.id, "item")
    if log_ch_id:
        log_ch = guild.get_channel(log_ch_id) or await guild.fetch_channel(log_ch_id)
        if log_ch:
            log_embed = discord.Embed(
                title="🔵 아이템 구매",
                color=discord.Color.blue(),  # 파란색
            )
            log_embed.add_field(
                name="구매자",
                value=f"{member.mention} (`{member.id}`)",
                inline=False,
            )
            log_embed.add_field(name="아이템 이름", value=f"`{이름}`", inline=True)
            log_embed.add_field(name="가격", value=f"`{price}`", inline=True)
            log_embed.add_field(name="타입", value=f"`{item_type}`", inline=True)
            log_embed.add_field(name="구매 후 잔액", value=f"`{new_money}`", inline=False)

            if item_type == "role" and role_id:
                role = guild.get_role(role_id)
                if role:
                    log_embed.add_field(
                        name="지급된 역할",
                        value=f"{role.mention} (`{role.id}`)",
                        inline=False,
                    )
            if item_type == "level" and level_val is not None:
                log_embed.add_field(
                    name="레벨 증가",
                    value=f"+{int(level_val)} (이전: {cur_level})",
                    inline=False,
                )
            if item_type == "exp" and exp_val is not None:
                log_embed.add_field(
                    name="경험치 증가",
                    value=f"+{int(exp_val)} (이전: {cur_exp})",
                    inline=False,
                )

            await log_ch.send(embed=log_embed)

# =========================
# 아이템 추가
# =========================

@bot.tree.command(name="아이템추가", description="상점 아이템을 추가합니다. (관리자)")
@app_commands.describe(
    이름="아이템 이름",
    가격="아이템 가격 (정수)",
    종류="아이템 종류 (역할, 레벨, 경험치)",
    역할="역할 아이템일 경우 지급할 역할",
    레벨="레벨 아이템일 경우 부여할 레벨 값",
    경험치="경험치 아이템일 경우 부여할 경험치 양",
)
@app_commands.choices(
    종류=[
        app_commands.Choice(name="역할", value="role"),
        app_commands.Choice(name="레벨", value="level"),
        app_commands.Choice(name="경험치", value="exp"),
    ]
)
async def add_item(
    interaction: discord.Interaction,
    이름: str,
    가격: int,
    종류: app_commands.Choice[str],
    역할: discord.Role | None = None,
    레벨: int | None = None,
    경험치: int | None = None,
):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    item_type = 종류.value  # 'role' / 'level' / 'exp'

    if item_type == "role":
        if 역할 is None:
            await interaction.response.send_message("역할 아이템은 역할 옵션이 필수입니다.", ephemeral=True)
            return
        role_id = 역할.id
        level_val = None
        exp_val = None

    elif item_type == "level":
        if 레벨 is None:
            await interaction.response.send_message("레벨 아이템은 레벨 값을 넣어야 합니다.", ephemeral=True)
            return
        role_id = None
        level_val = 레벨
        exp_val = None

    else:  # "exp"
        if 경험치 is None:
            await interaction.response.send_message("경험치 아이템은 경험치 값을 넣어야 합니다.", ephemeral=True)
            return
        role_id = None
        level_val = None
        exp_val = 경험치

    # DB 저장
    cursor.execute(
        """
        INSERT INTO shop_items(guild_id, name, price, type, role_id, level, exp)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (guild.id, 이름, 가격, item_type, role_id, level_val, exp_val),
    )
    conn.commit()

    # 유저에게 응답
    await interaction.response.send_message(f"✅ `{이름}` 아이템을 추가했습니다.", ephemeral=True)

    # 아이템 로그 채널에 초록색 embed
    log_ch_id = get_log_channel(guild.id, "item")
    if log_ch_id:
        log_ch = guild.get_channel(log_ch_id) or await guild.fetch_channel(log_ch_id)
        if log_ch:
            embed = discord.Embed(
                title="🟢 아이템 추가",
                color=discord.Color.green(),  # 초록색
            )
            embed.add_field(name="아이템 이름", value=f"`{이름}`", inline=True)
            embed.add_field(name="가격", value=f"`{가격}`", inline=True)
            embed.add_field(name="타입", value=f"`{item_type}`", inline=True)

            if item_type == "role" and role_id:
                role = guild.get_role(role_id)
                if role:
                    embed.add_field(name="역할", value=f"{role.mention} (`{role.id}`)", inline=False)
            if item_type == "level" and level_val is not None:
                embed.add_field(name="레벨", value=f"+{level_val}", inline=False)
            if item_type == "exp" and exp_val is not None:
                embed.add_field(name="경험치", value=f"+{exp_val}", inline=False)

            embed.add_field(
                name="추가한 유저",
                value=f"{interaction.user.mention} (`{interaction.user.id}`)",
                inline=False,
            )
            await log_ch.send(embed=embed)

# =========================
# 아이템 제거
# =========================
@bot.tree.command(name="아이템삭제", description="상점에서 아이템을 삭제합니다. (관리자)")
@app_commands.describe(이름="삭제할 아이템 이름")
async def delete_item(interaction: discord.Interaction, 이름: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    # 삭제 전 정보 조회 (로그용)
    cursor.execute(
        """
        SELECT price, type, role_id, level, exp
        FROM shop_items
        WHERE guild_id=? AND name=?
        """,
        (guild.id, 이름),
    )
    row = cursor.fetchone()
    if not row:
        await interaction.response.send_message("해당 이름의 아이템이 없습니다.", ephemeral=True)
        return

    price, item_type, role_id, level_val, exp_val = row

    # 삭제
    cursor.execute(
        "DELETE FROM shop_items WHERE guild_id=? AND name=?",
        (guild.id, 이름),
    )
    conn.commit()

    await interaction.response.send_message(f"🗑 `{이름}` 아이템을 삭제했습니다.", ephemeral=True)

    # 아이템 로그 채널에 빨간색 embed
    log_ch_id = get_log_channel(guild.id, "item")
    if log_ch_id:
        log_ch = guild.get_channel(log_ch_id) or await guild.fetch_channel(log_ch_id)
        if log_ch:
            embed = discord.Embed(
                title="🔴 아이템 삭제",
                color=discord.Color.red(),  # 빨간색
            )
            embed.add_field(name="아이템 이름", value=f"`{이름}`", inline=True)
            embed.add_field(name="가격", value=f"`{price}`", inline=True)
            embed.add_field(name="타입", value=f"`{item_type}`", inline=True)

            if item_type == "role" and role_id:
                role = guild.get_role(role_id)
                if role:
                    embed.add_field(name="역할", value=f"{role.mention} (`{role.id}`)", inline=False)
            if item_type == "level" and level_val is not None:
                embed.add_field(name="레벨", value=f"+{level_val}", inline=False)
            if item_type == "exp" and exp_val is not None:
                embed.add_field(name="경험치", value=f"+{exp_val}", inline=False)

            embed.add_field(
                name="삭제한 유저",
                value=f"{interaction.user.mention} (`{interaction.user.id}`)",
                inline=False,
            )
            await log_ch.send(embed=embed)

# =========================
# 유저 정보
# =========================
@bot.tree.command(name="유저", description="유저 정보 확인")
async def userinfo(interaction: discord.Interaction, member: discord.Member | None = None):

    if member is None:
        member = interaction.user

    guild = interaction.guild

    # 경제 정보
    user = get_user(member.id)
    money = user[1]
    exp = user[3]
    level = user[4]
    need = 50 + (level * 25)

    embed = discord.Embed(title=f"{member.name} 정보")
    embed.set_thumbnail(url=member.display_avatar.url)

    embed.add_field(name="💰 돈", value=money, inline=True)
    embed.add_field(name="⭐ 레벨", value=level, inline=True)
    embed.add_field(name="📊 EXP", value=f"{exp}/{need}", inline=True)

    # ⚠️ 경고 횟수
    cursor.execute(
        "SELECT warns FROM warnings WHERE guild_id=? AND user_id=?",
        (guild.id, member.id),
    )
    row = cursor.fetchone()
    warn_count = row[0] if row else 0
    embed.add_field(name="⚠️ 경고 횟수", value=f"{warn_count}회", inline=True)

    # 📜 최근 제재 내역 (mod_logs 기준, 최대 5개)
    cursor.execute(
        """
        SELECT action, moderator_id, reason, created_at
        FROM mod_logs
        WHERE guild_id=? AND user_id=?
        ORDER BY created_at DESC
        LIMIT 5
        """,
        (guild.id, member.id),
    )
    logs = cursor.fetchall()
    if logs:
        lines = []
        for action, moderator_id, reason, created_at in logs:
            mod = guild.get_member(moderator_id)
            mod_name = mod.mention if mod else f"`{moderator_id}`"
            lines.append(
                f"[{created_at}] `{action}`\n"
                f"- 처리자: {mod_name}\n"
                f"- 사유: {reason or '사유 없음'}"
            )
        history_text = "\n\n".join(lines)
    else:
        history_text = "최근 제재 내역 없음"
    embed.add_field(
        name="📜 최근 제재 내역 (최대 5개)",
        value=history_text[:1024],
        inline=False,
    )

    # 🧩 로블 인증 정보 (users 테이블 기준)
    cursor.execute(
        """
        SELECT roblox_nick, roblox_user_id, verified
        FROM users
        WHERE discord_id=? AND guild_id=?
        """,
        (member.id, guild.id),
    )
    urow = cursor.fetchone()
    if urow:
        roblox_nick, roblox_user_id, verified = urow
        embed.add_field(
            name="🧩 로블 인증",
            value=(
                f"상태: `{'인증됨' if verified else '미인증'}`\n"
                f"닉네임: `{roblox_nick}`\n"
                f"UserId: `{roblox_user_id}`"
            ),
            inline=False,
        )
    else:
        embed.add_field(
            name="🧩 로블 인증",
            value="등록된 인증 정보가 없습니다.",
            inline=False,
        )

    # 🔒 강제인증 여부
    cursor.execute(
        "SELECT 1 FROM forced_verified WHERE discord_id=? AND guild_id=?",
        (member.id, guild.id),
    )
    forced = cursor.fetchone() is not None
    embed.add_field(
        name="🔒 강제인증",
        value="예" if forced else "아니오",
        inline=True,
    )

    await interaction.response.send_message(embed=embed, ephemeral=True)

# =========================
# 송금
# =========================
@bot.tree.command(name="송금", description="다른 유저에게 돈을 송금합니다.")
@app_commands.describe(
    대상="돈을 보낼 유저",
    금액="보낼 금액 (정수)"
)
async def pay(
    interaction: discord.Interaction,
    대상: discord.Member,
    금액: int,
):
    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    보낸이 = interaction.user

    if guild is None:
        await interaction.followup.send("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    if 대상.id == 보낸이.id:
        await interaction.followup.send("자기 자신에게는 송금할 수 없습니다.", ephemeral=True)
        return

    if 금액 <= 0:
        await interaction.followup.send("송금 금액은 1 이상이어야 합니다.", ephemeral=True)
        return

    # 보낸이 경제 정보
    sender_data = get_user(보낸이.id)
    sender_money = sender_data[1]

    if sender_money < 금액:
        await interaction.followup.send("잔액이 부족합니다.", ephemeral=True)
        return

    # 받는이 경제 정보 (get_user가 없으면 새로 생성)
    receiver_data = get_user(대상.id)
    receiver_money = receiver_data[1]

    # DB 업데이트
    new_sender_money = sender_money - 금액
    new_receiver_money = receiver_money + 금액

    cur.execute(
        "UPDATE economy SET money=? WHERE user_id=?",
        (new_sender_money, 보낸이.id),
    )
    cur.execute(
        "UPDATE economy SET money=? WHERE user_id=?",
        (new_receiver_money, 대상.id),
    )
    conn.commit()

    # 송금 로그 저장 (선택)
    cursor.execute(
        """
        INSERT INTO transfer_logs(guild_id, from_id, to_id, amount, created_at)
        VALUES(?, ?, ?, ?, datetime('now'))
        """,
        (guild.id, 보낸이.id, 대상.id, 금액),
    )
    conn.commit()

    # 유저에게 응답
    embed = discord.Embed(
        title="💸 송금 완료",
        color=discord.Color.green(),
        description=(
            f"{보낸이.mention} → {대상.mention}\n"
            f"`{금액}`원을 송금했습니다."
        ),
    )
    embed.add_field(name="내 잔액", value=f"`{new_sender_money}`", inline=True)
    embed.add_field(name="상대 잔액", value=f"`{new_receiver_money}`", inline=True)

    await interaction.followup.send(embed=embed, ephemeral=True)

    # 관리자 로그
    await send_admin_log(
        guild,
        title="💸 송금",
        description="유저 간 송금이 발생했습니다.",
        color=discord.Color.blurple(),
        fields=[
            ("보낸이", f"{보낸이.mention} (`{보낸이.id}`)", False),
            ("받는이", f"{대상.mention} (`{대상.id}`)", False),
            ("금액", f"`{금액}`", True),
            ("보낸이 잔액", f"`{new_sender_money}`", True),
            ("받는이 잔액", f"`{new_receiver_money}`", True),
        ],
    )

# =========================
# 랭킹
# =========================
@bot.tree.command(name="랭킹", description="서버 경제 랭킹을 보여줍니다.")
@app_commands.describe(
    종류="랭킹 기준 (money, level, exp)"
)
@app_commands.choices(
    종류=[
        app_commands.Choice(name="돈", value="money"),
        app_commands.Choice(name="레벨", value="level"),
        app_commands.Choice(name="경험치", value="exp"),
    ]
)
async def ranking(
    interaction: discord.Interaction,
    종류: app_commands.Choice[str],
):
    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    column = 종류.value  # 'money' / 'level' / 'exp'

    # economy: user_id PRIMARY KEY, money, exp, level ...
    cur.execute(
        f"""
        SELECT user_id, {column}
        FROM economy
        ORDER BY {column} DESC
        LIMIT 10
        """
    )
    rows = cur.fetchall()
    if not rows:
        await interaction.followup.send("랭킹 데이터가 없습니다.", ephemeral=True)
        return

    lines = []
    for idx, (user_id, value) in enumerate(rows, start=1):
        member = guild.get_member(user_id)
        name = member.display_name if member else f"`{user_id}`"
        lines.append(f"{idx}. {name} - `{value}`")

    title = {
        "money": "💰 돈 랭킹",
        "level": "⭐ 레벨 랭킹",
        "exp": "📊 경험치 랭킹",
    }[column]

    embed = discord.Embed(
        title=title,
        description="\n".join(lines),
        color=discord.Color.gold(),
    )
    await interaction.followup.send(embed=embed, ephemeral=True)

# ------------------------
# 전역 변수
# ------------------------
BOT_STATUS = "정상"
STATUS_EMOJIS = {
    "정상": "🟢",
    "서비스 준비중": "🟡",
    "중지": "🔴"
}

SUPPORT_SERVER = "https://discord.gg/e3Mb5mdSAe"
bot_start_time = time.time()
status_channel_id = 1480268362889166989  # 15초 갱신용 채널
status_msg = None  # 마지막 상태 메시지 저장용

# ------------------------
# 상태 embed 생성 함수
# ------------------------
def generate_status_embed(title="🤖 봇 상태"):
    uptime = int(time.time() - bot_start_time)
    hours = uptime // 3600
    minutes = (uptime % 3600) // 60
    seconds = uptime % 60
    status_emoji = STATUS_EMOJIS.get(BOT_STATUS, "⚪")
    cpu_usage = psutil.cpu_percent(interval=None)
    memory_usage = psutil.virtual_memory().percent
    total_users = sum(g.member_count for g in bot.guilds)
    command_count = len(bot.commands)
    ping = round(bot.latency * 1000)

    embed = discord.Embed(title=title, color=discord.Color.green())
    embed.add_field(name="⏱ 업타임", value=f"{hours}시간 {minutes}분 {seconds}초", inline=False)
    embed.add_field(name="📡 봇 상태", value=f"{status_emoji} {BOT_STATUS}", inline=False)
    embed.add_field(name="🌍 서버 수", value=f"{len(bot.guilds)}개", inline=False)
    embed.add_field(name="👥 총 유저 수", value=f"{total_users}", inline=True)
    embed.add_field(name="📦 명령어 수", value=f"{command_count}", inline=True)
    embed.add_field(name="⚡ 핑", value=f"{ping}ms", inline=True)
    embed.add_field(name="🧠 메모리 사용량", value=f"{memory_usage}%", inline=True)
    embed.add_field(name="📊 CPU 사용량", value=f"{cpu_usage}%", inline=True)
    embed.add_field(name="🔗 서포트 서버", value=SUPPORT_SERVER, inline=False)
    return embed

# ------------------------
# 슬래시 명령어: 봇 상태 지정
# ------------------------
@bot.tree.command(
    name="봇상태지정",
    description="봇 상태를 변경합니다 (관리자)"
)
@app_commands.describe(status="봇 상태 선택")
@app_commands.choices(status=[
    app_commands.Choice(name="정상", value="정상"),
    app_commands.Choice(name="서비스 준비중", value="서비스 준비중"),
    app_commands.Choice(name="중지", value="중지")
])
async def set_bot_status(interaction: Interaction, status: app_commands.Choice[str]):
    global BOT_STATUS
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return
    BOT_STATUS = status.value
    await interaction.response.send_message(
        f"봇 상태가 **{STATUS_EMOJIS[BOT_STATUS]} {BOT_STATUS}** 로 변경되었습니다.",
        ephemeral=True
    )

# ------------------------
# 슬래시 명령어: 봇 정보
# ------------------------
@bot.tree.command(
    name="봇정보",
    description="봇 정보를 확인합니다"
)
async def bot_info(interaction: Interaction):
    embed = generate_status_embed(title="🤖 봇 정보")
    embed.color = discord.Color.blue()
    embed.set_footer(text=f"요청자: {interaction.user}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ------------------------
# 슬래시 명령어: 상태 채널 지정
# ------------------------
@bot.tree.command(
    name="상태채널지정",
    description="봇 상태 자동 갱신 채널을 지정합니다 (관리자)"
)
@app_commands.describe(channel="봇 상태가 갱신될 채널")
async def set_status_channel(interaction: Interaction, channel: discord.TextChannel):
    global status_channel_id
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return
    status_channel_id = channel.id
    await interaction.response.send_message(f"{channel.mention} 채널로 상태 갱신이 설정되었습니다.", ephemeral=True)
# ------------------------
# 15초 루프: 상태 자동 갱신
# ------------------------
@tasks.loop(seconds=5)
async def update_status():
    global status_msg
    if not status_channel_id:
        return

    channel = bot.get_channel(status_channel_id)
    if not channel:
        return

    # 상태 embed 생성
    embed = generate_status_embed(title="🤖 봇 상태 (자동 갱신)")

    # 첫 실행: 메시지 없으면 새로 보내기
    if status_msg is None:
        try:
            status_msg = await channel.send(embed=embed)
        except discord.Forbidden:
            # 권한 없으면 무시
            return
        except discord.HTTPException:
            return
        return  # 첫 메시지 전송 후 루프 종료

    # 이미 메시지가 있으면 수정
    try:
        await status_msg.edit(embed=embed)
    except discord.NotFound:
        # 메시지가 삭제되었으면 새로 전송
        try:
            status_msg = await channel.send(embed=embed)
        except:
            pass
    except discord.HTTPException:
        # 기타 예외 무시
        pass
    
# -- Fast API --
@app.get("/api/bot-stats")
def bot_stats():
    """Bot 통계"""
    try:
        guilds_count = len(bot.guilds)
        return {
            "guilds": guilds_count,
            "verified_users": guilds_count * 10,
            "warn_records": 0,
        }
    except Exception as e:
        print(f"Bot stats error: {e}")
        return {"guilds": 0, "verified_users": 0, "warn_records": 0}

# -- 이벤트 --
ALLOWED_GUILD_IDS = [
    1461636782176075830,
    1479791881046065286
    ]
SECURITY_LOG_CHANNEL_ID = 1468191965052141629
DEVELOPER_ID = 1276176866440642561 

KST = timezone(timedelta(hours=9)) 

@tasks.loop(hours=6)
async def sync_all_nicknames_task():
    """6시간마다 전체 유저의 Roblox 정보를 동기화하고 닉네임 업데이트"""
    try:
        cursor.execute("SELECT guild_id FROM rank_log_settings WHERE enabled=1")
        settings = cursor.fetchall() 

        for (guild_id,) in settings:
            guild = bot.get_guild(guild_id)
            if not guild:
                continue 

            # 인증된 모든 유저 조회
            cursor.execute(
                "SELECT discord_id, roblox_nick FROM users WHERE guild_id=? AND verified=1",
                (guild_id,),
            )
            users = cursor.fetchall() 

            if not users:
                continue 

            usernames = [u[1] for u in users]
            
            # 배치 처리 (100명씩)
            BATCH_SIZE = 100
            for i in range(0, len(usernames), BATCH_SIZE):
                batch = usernames[i:i + BATCH_SIZE]
                
                try:
                    # 현재 Roblox 정보 조회
                    resp = requests.post(
                        f"{RANK_API_URL_ROOT}/bulk-status",
                        json={"usernames": batch},
                        headers=_rank_api_headers(),
                        timeout=30,
                    ) 

                    if resp.status_code == 200:
                        data = resp.json()
                        
                        for r in data.get("results", []):
                            if r.get("success"):
                                username = r['username']
                                role_info = r.get("role", {})
                                rank_name = role_info.get("name", "?")
                                
                                # Discord 닉네임 업데이트
                                for discord_id, roblox_nick in users:
                                    if roblox_nick == username:
                                        member = guild.get_member(discord_id)
                                        if member:
                                            try:
                                                new_nick = f"[{rank_name}] {username}"
                                                if len(new_nick) > 32:
                                                    new_nick = new_nick[:32]
                                                
                                                # 닉네임이 다를 때만 변경
                                                if member.nick != new_nick:
                                                    await member.edit(nick=new_nick)
                                            except Exception as e:
                                                print(f"닉네임 변경 실패 {username}: {e}")
                                        break
                    
                    # Rate limit 방지
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f"Batch {i} sync error: {e}")
                    continue 

        print(f"[{datetime.now()}] 전체 닉네임 동기화 완료")
        
    except Exception as e:
        print(f"sync_all_nicknames_task error: {e}")


@sync_all_nicknames_task.before_loop
async def before_sync_all_nicknames_task():
    await bot.wait_until_ready() 
    
@tasks.loop(seconds=5)
async def rank_log_task():
    """5분마다 그룹 가입자들의 랭크를 로그"""
    try:
        cursor.execute("SELECT guild_id, channel_id FROM rank_log_settings WHERE enabled=1")
        settings = cursor.fetchall() 

        for guild_id, channel_id in settings:
            guild = bot.get_guild(guild_id)
            if not guild:
                continue 

            channel = guild.get_channel(channel_id)
            if not channel:
                continue 

            try:
                cursor.execute(
                    "SELECT roblox_nick FROM users WHERE guild_id=? AND verified=1",
                    (guild_id,),
                )
                users = cursor.fetchall() 

                if not users:
                    continue 

                usernames = [u[0] for u in users]
                
                try:
                    resp = requests.post(
                        f"{RANK_API_URL_ROOT}/bulk-status",
                        json={"usernames": usernames},
                        headers=_rank_api_headers(),
                        timeout=30,
                    ) 

                    if resp.status_code == 200:
                        data = resp.json()
                        
                        # 현재 상태
                        current_state = {}
                        for r in data.get("results", []):
                            if r.get("success"):
                                role_info = r.get("role", {})
                                current_state[r['username']] = {
                                    "rank": role_info.get('rank', 0),
                                    "rank_name": role_info.get('name', '?')
                                } 

                        # 이전 로그 가져오기
                        cursor.execute(
                            "SELECT id, log_data FROM rank_log_history WHERE guild_id=? ORDER BY id DESC LIMIT 1",
                            (guild_id,),
                        )
                        prev_row = cursor.fetchone() 

                        changes = []
                        if prev_row:
                            prev_id, prev_log = prev_row
                            prev_data = json.loads(prev_log)
                            prev_state = {item["username"]: item for item in prev_data} 

                            # 변경 사항만 찾기
                            for username, current in current_state.items():
                                if username in prev_state:
                                    prev = prev_state[username]
                                    if prev["rank"] != current["rank"]:
                                        changes.append({
                                            "username": username,
                                            "old_rank": prev["rank"],
                                            "old_rank_name": prev["rank_name"],
                                            "new_rank": current["rank"],
                                            "new_rank_name": current["rank_name"]
                                        }) 

                        # 변경사항이 있을 때만 처리
                        if changes:
                            # 5초 안에 10명 이상 변경 시 자동 롤백 체크
                            cursor.execute(
                                "SELECT auto_rollback FROM rollback_settings WHERE guild_id=?",
                                (guild_id,),
                            )
                            rollback_row = cursor.fetchone()
                            auto_rollback = rollback_row[0] if rollback_row else 1 

                            if len(changes) >= 10 and auto_rollback == 1:
                                # 자동 롤백 실행
                                try:
                                    rollback_results = []
                                    for change in changes:
                                        resp_rollback = requests.post(
                                            f"{RANK_API_URL_ROOT}/rank",
                                            json={
                                                "username": change["username"],
                                                "rank": change["old_rank"]
                                            },
                                            headers=_rank_api_headers(),
                                            timeout=15,
                                        )
                                        if resp_rollback.status_code == 200:
                                            rollback_results.append(f"{change['username']}")
                                        else:
                                            rollback_results.append(f"{change['username']}") 

                                    # 롤백 알림
                                    embed = discord.Embed(
                                        title="자동 롤백 실행",
                                        description=f"5분 내 {len(changes)}명 변경 감지 → 자동 롤백",
                                        color=discord.Color.red(),
                                        timestamp=datetime.now(timezone.utc),
                                    )
                                    embed.add_field(
                                        name="롤백 결과",
                                        value="\n".join(rollback_results[:20]),
                                        inline=False
                                    )
                                    await channel.send(embed=embed)
                                    
                                    # 롤백했으니 로그는 저장 안 함
                                    continue 

                                except Exception as e:
                                    print(f"Auto rollback error: {e}") 

                            # 로그 저장
                            log_data = [{"username": k, **v} for k, v in current_state.items()]
                            cursor.execute(
                                "INSERT INTO rank_log_history(guild_id, log_data, created_at) VALUES(?, ?, ?)",
                                (guild_id, json.dumps(log_data), datetime.now().isoformat()),
                            )
                            conn.commit()
                            
                            cursor.execute(
                                "SELECT id FROM rank_log_history WHERE guild_id=? ORDER BY id DESC LIMIT 1",
                                (guild_id,),
                            )
                            log_id = cursor.fetchone()[0]
                            
                            # 변경사항 출력
                            change_lines = []
                            for c in changes:
                                change_lines.append(
                                    f"{c['username']}: {c['old_rank_name']}(rank {c['old_rank']}) → {c['new_rank_name']}(rank {c['new_rank']})"
                                )
                            
                            msg = "\n".join(change_lines)
                            embed = discord.Embed(
                                title="명단 변경 로그",
                                description=msg[:2000],
                                color=discord.Color.orange(),
                                timestamp=datetime.now(timezone.utc),
                            )
                            embed.set_footer(text=f"일련번호: {log_id} | 변경: {len(changes)}건")
                            await channel.send(embed=embed) 

                except Exception as e:
                    print(f"rank_log_task API error: {e}") 

            except Exception as e:
                print(f"rank_log_task error for guild {guild_id}: {e}") 

    except Exception as e:
        print(f"rank_log_task error: {e}")


@rank_log_task.before_loop
async def before_rank_log_task():
    await bot.wait_until_ready() 

@bot.event
async def on_guild_join(guild: discord.Guild):
    now_kst = datetime.now(KST)

    # =========================
    # ✅ 허용 서버
    # =========================
    if guild.id in ALLOWED_GUILD_IDS:
        dev = await bot.fetch_user(DEVELOPER_ID)
        embed = discord.Embed(
            title="✅ 허용 서버 연결",
            description=(
                f"서버 이름: {guild.name}\n"
                f"서버 ID: {guild.id}\n"
                f"인원수: {guild.member_count}"
            ),
            color=discord.Color.green(),
            timestamp=now_kst
        )
        await dev.send(embed=embed)
        return

    # =========================
    # 🔥 멤버 로딩
    # =========================
    await guild.chunk()

    # =========================
    # 🔎 교집합 유저 찾기 (허용 서버들 전부 기준)
    # =========================
    shared_members: list[discord.Member] = []

    for allowed_id in ALLOWED_GUILD_IDS:
        allowed_guild = bot.get_guild(allowed_id)
        if not allowed_guild:
            continue

        await allowed_guild.chunk()

        allowed_ids = {m.id for m in allowed_guild.members}
        for member in guild.members:
            if member.id in allowed_ids:
                shared_members.append(member)

    # =========================
    # 📩 교집합 유저 DM
    # =========================
    for member in shared_members:
        try:
            user = await bot.fetch_user(member.id)
            await user.send(
                f"⚠️ 경고: 당신은 허용되지 않은 서버 '{guild.name}'에 있습니다.\n"
                "보안 시스템에 의해 기록되었습니다."
            )
        except:
            pass

    # =========================
    # 📄 멤버 목록 파일 생성
    # =========================
    member_lines = [f"{m} ({m.id})" for m in guild.members]
    buffer = io.BytesIO("\n".join(member_lines).encode("utf-8"))
    member_file = discord.File(buffer, filename=f"{guild.id}_members.txt")

    # =========================
    # 🚨 보안 로그 임베드
    # =========================
    owner = guild.owner
    owner_text = f"{owner} ({owner.id})" if owner else "알 수 없음"
    created_text = guild.created_at.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

    log_channel = bot.get_channel(SECURITY_LOG_CHANNEL_ID)

    if log_channel:
        embed = discord.Embed(
            title="🚨 비허용 서버 감지",
            description=(
                f"서버 이름: {guild.name}\n"
                f"서버 ID: {guild.id}\n"
                f"인원수: {guild.member_count}\n"
                f"서버 주인: {owner_text}\n"
                f"생성일(KST): {created_text}\n"
                f"교집합 인원: {len(shared_members)}명\n\n"
                "봇이 즉시 서버를 떠납니다."
            ),
            color=discord.Color.red(),
            timestamp=now_kst
        )

        if guild.icon:
            embed.set_thumbnail(url=guild.icon.url)

        await log_channel.send(embed=embed, file=member_file)

    # =========================
    # ❌ 서버 탈퇴
    # =========================
    await guild.leave()

# ---------- 봇 시작 ----------
# 🔒 허가되지 않은 길드 강제 탈퇴 함수
async def force_leave(guild: discord.Guild) -> None:
    """허가되지 않은 길드에서 나가고 로그 남김."""
    try:
        print(f"[FORCE_LEAVE] Leaving unauthorized guild: {guild.name} ({guild.id})")
        await guild.leave()
    except Exception as e:
        print(f"[FORCE_LEAVE] Failed to leave guild {guild.id}: {e}") 

@tasks.loop(seconds=5)
async def update_status():
    if not status_channel_id:
        print("상태 채널 미지정")
        return

    channel = bot.get_channel(status_channel_id)
    if not channel:
        print("채널을 찾을 수 없음")
        return

    uptime = int(time.time() - bot_start_time)
    hours = uptime // 3600
    minutes = (uptime % 3600) // 60
    seconds = uptime % 60
    status_emoji = STATUS_EMOJIS.get(BOT_STATUS, "⚪")

    cpu_usage = psutil.cpu_percent(interval=0.5)
    memory_usage = psutil.virtual_memory().percent
    total_users = sum(g.member_count for g in bot.guilds)
    ping = round(bot.latency * 1000)

    embed = discord.Embed(title="🤖 봇 상태", color=discord.Color.green())
    embed.add_field(name="⏱ 업타임", value=f"{hours}시간 {minutes}분 {seconds}초", inline=False)
    embed.add_field(name="📡 봇 상태", value=f"{status_emoji} {BOT_STATUS}", inline=False)
    embed.add_field(name="🌍 서버 수", value=f"{len(bot.guilds)}개", inline=False)
    embed.add_field(name="👥 총 유저 수", value=f"{total_users}", inline=True)
    embed.add_field(name="⚡ 핑", value=f"{ping}ms", inline=True)
    embed.add_field(name="🧠 메모리 사용량", value=f"{memory_usage}%", inline=True)
    embed.add_field(name="📊 CPU 사용량", value=f"{cpu_usage}%", inline=True)

    try:
        await channel.send(embed=embed)
    except Exception as e:
        print(f"상태 전송 실패: {e}")
    
@bot.event
async def on_app_command_completion(
    interaction: discord.Interaction,
    command: discord.app_commands.Command,
):
    try:
        if not interaction.guild:
            return

        guild_id = interaction.guild.id
        user = interaction.user

        # 전체 명령어 문자열 예시: "/구매 이름: VIP"
        options = []
        if interaction.namespace:
            for k, v in interaction.namespace.__dict__.items():
                options.append(f"{k}={v}")
        full_str = f"/{command.qualified_name}"
        if options:
            full_str += " " + " ".join(options)

        cursor.execute(
            """
            INSERT INTO command_logs(
                guild_id, user_id, user_name,
                command_name, command_full, created_at
            )
            VALUES(?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                guild_id,
                user.id,
                f"{user.name}#{user.discriminator}",
                command.qualified_name,
                full_str,
            ),
        )
        conn.commit()
    except Exception as e:
        add_error_log(f"command_log: {repr(e)}")

# ------------------------
# 봇 준비 이벤트 (on_ready)
# ------------------------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

    # 🔹 허용되지 않은 서버 강제 탈퇴
    for guild in bot.guilds:
        if guild.id not in ALLOWED_GUILD_IDS:
            await force_leave(guild)  # async 함수여야 함

    # 🔹 트리 명령어 동기화
    try:
        if GUILD_ID > 0:
            await bot.tree.sync(guild=discord.Object(id=GUILD_ID))  # 특정 길드 우선 동기화
        await bot.tree.sync()  # 글로벌 명령어 동기화
    except Exception as e:
        print("동기화 실패:", e)

    # 🔹 기존 백그라운드 태스크 시작
    if not rank_log_task.is_running():
        rank_log_task.start()
    if not sync_all_nicknames_task.is_running():
        sync_all_nicknames_task.start()

    # 🔹 15초마다 상태 갱신 루프 시작 (처음 메시지 보장)
    if not update_status.is_running():
        update_status.start()  # ⚠ tasks.loop는 start()만 사용, 괄호 붙이면 안 됨

@bot.event
async def on_interaction(interaction: discord.Interaction): 

    if interaction.type == discord.InteractionType.application_command: 

        for cmd in DISABLED_COMMANDS:
            if interaction.data["name"] == cmd:
                await interaction.response.send_message(
                    "현재는 이용할 수 없습니다.",
                    ephemeral=True
                )
                return
            
# ==================== FastAPI 엔드포인트 ====================
@app.get("/api/errors")
def get_errors():
    """최근 에러 로그 반환"""
    try:
        recent_errors = error_logs[-20:] if error_logs else []  # 최근 20개
        
        formatted = []
        for err in recent_errors:
            ts = err.get("timestamp")
            if ts:
                time_str = ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(ts, 'strftime') else str(ts)
            else:
                time_str = "N/A"
            
            formatted.append({
                "timestamp": time_str,
                "message": err.get("message", "Unknown error")
            })
        
        return formatted
    except Exception as e:
        print(f"[API] Error logs error: {e}")
        return []

@app.get("/")
def root():
    return {"status": "SKY ARMY BOT", "ready": bot.is_ready()}

@app.get("/api/bot-stats")
def bot_stats():
    """Bot 통계 반환"""
    try:
        guilds_count = len(bot.guilds)
        
        # 경고 기록 수 계산
        cursor.execute("SELECT COUNT(*) FROM warnings")
        warn_count = cursor.fetchone()[0]
        
        # 인증된 유저 수
        cursor.execute("SELECT COUNT(DISTINCT discordid) FROM users WHERE verified=1")
        verified_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT discordid) FROM forcedverified")
        forced_count = cursor.fetchone()[0]
        
        total_users = verified_count + forced_count
        
        print(f"[API] Stats: {guilds_count} guilds, {total_users} users, {warn_count} warns")
        
        return {
            "guilds": guilds_count,
            "verified_users": total_users,
            "warn_records": warn_count,
        }
    except Exception as e:
        print(f"[API] Error: {e}")
        import traceback
        traceback.print_exc()
        return {"guilds": 0, "verified_users": 0, "warn_records": 0}
# ========================================================

if __name__ == "__main__":
    import asyncio
    from hypercorn.asyncio import serve
    from hypercorn.config import Config
    
    async def run_both():
        config = Config()
        config.bind = [f"0.0.0.0:{int(os.getenv('PORT', 8080))}"]
        
        await asyncio.gather(
            bot.start(TOKEN),
            serve(app, config)
        )
    
    asyncio.run(run_both())
