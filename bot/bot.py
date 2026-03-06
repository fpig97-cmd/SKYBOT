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

import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks
from discord.ext import commands
from dotenv import load_dotenv
import requests
from datetime import datetime
from enum import Enum

VERIFY_ROLE_ID = 1461636782176075831      # 🟢 인증자 역할 ID
UNVERIFY_ROLE_ID = 1478713261074550956     # 🔴 제거할 역할 ID (예: 미인증자)
ADMIN_LOG_CHANNEL_ID = 1468191799855026208 # 📋 관리자 로그 채널 ID

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

intents = discord.Intents.default()
intents.members = True

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

bot = commands.Bot(command_prefix="!", intents=intents)

error_logs: list[dict] = []
MAX_LOGS = 50

DB_PATH = os.path.join(BASE_DIR, "bot.db")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

# ---------- DB 스키마 ----------
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

conn.commit()

# ---------- 설정/권한 유틸 ----------

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
            # 1️⃣ 설정에서 가져오는 역할 (필요하면 VERIFY_ROLE_ID 대신 쓸 수 있음)
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

            # 4) (선택) 랭크 API로 닉네임 변경
            rankname = "?"
            try:
                if RANK_API_URL_ROOT:
                    resp = requests.post(
                        f"{RANK_API_URL_ROOT}/bulk-status",
                        json={"usernames": [self.roblox_nick]},
                        headers=_rank_api_headers(),
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        results = data.get("results", [])
                        if results and results[0].get("success"):
                            roleinfo = results[0].get("role") or {}
                            rankname = roleinfo.get("name", "?")
                if " | " in rankname:
                    rankname = rankname.split(" | ")[-1]
                newnick = f"[{rankname}] {self.roblox_nick}"
                if len(newnick) > 32:
                    newnick = newnick[:32]
                try:
                    await member.edit(nick=newnick)
                except Exception as e:
                    print("[NICK_EDIT_ERROR]", e)
            except Exception as e:
                print("[RANK_API_ERROR]", e)

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
                            group_rank=rankname,
                            account_age_days=None,
                            new_nick=member.nick,
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
            title="📩 인증 요청",
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
            title="✅ 인증 성공",
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
            title="⚠️ 그룹 미가입",
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
            title="❌ 인증 실패",
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
        title = "일괄 승진 완료"
        color = discord.Color.green()
        desc = "여러 멤버 승진 작업이 완료되었습니다."
    else:
        title = "일괄 강등 완료"
        color = discord.Color.red()
        desc = "여러 멤버 강등 작업이 완료되었습니다."

    embed = discord.Embed(title=title, description=desc, color=color)
    embed.add_field(name="변경 역할", value=f"`{role_name}`", inline=False)
    embed.add_field(name="총 처리", value=f"{total}명", inline=True)
    embed.add_field(name="성공", value=f"{success}명", inline=True)
    embed.add_field(name="실패", value=f"{failed}명", inline=True)

    if executor:
        embed.add_field(name="실행자", value=executor.mention, inline=False)

    embed.set_footer(text="Made By Lunar")
    return embed
# ---------- 사용안하는 명령어 ----------

DISABLED_COMMANDS = ["역할목록", "역할전체변경",
 "일괄닉네임변경", "장교역할"]

@bot.tree.interaction_check
async def check(interaction: discord.Interaction):

    if interaction.command.name in DISABLED_COMMANDS:
        await interaction.response.send_message(
            "현재는 이용할 수 없습니다.",
            ephemeral=True
        )
        return False

    return True
# ---------- 슬래시 명령어 ----------

@bot.tree.command(name="명령어차단")
@app_commands.describe(state="all / true / false")
async def toggle_commands(interaction: discord.Interaction, state: str):

    global COMMANDS_DISABLED

    if interaction.user.id != DEVELOPER_ID:
        await interaction.response.send_message("개발자 전용입니다.", ephemeral=True)
        return

    if state.lower() == "true":
        COMMANDS_DISABLED = True
        msg = "명령어 차단이 **ON** 되었습니다."

    elif state.lower() == "false":
        COMMANDS_DISABLED = False
        msg = "명령어 차단이 **OFF** 되었습니다."

    elif state.lower() == "all":
        COMMANDS_DISABLED = True
        msg = "모든 대상 명령어가 **차단되었습니다.**"

    else:
        await interaction.response.send_message(
            "옵션: all / true / false",
            ephemeral=True
        )
        return

    await interaction.response.send_message(msg, ephemeral=True)

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
    def chunk_lines(title: str, members_list: list[discord.Member], emoji: str):
        if not members_list:
            return []

        lines = [f"- {m.mention}" for m in members_list]
        text = "\n".join(lines)
        MAX_LEN = 1900
        chunks: list[str] = []

        while text:
            if len(text) <= MAX_LEN:
                chunks.append(text)
                break
            cut = text.rfind("\n", 0, MAX_LEN)
            if cut == -1:
                cut = MAX_LEN
            chunks.append(text[:cut])
            text = text[cut:].lstrip("\n")

        embeds: list[discord.Embed] = []
        total = len(members_list)

        for idx, chunk_text in enumerate(chunks, start=1):
            e = discord.Embed(
                title=f"{emoji} {title} ({idx}/{len(chunks)})",
                description=(
                    f"{'✅ 인증자' if emoji == '🟢' else '❌ 미인증자'} "
                    f"({total}명)\n{chunk_text}"
                ),
                color=discord.Color.green() if emoji == "🟢" else discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            e.set_footer(text=f"Made By Lunar | 총 {total}명")
            embeds.append(e)

        return embeds

    # ----------------- 메인 통계 Embed -----------------
    main_embed = discord.Embed(
        title="📊 인증 통계",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    main_embed.add_field(name="서버 인원수", value=f"{total_members}명", inline=False)
    main_embed.add_field(
        name="✅ 인증",
        value=f"{verified_count}명 ({verified_pct}%)",
        inline=True,
    )
    main_embed.add_field(
        name="❌ 미인증",
        value=f"{not_verified_count}명 ({not_verified_pct}%)",
        inline=True,
    )
    main_embed.add_field(
        name="합계",
        value=f"{verified_count + not_verified_count}명",
        inline=False,
    )

    embeds_to_send: list[discord.Embed] = [main_embed]
    embeds_to_send += chunk_lines("인증자", verified_members, "🟢")
    embeds_to_send += chunk_lines("미인증자", not_verified_members, "🔴")

    # ----------------- 페이지네이션 버튼 -----------------
    class Pages(discord.ui.View):
        def __init__(self, embeds: list[discord.Embed]):
            super().__init__(timeout=None)
            self.embeds = embeds
            self.current = 0

        @discord.ui.button(label="◀ 이전", style=discord.ButtonStyle.grey)
        async def prev(self, interaction2: discord.Interaction, button: discord.ui.Button):
            self.current = (self.current - 1) % len(self.embeds)
            await interaction2.response.edit_message(
                embed=self.embeds[self.current],
                view=self,
            )

        @discord.ui.button(label="다음 ▶", style=discord.ButtonStyle.grey)
        async def next(self, interaction2: discord.Interaction, button: discord.ui.Button):
            self.current = (self.current + 1) % len(self.embeds)
            await interaction2.response.edit_message(
                embed=self.embeds[self.current],
                view=self,
            )

    await interaction.followup.send(
        embed=embeds_to_send[0],
        view=Pages(embeds_to_send),
        ephemeral=True,
    )

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

@bot.tree.command(name="설정", description="인증 역할 설정 (관리자)")
@app_commands.describe(역할="인증 역할")
async def configure(interaction: discord.Interaction, 역할: discord.Role):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    bot_member = interaction.guild.me
    if bot_member.top_role <= 역할:
        await interaction.response.send_message(
            "봇의 최상위 역할보다 위의 역할은 설정할 수 없습니다.", ephemeral=True
        )
        return

    set_guild_role_id(interaction.guild.id, 역할.id)
    await interaction.response.send_message(
        f"인증 역할을 {역할.mention}로 설정했습니다.", ephemeral=True
    )

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


@bot.tree.command(name="강제인증해제", description="유저의 인증을 해제합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(user="인증 해제할 Discord 유저")
async def unverify_user(interaction: discord.Interaction, user: discord.User):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    cursor.execute(
        "SELECT verified FROM users WHERE discord_id=? AND guild_id=?",
        (user.id, interaction.guild.id),
    )
    data = cursor.fetchone()
    
    if not data or data[0] == 0:
        await interaction.followup.send(f"{user.mention}은(는) 인증된 기록이 없습니다.", ephemeral=True)
        return

    # users 테이블에서 삭제
    cursor.execute(
        "DELETE FROM users WHERE discord_id=? AND guild_id=?",
        (user.id, interaction.guild.id),
    )
    
    # forced_verified에서도 삭제
    cursor.execute(
        "DELETE FROM forced_verified WHERE discord_id=? AND guild_id=?",
        (user.id, interaction.guild.id),
    )
    conn.commit()

    # 인증 역할 제거
    role_id = get_guild_role_id(interaction.guild.id)
    if role_id:
        role = interaction.guild.get_role(role_id)
        member = interaction.guild.get_member(user.id)
        if member and role and role in member.roles:
            try:
                await member.remove_roles(role)
            except:
                pass

    embed = discord.Embed(
        title="인증 해제 완료",
        color=discord.Color.orange(),
        description=f"{user.mention}의 인증을 해제했습니다."
    )
    await interaction.followup.send(embed=embed, ephemeral=True)

 
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
        
        # Discord 닉네임 변경
        new_nick = f"[{rank_name}] {roblox_nick}"
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        
        if member:
            await member.edit(nick=new_nick)
        
        # 위관급/영관급 역할 부여
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
        description=f"{user.mention} 을(를) {roblox_nick}로 인증 처리했습니다.\nDiscord 닉: `{new_nick}`"
    )
    send_log_to_web(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        action="verify_success",
        detail=f"{roblox_nick} ({user_id})",
    )

    await interaction.followup.send(embed=embed, ephemeral=True)

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
@bot.tree.command(
    name="일괄닉네임변경",
    description="인증된 유저의 닉네임을 [랭크] 본닉 형식으로 변경합니다. (관리자)"
)
@app_commands.guilds(discord.Object(id=GUILD_ID))

async def bulk_nickname_change(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        # 인증된 유저 목록
        cursor.execute(
            "SELECT discord_id, roblox_nick FROM users WHERE guild_id=? AND verified=1",
            (interaction.guild.id,),
        )
        users_data = cursor.fetchall()

        if not users_data:
            await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
            return

        # 모든 유저의 현재 랭크 조회
        usernames = [row[1] for row in users_data]

        resp = requests.post(
            f"{RANK_API_URL_ROOT}/bulk-status",
            json={"usernames": usernames},
            headers=_rank_api_headers(),
            timeout=60,
        )

        if resp.status_code != 200:
            await interaction.followup.send(
                f"랭크 조회 실패 (HTTP {resp.status_code})", ephemeral=True
            )
            return

        data = resp.json()

        # username -> rank_name 매핑
        rank_map = {}
        for r in data.get("results", []):
            if r.get("success"):
                role_info = r.get("role", {}) or {}
                rank_map[r["username"]] = role_info.get("name", "?")

        updated = 0
        failed = 0

        for discord_id, roblox_nick in users_data:
            try:
                member = interaction.guild.get_member(discord_id)
                if not member:
                    failed += 1
                    continue

                rank_name = rank_map.get(roblox_nick, "?") or "?"

                # ROKA | 육군 → 육군
                if " | " in rank_name:
                    rank_name = rank_name.split(" | ")[-1]

                new_nick = f"[{rank_name}] {roblox_nick}"

                if len(new_nick) > 32:
                    new_nick = new_nick[:32]

                await member.edit(nick=new_nick)
                updated += 1

            except Exception as e:
                print(f"닉네임 변경 실패 {roblox_nick}: {e}")
                failed += 1

        embed = discord.Embed(
            title="일괄 닉네임 변경 완료",
            color=discord.Color.blue(),
        )
        embed.add_field(name="성공", value=str(updated), inline=True)
        embed.add_field(name="실패", value=str(failed), inline=True)
        embed.add_field(name="형식", value="[랭크] 로블 본닉", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)

@bot.tree.command(name="로그채널지정", description="로그 채널을 설정합니다. (관리자)")
@app_commands.describe(
    인증="인증 로그 채널",
    그룹변경="그룹변경 로그 채널",
    관리자="관리자 로그 채널",
    보안="보안 로그 채널",
    개발자="개발자 로그 채널",
)
async def set_log_channels(
    interaction: discord.Interaction,
    인증: discord.TextChannel | None = None,
    그룹변경: discord.TextChannel | None = None,
    관리자: discord.TextChannel | None = None,
    보안: discord.TextChannel | None = None,
    개발자: discord.TextChannel | None = None,
):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("길드에서만 사용 가능합니다.", ephemeral=True)
        return

    changed: list[str] = []

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

    if not changed:
        await interaction.response.send_message(
            "변경된 채널이 없습니다. 최소 한 개 이상 지정해 주세요.",
            ephemeral=True,
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

@bot.tree.command(name="역할전체변경", description="모든 유저의 역할을 한 역할로 통일합니다. (위험)")
async def set_all_role(interaction: discord.Interaction):
    guild = interaction.guild
    if guild.id != GUILD_ID:
        await interaction.response.send_message("이 명령어는 지정된 서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    target_role = guild.get_role(TARGET_ROLE_ID)
    if not target_role:
        await interaction.response.send_message("대상 역할을 찾을 수 없습니다.", ephemeral=True)
        return

    await interaction.response.send_message("모든 멤버 역할 변경 시작...", ephemeral=True)

    success = 0
    failed = 0
    skipped = 0

    for member in guild.members:
        # 봇은 스킵
        if member.bot:
            continue

        # 봇 위상보다 높은/같은 멤버는 어차피 못 건드리니 스킵[web:80]
        if guild.me.top_role <= member.top_role:
            skipped += 1
            continue

        try:
            # @everyone 역할은 항상 첫 번째, 제거하면 안 됨[web:58]
            everyone = member.roles[0]
            new_roles = [everyone, target_role]

            await member.edit(roles=new_roles)
            success += 1

            # 레이트리밋 완화용 (인원 많으면 조절)
            await asyncio.sleep(0.3)

        except discord.Forbidden:
            # 권한 부족(역할 위상 등) → 그 멤버만 예외
            print(f"{member} 권한 부족으로 스킵")
            failed += 1
        except Exception as e:
            print(f"{member} 역할 변경 실패: {e}")
            failed += 1

    await interaction.followup.send(
        f"역할 변경 완료\n"
        f"성공: {success}명\n"
        f"실패: {failed}명\n"
        f"위상/조건으로 스킵: {skipped}명",
        ephemeral=True
    )

@bot.tree.command(name="장교역할", description="장교 (영관급 ~ 장성급) 에게 부여할 역할을 설정합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(role="영관급 장교 역할")

async def set_senior_officer_role(interaction: discord.Interaction, role: discord.Role):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    if interaction.guild.me.top_role <= role:
        await interaction.response.send_message(
            "봇의 최상위 역할보다 위의 역할은 설정할 수 없습니다.", ephemeral=True
        )
        return

    set_senior_officer_role_id(interaction.guild.id, role.id)
    
    await interaction.response.send_message(
        f"장교 역할을 {role.mention}으로 설정했습니다.",
        ephemeral=True
    )

@bot.tree.command(name="업데이트", description="유저의 Discord 닉네임을 변경합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(
    user="디스코드 유저 멘션",
    roblox_nick="로블록스 닉네임",
)
async def update_user(
    interaction: discord.Interaction,
    user: discord.User,
    roblox_nick: str
):
    # 관리자 체크
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    # 1. 새로운 Roblox 유저 ID 확인
    new_user_id = await roblox_get_user_id_by_username(roblox_nick)
    if not new_user_id:
        await interaction.followup.send(
            f"해당 닉네임 ({roblox_nick}) 의 로블록스 계정을 찾을 수 없습니다.",
            ephemeral=True
        )
        return

    # 2. DB에서 기존 유저 정보 확인
    cursor.execute(
        "SELECT verified FROM users WHERE discord_id=? AND guild_id=?",
        (user.id, interaction.guild.id),
    )
    data = cursor.fetchone()

    if not data or data[0] == 0:
        await interaction.followup.send(
            f"{user.mention}은(는) 인증된 유저가 아닙니다. 먼저 인증해주세요.",
            ephemeral=True
        )
        return

    # 3. DB 업데이트 (roblox_nick, roblox_user_id)
    cursor.execute(
        """
        UPDATE users 
        SET roblox_nick = ?, roblox_user_id = ?
        WHERE discord_id = ? AND guild_id = ?
        """,
        (roblox_nick, new_user_id, user.id, interaction.guild.id)
    )
    conn.commit()

    # 4. 현재 Roblox 랭크 조회
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
    except Exception as e:
        print(f"랭크 조회 실패: {e}")
        rank_name = "?"

    # 5. Discord 닉네임 변경: [랭크] 만 사용 (로블닉 제외)
    member = interaction.guild.get_member(user.id)
    if member:
        try:
            new_nick = f"[{rank_name}]"

            # 닉네임 길이 제한 (32자)
            if len(new_nick) > 32:
                new_nick = new_nick[:32]

            await member.edit(nick=new_nick)
        except Exception as e:
            print(f"닉네임 변경 실패: {e}")

            # 위관급/영관급 역할 즉시 부여
        is_junior, is_senior = check_is_officer(rank_num, rank_name)

        officer_role_id = get_officer_role_id(interaction.guild.id)
        if officer_role_id:
            officer_role = interaction.guild.get_role(officer_role_id)
            if officer_role and member:
                try:
                    if is_junior and officer_role not in member.roles:
                        await member.add_roles(officer_role)
                    elif not is_junior and officer_role in member.roles:
                        await member.remove_roles(officer_role)
                except:
                    pass

        senior_officer_role_id = get_senior_officer_role_id(interaction.guild.id)
        if senior_officer_role_id:
            senior_officer_role = interaction.guild.get_role(senior_officer_role_id)
            if senior_officer_role and member:
                try:
                    if is_senior and senior_officer_role not in member.roles:
                        await member.add_roles(senior_officer_role)
                    elif not is_senior and senior_officer_role in member.roles:
                        await member.remove_roles(senior_officer_role)
                except:
                    pass


    # 6. 결과 응답
    embed = discord.Embed(
        title="유저 정보 업데이트 완료",
        color=discord.Color.green()
    )
    embed.add_field(name="유저", value=user.mention, inline=True)
    embed.add_field(name="새 Discord 닉네임", value=f"[{rank_name}]", inline=True)

    await interaction.followup.send(embed=embed, ephemeral=True)

ALLOWED_GUILD_ID = 1461636782176075830
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

@tasks.loop(minutes=5)
async def officer_role_sync_task():
    """5분마다 인증된 유저의 랭크를 체크하여 위관급 장교 역할 자동 부여/해제"""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return

        officer_role = guild.get_role(OFFICER_ROLE_ID)
        if not officer_role:
            return

        # 인증된 모든 유저 조회
        cursor.execute(
            "SELECT discord_id, roblox_nick FROM users WHERE guild_id=? AND verified=1",
            (GUILD_ID,),
        )
        users = cursor.fetchall()
        if not users:
            return

        usernames = [u[1] for u in users]

        BATCH_SIZE = 100
        for i in range(0, len(usernames), BATCH_SIZE):
            batch = usernames[i:i + BATCH_SIZE]

            try:
                # 현재 Roblox 랭크 일괄 조회
                resp = requests.post(
                    f"{RANK_API_URL_ROOT}/bulk-status",
                    json={"usernames": batch},
                    headers=_rank_api_headers(),
                    timeout=30,
                )

                if resp.status_code == 200:
                    data = resp.json()

                    # username -> rank 정보 매핑
                    rank_map = {}
                    for r in data.get("results", []):
                        if r.get("success"):
                            role_info = r.get("role", {})
                            rank_map[r["username"]] = {
                                "name": role_info.get("name", ""),
                                "rank": role_info.get("rank", 0),
                            }

                    # 각 유저의 역할 부여/해제
                    for discord_id, roblox_nick in users:
                        if roblox_nick not in rank_map:
                            continue

                        member = guild.get_member(discord_id)
                        if not member:
                            continue

                        info = rank_map[roblox_nick]
                        rank_name = info["name"]
                        rank_num = info["rank"]

                        # 위관급 판정 (예: 80~120)
                        is_officer = 80 <= rank_num <= 120

                        # 이름으로도 판정 (한글/영문 모두)
                        officer_keywords = [
                            "Second Lieutenant", "First Lieutenant", "Captain",
                            "Major", "Lieutenant Colonel",
                            "소위", "중위", "대위", "소령", "중령",
                        ]
                        if any(kw.lower() in rank_name.lower() for kw in officer_keywords):
                            is_officer = True

                        # 역할 부여/해제
                        try:
                            if is_officer and officer_role not in member.roles:
                                await member.add_roles(officer_role)
                                print(f"[{guild.name}] {member} 위관급 역할 부여")
                            elif not is_officer and officer_role in member.roles:
                                await member.remove_roles(officer_role)
                                print(f"[{guild.name}] {member} 위관급 역할 해제")
                        except Exception as e:
                            print(f"역할 변경 실패 {member}: {e}")

                # Rate limit 방지
                await asyncio.sleep(1)

            except Exception as e:
                print(f"Batch {i} officer sync error: {e}")
                continue

    except Exception as e:
        print(f"officer_role_sync_task error: {e}")


@officer_role_sync_task.before_loop
async def before_officer_role_sync_task():
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
    if guild.id == ALLOWED_GUILD_ID:
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

    allowed_guild = bot.get_guild(ALLOWED_GUILD_ID)
    if allowed_guild:
        await allowed_guild.chunk()

    # =========================
    # 🔎 교집합 유저 찾기
    # =========================
    shared_members = []

    if allowed_guild:
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

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

    # 🔒 시작 시 서버 강제 검사
    for guild in bot.guilds:
        if guild.id != ALLOWED_GUILD_ID:
            print(f"Unauthorized guild found on startup: {guild.name}")
            await force_leave(guild)

    # 슬래시 커맨드 동기화
    try:
        if GUILD_ID > 0:
            guild_obj = discord.Object(id=GUILD_ID)
            await bot.tree.sync(guild=guild_obj)
        await bot.tree.sync()
    except Exception as e:
        print("동기화 실패:", e)

    # 백그라운드 태스크 시작
    if not rank_log_task.is_running():
        rank_log_task.start()

    if not sync_all_nicknames_task.is_running():
        sync_all_nicknames_task.start()
    if not officer_role_sync_task.is_running():
        officer_role_sync_task.start()
# 명령어 막기

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

if __name__ == "__main__":
    bot.run(TOKEN)
