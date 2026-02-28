import os
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

# ---------- 기본 설정 ----------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

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

def get_guild_group_id(guild_id: int) -> Optional[int]:
    cursor.execute("SELECT group_id FROM group_settings WHERE guild_id=?", (guild_id,))
    row = cursor.fetchone()
    return row[0] if row else None


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

# ---------- 인증 View ----------

class VerifyView(discord.ui.View):
    def __init__(self, code: str, expire_time: datetime, guild_id: int):
        super().__init__(timeout=300)
        self.code = code
        self.expire_time = expire_time
        self.guild_id = guild_id

# ---------- View 클래스 ----------
class VerifyView(discord.ui.View):
    def __init__(self, code: str, expire_time: datetime, guild_id: int):
        super().__init__(timeout=300)
        self.code = code
        self.expire_time = expire_time
        self.guild_id = guild_id

    @discord.ui.button(label="인증하기", style=discord.ButtonStyle.green)
    async def verify_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        if interaction is None:
            return
        
        try:
            guild = bot.get_guild(self.guild_id)
            if guild is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "서버 정보를 불러올 수 없습니다.", ephemeral=True
                    )
                return

            cursor.execute(
                "SELECT roblox_nick, roblox_user_id, expire_time, code FROM users "
                "WHERE discord_id=? AND guild_id=?",
                (interaction.user.id, self.guild_id),
            )
            data = cursor.fetchone()

            if not data:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "인증 정보가 없습니다. 다시 /인증 명령어를 실행해주세요.",
                        ephemeral=True,
                    )
                return

            nick, roblox_user_id, expire_str, saved_code = data
            expire = datetime.fromisoformat(expire_str)

            if datetime.now() > expire:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "인증 시간이 만료되었습니다. 다시 /인증 명령어를 실행해주세요.",
                        ephemeral=True,
                    )
                return

            if saved_code != self.code:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "코드가 일치하지 않습니다.", ephemeral=True
                    )
                return

            if not roblox_user_id:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "로블록스 계정 정보가 없습니다. 다시 /인증 명령어를 실행해주세요.",
                        ephemeral=True,
                    )
                return

            description = await roblox_get_description_by_user_id(roblox_user_id)
            if description is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "로블록스 프로필을 불러올 수 없습니다. 잠시 후 다시 시도해주세요.",
                        ephemeral=True,
                    )
                return

            if self.code not in description:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "프로필 설명란에 인증 코드가 없습니다. 정확히 입력했는지 확인해주세요.",
                        ephemeral=True,
                    )
                return

            role_id = get_guild_role_id(self.guild_id)
            if not role_id:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "인증 역할이 설정되지 않았습니다. /설정 명령어를 사용해주세요.",
                        ephemeral=True,
                    )
                return

            role = guild.get_role(role_id)
            if role is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "인증 역할을 찾을 수 없습니다.", ephemeral=True
                    )
                return

            member = guild.get_member(interaction.user.id)
            if member is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "서버에서 유저 정보를 찾을 수 없습니다.", ephemeral=True
                    )
                return
            # 역할 부여
            await member.add_roles(role)
                
            # 닉네임 변경
            try:
                resp = requests.post(
                    f"{RANK_API_URL_ROOT}/bulk-status",
                    json={"usernames": [nick]},
                    headers=_rank_api_headers(),
                    timeout=15,
                )

                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get("results", [])
                    if results and results[0].get("success"):
                        role_info = results[0].get("role", {}) or {}
                        rank_name = role_info.get("name", "?")
                    else:
                        rank_name = "?"
                else:
                    rank_name = "?"

                # 여기서 ROKA | 육군 → 육군 으로 정제
                if " | " in rank_name:
                    rank_name = rank_name.split(" | ")[-1]

                new_nick = f"[{rank_name}] {nick}"
                if len(new_nick) > 32:
                    new_nick = new_nick[:32]

                await member.edit(nick=new_nick)
            except Exception as e:
                print(f"닉네임 변경 실패: {e}")
                # 실패해도 인증은 완료


            cursor.execute(
                "UPDATE users SET verified=1 WHERE discord_id=? AND guild_id=?",
                (interaction.user.id, self.guild_id),
            )
            cursor.execute(
                "INSERT OR IGNORE INTO stats(guild_id) VALUES(?)",
                (self.guild_id,),
            )
            cursor.execute(
                "UPDATE stats SET verify_count = verify_count + 1 WHERE guild_id=?",
                (self.guild_id,),
            )
            conn.commit()

            if not interaction.response.is_done():
                await interaction.response.send_message("인증 완료!", ephemeral=True)

        except Exception as e:
            add_error_log(f"verify_button: {repr(e)}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "내부 오류가 발생했습니다.", ephemeral=True
                )
    
# ---------- 슬래시 명령어 ----------

@bot.tree.command(name="인증", description="로블록스 계정 인증을 시작합니다.")
@app_commands.describe(로블닉="로블록스 닉네임")
async def verify(interaction: discord.Interaction, 로블닉: str):
    await interaction.response.defer(ephemeral=True)

    cursor.execute(
        "SELECT verified FROM users WHERE discord_id=? AND guild_id=?",
        (interaction.user.id, interaction.guild.id),
    )
    data = cursor.fetchone()
    if data and data[0] == 1:
        await interaction.followup.send("이미 인증된 사용자입니다.", ephemeral=True)
        return

    user_id = await roblox_get_user_id_by_username(로블닉)
    if not user_id:
        await interaction.followup.send(
            "해당 닉네임의 로블록스 계정을 찾을 수 없습니다.", ephemeral=True
        )
        return

    # ✅ 블랙리스트 체크
    cursor.execute(
        "SELECT group_id FROM blacklist WHERE guild_id=?",
        (interaction.guild.id,),
    )
    blacklist_groups = set([row[0] for row in cursor.fetchall()])

    if blacklist_groups:
        # 비동기로 사용자 그룹 확인
        user_groups = await roblox_get_user_groups(user_id)

        # 블랙리스트 그룹에 속하는지 체크
        blocked_groups = [g for g in user_groups if g in blacklist_groups]

        if blocked_groups:
            await interaction.followup.send(
                f"❌ 블랙리스트된 그룹에 속해 있어서 인증할 수 없습니다.\n차단된 그룹: {', '.join(map(str, blocked_groups))}",
                ephemeral=True,
            )
            return

    code = generate_code()
    expire_time = datetime.now() + timedelta(minutes=5)

    cursor.execute(
        """INSERT OR REPLACE INTO users(
               discord_id, guild_id, roblox_nick,
               roblox_user_id, code, expire_time, verified
           )
           VALUES(?,?,?,?,?,?,0)""",
        (
            interaction.user.id,
            interaction.guild.id,
            로블닉,
            user_id,
            code,
            expire_time.isoformat(),
        ),
    )
    conn.commit()

    embed = discord.Embed(title="로블록스 인증", color=discord.Color.blue())
    embed.description = (
        f"> Roblox: `{로블닉}` (ID: `{user_id}`)\n"
        f"> 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        "1️⃣ Roblox 프로필로 이동\n"
        "2️⃣ 설명란에 코드 입력\n"
        "3️⃣ '인증하기' 버튼 클릭\n\n"
        f"🔐 코드: `{code}`\n"
        "⏱ 남은 시간: 5분\n\n"
        "made by Lunar"
    )

    try:
        await interaction.user.send(
            embed=embed, view=VerifyView(code, expire_time, interaction.guild.id)
        )
        await interaction.followup.send("📩 DM을 확인해주세요.", ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send(
            "DM 전송 실패. DM 수신을 허용해주세요.", ephemeral=True
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

@bot.tree.command(name="역할전체", description="서버 역할과 봇 역할을 10개씩 출력합니다.")
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
                title=f"📋 서버 역할 목록 (총 {len(roles)}개) ({idx}/{len(chunks)})",
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
                title=f"🤖 봇 역할 목록 (총 {len(bot_roles)}개) ({idx}/{len(chunks)})",
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
            newRole = data.get("newRole", {})
            await interaction.followup.send(
                f"`{username}` 님을 역할 `{role_name}` 으로 변경했습니다.\n"
                f"실제 반영: {newRole.get('name','?')} (rank {newRole.get('rank','?')})",
                ephemeral=True,
            )
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
        print("DEBUG ROOT:", repr(RANK_API_URL_ROOT))
        print("DEBUG URL:", f"{RANK_API_URL_ROOT}/rank")
        resp = requests.post(
            f"{RANK_API_URL_ROOT}/rank",
            json=payload,
            headers=_rank_api_headers(),
            timeout=30,
        )
        print("DEBUG STATUS:", resp.status_code, resp.text[:200])

        if resp.status_code == 200:
            data = resp.json()
            newRole = data.get("newRole", {})
            await interaction.followup.send(
                f"`{username}` 님을 역할 `{role_name}` 으로 변경했습니다.\n"
                f"실제 반영: {newRole.get('name','?')} (rank {newRole.get('rank','?')})",
                ephemeral=True,
            )
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


    cursor.execute(
        "SELECT roblox_nick FROM users WHERE guild_id=? AND verified=1",
        (interaction.guild.id,),
    )
    verified_users = [row[0] for row in cursor.fetchall() if row[0]]

    cursor.execute(
        "SELECT roblox_nick FROM forced_verified WHERE guild_id=?",
        (interaction.guild.id,),
    )
    forced_excluded = set([row[0] for row in cursor.fetchall() if row[0]])

    all_users = [u for u in verified_users if u not in forced_excluded]

    if not all_users:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return

    total = len(all_users)
    
    # 대량 처리 경고
    if total > 1000:
        await interaction.followup.send(
            f"{total}명 처리 예정 (약 {total // 60}분 소요)\n처리 시작합니다...",
            ephemeral=True
        )

    BATCH_SIZE = 100
    all_results = []
    
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
            
            # 진행 상황 업데이트 (1000명마다)
            if (i + BATCH_SIZE) % 1000 == 0:
                await interaction.followup.send(
                    f"진행 중... {i + BATCH_SIZE}/{total}명",
                    ephemeral=True
                )
            
            # Rate limit 방지
            await asyncio.sleep(1)
            
        except Exception as e:
            print(f"Batch {i} error: {e}")
            continue

    # 최종 결과
    embed = discord.Embed(title="일괄 승진 완료", color=discord.Color.green())
    embed.add_field(name="총 처리", value=f"{total}명", inline=True)
    embed.add_field(name="성공", value=f"{len([r for r in all_results if r.get('success')])}명", inline=True)
    embed.add_field(name="실패", value=f"{len([r for r in all_results if not r.get('success')])}명", inline=True)
    
    await interaction.followup.send(embed=embed, ephemeral=True)

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
    forced_excluded = set([row[0] for row in cursor.fetchall() if row[0]])

    all_users = [u for u in verified_users if u not in forced_excluded]

    if not all_users:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return

    total = len(all_users)
    
    # 대량 처리 경고
    if total > 1000:
        await interaction.followup.send(
            f"{total}명 처리 예정 (약 {total // 60}분 소요)\n처리 시작합니다...",
            ephemeral=True
        )

    BATCH_SIZE = 100
    all_results = []
    
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
            
            # 진행 상황 업데이트 (1000명마다)
            if (i + BATCH_SIZE) % 1000 == 0:
                await interaction.followup.send(
                    f"진행 중... {i + BATCH_SIZE}/{total}명",
                    ephemeral=True
                )
            
            # Rate limit 방지
            import asyncio
            await asyncio.sleep(1)
            
        except Exception as e:
            print(f"Batch {i} error: {e}")
            continue

    # 최종 결과
    embed = discord.Embed(title="일괄 강등 완료", color=discord.Color.red())
    embed.add_field(name="총 처리", value=f"{total}명", inline=True)
    embed.add_field(name="성공", value=f"{len([r for r in all_results if r.get('success')])}명", inline=True)
    embed.add_field(name="실패", value=f"{len([r for r in all_results if not r.get('success')])}명", inline=True)
    
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="동기화", description="슬래시 명령어를 다시 동기화합니다. (관리자)")
async def sync_commands(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용 가능합니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        if interaction.guild:
            await bot.tree.sync(guild=interaction.guild)
            msg = f"{interaction.guild.name}({interaction.guild.id}) 에서 슬래시 명령 동기화 완료"
        else:
            await bot.tree.sync()
            msg = "글로벌 슬래시 명령 동기화 완료"

        await interaction.followup.send(msg, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"동기화 중 오류: {e}", ephemeral=True)

@bot.tree.command(name="강제인증", description="유저를 강제로 인증 처리합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(
    user="Discord 유저 멘션",
    roblox_nick="Roblox 본닉"
)
async def force_verify(interaction: discord.Interaction, user: discord.User, roblox_nick: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    # Roblox 유저 ID 가져오기
    user_id = await roblox_get_user_id_by_username(roblox_nick)
    if not user_id:
        await interaction.followup.send(
            f"해당 닉네임의 로블록스 계정을 찾을 수 없습니다.",
            ephemeral=True,
        )
        return

    # users 테이블에 verified=1로 직접 저장 (인증 처리)
    cursor.execute(
        """INSERT OR REPLACE INTO users(discord_id, guild_id, roblox_nick, roblox_user_id, code, expire_time, verified)
           VALUES(?, ?, ?, ?, ?, ?, 1)""",
        (user.id, interaction.guild.id, roblox_nick, user_id, "forced", datetime.now().isoformat()),
    )
    conn.commit()

    # 인증 역할 부여
    role_id = get_guild_role_id(interaction.guild.id)
    if role_id:
        role = interaction.guild.get_role(role_id)
        member = interaction.guild.get_member(user.id)
        if member and role:
            try:
                await member.add_roles(role)
            except:
                pass

    embed = discord.Embed(
        title="강제인증 완료",
        color=discord.Color.green(),
        description=f"{user.mention} 을(를) {roblox_nick}로 인증 처리했습니다."
    )
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="일괄닉네임변경", description="인증된 유저의 닉네임을 [랭크] 본닉 형식으로 변경합니다. (관리자)")
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
                role_info = r.get("role", {})
                rank_map[r['username']] = role_info.get('name', '?')

        updated = 0
        failed = 0

        for discord_id, roblox_nick in users_data:
            try:
                member = interaction.guild.get_member(discord_id)
                if member:
                    rank_name = rank_map.get(roblox_nick, '?')
                    new_nick = f"[{rank_name}] {roblox_nick}"
                    
                    # 닉네임 32자 제한
                    if len(new_nick) > 32:
                        new_nick = new_nick[:32]
                    
                    await member.edit(nick=new_nick)
                    updated += 1
            except Exception as e:
                print(f"닉네임 변경 실패 {roblox_nick}: {e}")
                failed += 1

        embed = discord.Embed(
            title="일괄 닉네임 변경 완료",
            color=discord.Color.blue()
        )
        embed.add_field(name="성공", value=str(updated), inline=True)
        embed.add_field(name="실패", value=str(failed), inline=True)
        embed.add_field(name="형식", value="[랭크] 로블 본닉", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)

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

    

# ---------- 봇 시작 ----------
@bot.event
async def on_ready():
    print(f"로그인: {bot.user} (id={bot.user.id})")
    try:
        # 특정 길드에만 등록하고 싶으면 GUILD_ID 사용
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild)
            await bot.tree.sync(guild=guild)
            print(f"슬래시 명령 동기화 완료 (guild={GUILD_ID})")
        else:
            # 전체 글로벌 커맨드 동기화
            await bot.tree.sync()
            print("글로벌 슬래시 명령 동기화 완료")
    except Exception as e:
        print(f"슬래시 명령 동기화 실패: {e}")

if __name__ == "__main__":
    bot.run(TOKEN)
