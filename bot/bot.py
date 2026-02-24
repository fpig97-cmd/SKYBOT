import os
import re
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
bot = commands.Bot(command_prefix="/", intents=intents)

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


def is_admin(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True

    admin_ids = get_guild_admin_role_ids(member.guild.id)
    for rid in admin_ids:
        role = member.guild.get_role(int(rid))
        if role and role in member.roles:
            return True

    return False


def is_owner(user_id: int) -> bool:
    return OWNER_ID > 0 and user_id == OWNER_ID


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
                "SELECT roblox_nick, roblox_user_id, expire_time, code FROM users WHERE discord_id=? AND guild_id=?",
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

            await member.add_roles(role)

            cursor.execute(
                "UPDATE users SET verified=1 WHERE discord_id=? AND guild_id=?",
                (interaction.user.id, self.guild_id),
            )
            cursor.execute(
                "INSERT OR IGNORE INTO stats(guild_id) VALUES(?)", (self.guild_id,)
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

# ---------- 공용 유틸 ----------
async def roblox_get_user_groups(user_id: int) -> list[int]:
    """사용자가 속한 그룹 ID 목록 반환"""
    url = f"https://groups.roblox.com/v1/users/{user_id}/groups"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                groups = data.get("data", [])
                return [g.get("group", {}).get("id") for g in groups if g.get("group")]
        except Exception as e:
            add_error_log(f"roblox_get_user_groups: {repr(e)}")
            return []
        
def get_verified_users_in_guild(guild_id: int):
    cursor.execute(
        "SELECT discord_id, roblox_nick, roblox_user_id FROM users WHERE guild_id=? AND verified=1",
        (guild_id,),
    )
    return cursor.fetchall()


def _rank_api_headers():
    return {"Content-Type": "application/json", "X-API-KEY": RANK_API_KEY}

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

    #  블랙리스트 그룹 체크
    cursor.execute(
        "SELECT group_id FROM blacklist WHERE guild_id=?",
        (interaction.guild.id,),
    )
    blacklist_groups = set([row[0] for row in cursor.fetchall()])
    
    if blacklist_groups:
        # 사용자가 속한 그룹 확인
        user_groups = await roblox_get_user_groups(user_id)
        
        # 블랙리스트 그룹에 속하는지 체크
        blocked_groups = [g for g in user_groups if g in blacklist_groups]
        
        if blocked_groups:
            await interaction.followup.send(
                f" 블랙리스트된 그룹에 속해 있어서 인증할 수 없습니다.\n차단된 그룹: {', '.join(map(str, blocked_groups))}",
                ephemeral=True
            )
            return

    code = generate_code()
    expire_time = datetime.now() + timedelta(minutes=5)

    cursor.execute(
        """INSERT OR REPLACE INTO users(discord_id, guild_id, roblox_nick,
           roblox_user_id, code, expire_time, verified)
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


@bot.tree.command(name="관리자지정", description="관리자 역할 설정 (개발자)")
@app_commands.describe(역할들="관리자 역할들을 멘션으로 여러 개 입력 (비워두면 전부 해제)")
async def set_admin_roles(interaction: discord.Interaction, 역할들: Optional[str] = None):
    if not is_owner(interaction.user.id):
        await interaction.response.send_message("개발자만 사용할 수 있습니다.", ephemeral=True)
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message("길드에서만 사용할 수 있습니다.", ephemeral=True)
        return

    if 역할들 is None:
        set_guild_admin_role_ids(guild.id, [])
        await interaction.response.send_message(
            "관리자 역할 설정을 해제했습니다.", ephemeral=True
        )
        return

    ids = re.findall(r"\d+", 역할들)
    if not ids:
        await interaction.response.send_message(
            "역할을 멘션해서 입력하거나, 인자를 비워서 전체 해제해주세요.",
            ephemeral=True,
        )
        return

    bot_member = guild.me
    role_ids: list[int] = []
    mentions: list[str] = []

    for _id in ids:
        role = guild.get_role(int(_id))
        if not role:
            continue
        if bot_member.top_role <= role:
            await interaction.response.send_message(
                f"{role.mention} 은(는) 봇의 최상위 역할보다 위 역할이라 설정할 수 없습니다.",
                ephemeral=True,
            )
            return

        if role.id not in role_ids:
            role_ids.append(role.id)
            mentions.append(role.mention)

    set_guild_admin_role_ids(guild.id, role_ids)

    await interaction.response.send_message(
        "관리자 역할을 다음 역할들로 설정했습니다:\n" + ", ".join(mentions),
        ephemeral=True,
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

    # 인증된 유저 + 강제인증 유저 모두 포함
    cursor.execute(
        "SELECT roblox_nick FROM users WHERE guild_id=? AND verified=1",
        (interaction.guild.id,),
    )
    verified_users = [row[0] for row in cursor.fetchall() if row[0]]

    cursor.execute(
        "SELECT roblox_nick FROM forced_verified WHERE guild_id=?",
        (interaction.guild.id,),
    )
    forced_users = [row[0] for row in cursor.fetchall() if row[0]]

    all_users = list(set(verified_users + forced_users))

    if not all_users:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return

    try:
        payload = {"usernames": all_users, "rank": role_name}
        resp = requests.post(
            f"{RANK_API_URL_ROOT}/bulk-promote-to-role",
            json=payload,
            headers=_rank_api_headers(),
            timeout=120,
        )

        if resp.status_code == 200:
            data = resp.json()
            embed = discord.Embed(title=" 일괄 승진 완료", color=discord.Color.green())
            
            lines = []
            for r in data.get("results", []):
                if r.get("success"):
                    oldRole = r.get("oldRole", {})
                    newRole = r.get("newRole", {})
                    lines.append(
                        f"{r['username']}: {oldRole.get('name','?')}({oldRole.get('rank','?')}) → {newRole.get('name','?')}({newRole.get('rank','?')})"
                    )
                else:
                    lines.append(f"{r['username']}: {r.get('error','오류')}")
            
            msg = "\n".join(lines) or "결과가 없습니다."
            embed.description = msg[:2000]
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.followup.send(
                f"일괄 승진 실패 (HTTP {resp.status_code}): {resp.text}",
                ephemeral=True,
            )
    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)


@bot.tree.command(name="일괄강등", description="인증된 모든 유저를 특정 역할로 변경합니다. (관리자)")
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

    # 인증된 유저 + 강제인증 유저 모두 포함
    cursor.execute(
        "SELECT roblox_nick FROM users WHERE guild_id=? AND verified=1",
        (interaction.guild.id,),
    )
    verified_users = [row[0] for row in cursor.fetchall() if row[0]]

    cursor.execute(
        "SELECT roblox_nick FROM forced_verified WHERE guild_id=?",
        (interaction.guild.id,),
    )
    forced_users = [row[0] for row in cursor.fetchall() if row[0]]

    all_users = list(set(verified_users + forced_users))

    if not all_users:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return

    try:
        payload = {"usernames": all_users, "rank": role_name}
        resp = requests.post(
            f"{RANK_API_URL_ROOT}/bulk-demote-to-role",
            json=payload,
            headers=_rank_api_headers(),
            timeout=120,
        )

        if resp.status_code == 200:
            data = resp.json()
            embed = discord.Embed(title=" 일괄 강등 완료", color=discord.Color.red())
            
            lines = []
            for r in data.get("results", []):
                if r.get("success"):
                    oldRole = r.get("oldRole", {})
                    newRole = r.get("newRole", {})
                    lines.append(
                        f"{r['username']}: {oldRole.get('name','?')}({oldRole.get('rank','?')}) → {newRole.get('name','?')}({newRole.get('rank','?')})"
                    )
                else:
                    lines.append(f"{r['username']}: {r.get('error','오류')}")
            
            msg = "\n".join(lines) or "결과가 없습니다."
            embed.description = msg[:2000]
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.followup.send(
                f"일괄 강등 실패 (HTTP {resp.status_code}): {resp.text}",
                ephemeral=True,
            )
    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)


@bot.tree.command(name="강제인증", description="유저를 강제로 특정 role로 인증합니다. (관리자)")
@app_commands.describe(
    user="Discord 유저 멘션",
    roblox_nick="Roblox 본닉",
    rank="그룹 역할 이름 또는 숫자"
)
async def force_verify(interaction: discord.Interaction, user: discord.User, roblox_nick: str, rank: str):
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

    # 강제인증 DB에 저장
    cursor.execute(
        """INSERT OR REPLACE INTO forced_verified(discord_id, guild_id, roblox_nick, roblox_user_id, rank_role)
           VALUES(?, ?, ?, ?, ?)""",
        (user.id, interaction.guild.id, roblox_nick, user_id, rank),
    )
    conn.commit()

    embed = discord.Embed(
        title=" 강제인증 완료",
        color=discord.Color.green(),
        description=f"{user.mention} 을(를) {roblox_nick} ({rank}로 강제인증했습니다."
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="강제인증해제", description="강제인증된 유저를 제거합니다. (관리자)")
@app_commands.describe(user="Discord 유저 멘션")
async def force_unverify(interaction: discord.Interaction, user: discord.User):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    cursor.execute(
        "DELETE FROM forced_verified WHERE discord_id=? AND guild_id=?",
        (user.id, interaction.guild.id),
    )
    conn.commit()

    embed = discord.Embed(
        title="강제인증 해제 완료",
        color=discord.Color.orange(),
        description=f"{user.mention} 의 강제인증을 해제했습니다."
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="일괄닉네임변경", description="특정 role의 로블닉으로 Discord 닉네임을 일괄 변경합니다. (관리자)")
@app_commands.describe(role_name="Roblox 그룹 역할 이름")
async def bulk_nickname_change(interaction: discord.Interaction, role_name: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        # 해당 role의 모든 유저 가져오기 (일단 간단히, 실제로는 Roblox API로 role 확인 필요)
        cursor.execute(
            "SELECT discord_id, roblox_nick FROM users WHERE guild_id=? AND verified=1",
            (interaction.guild.id,),
        )
        users_data = cursor.fetchall()

        updated = 0
        failed = 0

        for discord_id, roblox_nick in users_data:
            try:
                member = interaction.guild.get_member(discord_id)
                if member:
                    await member.edit(nick=roblox_nick)
                    updated += 1
            except Exception as e:
                failed += 1

        embed = discord.Embed(
            title=" 일괄 닉네임 변경 완료",
            color=discord.Color.blue()
        )
        embed.add_field(name="성공", value=str(updated), inline=True)
        embed.add_field(name="실패", value=str(failed), inline=True)
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

@bot.tree.command(name="명단로그채널지정", description="명단 로그를 기록할 채널을 지정합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(channel="로그 채널")
async def set_rank_log_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    cursor.execute(
        """INSERT OR REPLACE INTO rank_log_settings(guild_id, channel_id, enabled)
           VALUES(?, ?, COALESCE((SELECT enabled FROM rank_log_settings WHERE guild_id=?), 0))""",
        (interaction.guild.id, channel.id, interaction.guild.id),
    )
    conn.commit()

    await interaction.response.send_message(
        f"명단 로그 채널을 {channel.mention}로 설정했습니다.",
        ephemeral=True,
    )

@bot.tree.command(name="명단로그", description="명단 로그 기능을 켜거나 끕니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(status="on 또는 off")
async def toggle_rank_log(interaction: discord.Interaction, status: str):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    if status.lower() not in ["on", "off"]:
        await interaction.response.send_message(
            "상태는 'on' 또는 'off' 만 가능합니다.", ephemeral=True
        )
        return

    enabled = 1 if status.lower() == "on" else 0

    cursor.execute(
        """INSERT OR REPLACE INTO rank_log_settings(guild_id, channel_id, enabled)
           VALUES(?, COALESCE((SELECT channel_id FROM rank_log_settings WHERE guild_id=?), 0), ?)""",
        (interaction.guild.id, interaction.guild.id, enabled),
    )
    conn.commit()

    status_text = "켜짐" if enabled else "꺼짐"
    await interaction.response.send_message(
        f"명단 로그 기능을 {status_text}으로 설정했습니다.",
        ephemeral=True,
    )

@bot.tree.command(name="그룹명단복구", description="저장된 명단 로그로부터 랭크를 복구합니다. (관리자)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(번호="복구할 로그의 일련번호")
async def restore_rank_log(interaction: discord.Interaction, 번호: int):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        # 해당 로그 찾기
        cursor.execute(
            "SELECT log_data FROM rank_log_history WHERE id=? AND guild_id=?",
            (번호, interaction.guild.id),
        )
        row = cursor.fetchone()

        if not row:
            await interaction.followup.send(
                f"일련번호가 {번호}인 로그를 찾을 수 없습니다.", ephemeral=True
            )
            return

        import json
        log_data = json.loads(row[0])

        if not log_data:
            await interaction.followup.send(
                f"로그에 복구할 데이터가 없습니다.", ephemeral=True
            )
            return

        # 모든 유저의 랭크를 저장된 상태로 복구
        results = []
        for item in log_data:
            try:
                username = item["username"]
                rank = item["rank"]  # 숫자 또는 문자열 rank

                resp = requests.post(
                    f"{RANK_API_URL_ROOT}/rank",
                    json={"username": username, "rank": rank},
                    headers=_rank_api_headers(),
                    timeout=15,
                )

                if resp.status_code == 200:
                    data = resp.json()
                    newRole = data.get("newRole", {})
                    results.append(
                        f"{username}: {newRole.get('name', '?')} (rank {newRole.get('rank', '?')})"
                    )
                else:
                    results.append(f"{username}: HTTP {resp.status_code}")

            except Exception as e:
                results.append(f"{username}: {str(e)}")

        msg = "\n".join(results)
        embed = discord.Embed(
            title="명단 복구 완료",
            description=msg[:2000],
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=f"일련번호: {번호}")
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ 복구 중 에러 발생: {e}", ephemeral=True)

@tasks.loop(minutes=5)
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
                    "SELECT roblox_nick, roblox_user_id FROM users WHERE guild_id=? AND verified=1",
                    (guild_id,),
                )
                users = cursor.fetchall()

                if not users:
                    continue

                usernames = [u[0] for u in users]
                
                try:
                    resp = requests.post(
                        f"{RANK_API_URL_ROOT}/bulk-promote",
                        json={"usernames": usernames},
                        headers=_rank_api_headers(),
                        timeout=30,
                    )

                    if resp.status_code == 200:
                        data = resp.json()
                        lines = []
                        log_data = []  # 복구용 데이터
                        
                        for r in data.get("results", []):
                            if r.get("success"):
                                newRole = r.get("newRole", {})
                                lines.append(
                                    f"{r['username']}: {newRole.get('name', '?')} (rank {newRole.get('rank', '?')})"
                                )
                                # 복구용 데이터 저장
                                log_data.append({
                                    "username": r['username'],
                                    "rank": newRole.get('rank', '?'),
                                    "rank_name": newRole.get('name', '?')
                                })
                            else:
                                lines.append(f"{r['username']}: 오류 - {r.get('error', '불명')}")

                        if lines:
                            # DB에 로그 저장
                            import json
                            cursor.execute(
                                "INSERT INTO rank_log_history(guild_id, log_data, created_at) VALUES(?, ?, ?)",
                                (guild_id, json.dumps(log_data), datetime.now().isoformat()),
                            )
                            conn.commit()
                            
                            # 일련번호 가져오기
                            cursor.execute(
                                "SELECT id FROM rank_log_history WHERE guild_id=? ORDER BY id DESC LIMIT 1",
                                (guild_id,),
                            )
                            log_id = cursor.fetchone()[0]
                            
                            msg = "\n".join(lines)
                            embed = discord.Embed(
                                title="명단 로그",
                                description=msg[:2000],
                                color=discord.Color.blue(),
                                timestamp=datetime.now(timezone.utc),
                            )
                            embed.set_footer(text=f"일련번호: {log_id}")
                            await channel.send(embed=embed)
                except Exception as e:
                    print(f"rank_log_task API error: {e}")

            except Exception as e:
                print(f"rank_log_task error for guild {guild_id}: {e}")

    except Exception as e:
        print(f"rank_log_task error: {e}")

# ---------- 봇 시작 ----------
@bot.event
async def on_ready():
    try:
        if GUILD_ID != 0:
            guild = discord.Object(id=GUILD_ID)
            synced = await bot.tree.sync(guild=guild)
            print(f"Synced {len(synced)} commands to guild.")
        else:
            synced = await bot.tree.sync()
            print(f"Synced {len(synced)} commands globally.")
    except Exception as e:
        print("동기화 실패:", e)

    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

    if not rank_log_task.is_running():
        rank_log_task.start()

