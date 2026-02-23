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
from discord.ext import commands
from dotenv import load_dotenv
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

TOKEN = str(os.getenv("DISCORD_TOKEN"))
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

RANK_API_URL_ROOT = os.getenv("RANK_API_URL_ROOT")
RANK_API_KEY = os.getenv("RANK_API_KEY")

CREATOR_ROBLOX_NICK = "DeSky_Lunarx"
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

# ---------- DB ----------
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


ROBLOX_USERNAME_API = "https://users.roblox.com/v1/usernames/users"
ROBLOX_USER_API = "https://users.roblox.com/v1/users/{userId}"

# ---------- Roblox API ----------


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


# ---------- 공용 ----------


def get_verified_users_in_guild(guild_id: int):
    cursor.execute(
        "SELECT discord_id, roblox_nick, roblox_user_id FROM users WHERE guild_id=? AND verified=1",
        (guild_id,),
    )
    return cursor.fetchall()


def _rank_api_headers():
    return {"Content-Type": "application/json", "X-API-KEY": RANK_API_KEY}


# ---------- 명령어 ----------


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

    code = generate_code()
    expire_time = datetime.now() + timedelta(minutes=5)

    cursor.execute(
        """INSERT OR REPLACE INTO users(discord_id, guild_id, roblox_nick,
           roblox_user_id, code, expire_time, verified)
           VALUES(?,?,?,?,?,?,0)""",
        (interaction.user.id, interaction.guild.id, 로블닉, user_id, code, expire_time.isoformat()),
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
                f"강등 실패 (HTTP {resp.status_code}): {resp.text}",
                ephemeral=True,
            )
    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)


@bot.tree.command(
    name="일괄승진",
    description="이 서버에서 인증된 모든 유저를 한 단계 승진합니다. (관리자)",
)
async def bulk_promote_verified(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    if not RANK_API_URL_ROOT or not RANK_API_KEY:
        await interaction.response.send_message(
            "랭킹 서버 설정이 되어 있지 않습니다.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    users_data = get_verified_users_in_guild(interaction.guild.id)
    if not users_data:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return

    username_list = [row[1] for row in users_data if row[1]]
    if not username_list:
        await interaction.followup.send("인증된 유저들의 로블록스 닉네임 정보가 없습니다.", ephemeral=True)
        return

    try:
        payload = {"usernames": username_list}
        resp = requests.post(
            f"{RANK_API_URL_ROOT}/bulk-promote",
            json=payload,
            headers=_rank_api_headers(),
            timeout=120,
        )

        if resp.status_code == 200:
            data = resp.json()
            lines = []
            for r in data.get("results", []):
                if r.get("success"):
                    oldRole = r.get("oldRole", {})
                    newRole = r.get("newRole", {})
                    lines.append(
                        f"{r['username']}: "
                        f"{oldRole.get('name','?')}({oldRole.get('rank','?')}) → "
                        f"{newRole.get('name','?')}({newRole.get('rank','?')})"
                    )
                else:
                    lines.append(f"{r['username']}: {r.get('error','오류')}")
            msg = "\n".join(lines) or "결과가 없습니다."
            await interaction.followup.send(msg[:1900], ephemeral=True)
        else:
            await interaction.followup.send(
                f"일괄 승진 실패 (HTTP {resp.status_code}): {resp.text}",
                ephemeral=True,
            )
    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)


@bot.tree.command(
    name="일괄강등",
    description="이 서버에서 인증된 모든 유저를 한 단계 강등합니다. (관리자)",
)
async def bulk_demote_verified(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message("관리자만 사용할 수 있습니다.", ephemeral=True)
        return

    if not RANK_API_URL_ROOT or not RANK_API_KEY:
        await interaction.response.send_message(
            "랭킹 서버 설정이 되어 있지 않습니다.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    users_data = get_verified_users_in_guild(interaction.guild.id)
    if not users_data:
        await interaction.followup.send("인증된 유저가 없습니다.", ephemeral=True)
        return

    username_list = [row[1] for row in users_data if row[1]]
    if not username_list:
        await interaction.followup.send("인증된 유저들의 로블록스 닉네임 정보가 없습니다.", ephemeral=True)
        return

    try:
        payload = {"usernames": username_list}
        resp = requests.post(
            f"{RANK_API_URL_ROOT}/bulk-demote",
            json=payload,
            headers=_rank_api_headers(),
            timeout=120,
        )

        if resp.status_code == 200:
            data = resp.json()
            lines = []
            for r in data.get("results", []):
                if r.get("success"):
                    oldRole = r.get("oldRole", {})
                    newRole = r.get("newRole", {})
                    lines.append(
                        f" {r['username']}: "
                        f"{oldRole.get('name','?')}({oldRole.get('rank','?')}) → "
                        f"{newRole.get('name','?')}({newRole.get('rank','?')})"
                    )
                else:
                    lines.append(f"{r['username']}: {r.get('error','오류')}")
            msg = "\n".join(lines) or "결과가 없습니다."
            await interaction.followup.send(msg[:1900], ephemeral=True)
        else:
            await interaction.followup.send(
                f"일괄 강등 실패 (HTTP {resp.status_code}): {resp.text}",
                ephemeral=True,
            )
    except Exception as e:
        await interaction.followup.send(f"요청 중 에러 발생: {e}", ephemeral=True)


@bot.event
async def on_ready():
    try:
        await bot.tree.sync()
    except Exception as e:
        print("동기화 실패:", e)
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")


bot.run(TOKEN)