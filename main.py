import discord
from discord import app_commands
import os
import libsql
import time
import re
import asyncio
import traceback
from datetime import datetime, timezone, timedelta

# ============================================================
# PANDEMONIUM DISCORD BOT
# Python + discord.py + Turso/libSQL
#
# Railway environment variables required:
#   DISCORD_TOKEN
#   TURSO_DATABASE_URL
#   TURSO_AUTH_TOKEN
#
# Fill the CONFIG placeholders below before deploying.
# ============================================================

# =========================
# BASIC CONFIGURATION
# =========================

BOT_NAME = "Pandemonium"
EMBED_COLOR = discord.Color(0x8B0000)

# Single-server configuration.
GUILD_ID =1543983137959190538 # TODO: Put Pandemonium's Discord server ID here.

# Event manager role. Administrators always bypass this check.
EVENT_MANAGER_ROLE_ID = 1543984184060543056  # TODO: Put the Event Manager role ID here.

# Channel where event warnings / live announcements are posted.
EVENT_CHANNEL_ID = 1543983138634735618  # TODO: Put the event announcement channel ID here.

# Channel where errors are logged. 0 disables Discord error logging.
ERROR_LOG_CHANNEL_ID = 1543983138634735618  # TODO: Put an error-log channel ID here.

# Ticket system.
# New ticket channels are created inside this category.
TICKET_CATEGORY_ID = 1543983138634735616  # TODO: Put the ticket category ID here.

# Optional role that can see/manage every ticket.
TICKET_MANAGER_ROLE_ID = 1543984184060543056  # TODO: Put the ticket manager role ID here.

# Ticket staff roles for each category.
# Add role IDs when you are ready.
TICKET_ROLE_IDS = {
    "Alliance Support": 1543984184060543056,
    "Recruitment": 1543984184060543056,
    "Report a Member": 1543984184060543056,
    "Other": 1543984184060543056,
}

# How often the scheduler checks Turso for due events/reminders.
SCHEDULER_INTERVAL = 15

# =========================
# DISCORD CLIENT
# =========================

intents = discord.Intents.default()
intents.members = True

client = discord.Client(
    intents=intents,
    chunk_guilds_at_startup=True,
)

tree = app_commands.CommandTree(client)

# =========================
# DATABASE
# =========================

db = None


def _connect_db():
    global db
    db = libsql.connect(
        database=os.environ["TURSO_DATABASE_URL"],
        auth_token=os.environ["TURSO_AUTH_TOKEN"],
    )


def _is_stream_error(error):
    text = str(error)
    return "stream not found" in text or "Hrana" in text


def _safe_params(params):
    """
    Discord snowflake IDs can exceed 2**53. Turso's remote transport can
    otherwise lose precision if a large Python int is sent through a numeric
    path. Convert large integers to decimal strings before binding.
    """
    return tuple(
        str(value)
        if isinstance(value, int)
        and not isinstance(value, bool)
        and abs(value) > 2**53
        else value
        for value in params
    )


class ResilientCursor:
    def __init__(self):
        self._cursor = db.cursor()
        self._lock = asyncio.Lock()

    def _refresh(self):
        print("[DB] Reconnecting after a stale Turso/Hrana stream.")
        _connect_db()
        self._cursor = db.cursor()

    def execute(self, query, params=()):
        params = _safe_params(params)
        try:
            return self._cursor.execute(query, params)
        except Exception as error:
            if _is_stream_error(error):
                self._refresh()
                return self._cursor.execute(query, params)
            raise

    def _execute_and_fetch(self, query, params, fetch, commit):
        params = _safe_params(params)

        last_error = None

        for _ in range(2):
            try:
                self._cursor.execute(query, params)

                if commit:
                    db.commit()

                if fetch == "one":
                    return self._cursor.fetchone()

                if fetch == "all":
                    return self._cursor.fetchall()

                return None

            except Exception as error:
                last_error = error

                if _is_stream_error(error):
                    self._refresh()
                    continue

                raise

        raise last_error

    async def aexecute(self, query, params=(), fetch=None, commit=False):
        async with self._lock:
            return await asyncio.to_thread(
                self._execute_and_fetch,
                query,
                params,
                fetch,
                commit,
            )


_connect_db()
cursor = ResilientCursor()


def db_commit():
    try:
        db.commit()
    except Exception as error:
        if _is_stream_error(error):
            cursor._refresh()
        else:
            raise


# =========================
# DATABASE MIGRATIONS
# =========================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        message_id INTEGER,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        event_time INTEGER NOT NULL,
        created_by INTEGER NOT NULL,
        thumbnail_url TEXT,
        warning_sent INTEGER DEFAULT 0,
        live_sent INTEGER DEFAULT 0,
        status TEXT DEFAULT 'scheduled',
        created_at INTEGER NOT NULL
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS event_attendees (
        event_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        notifications_enabled INTEGER DEFAULT 1,
        joined_at INTEGER NOT NULL,
        PRIMARY KEY (event_id, user_id)
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS event_votes (
        vote_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        option_index INTEGER NOT NULL,
        voted_at INTEGER NOT NULL,
        PRIMARY KEY (vote_id, user_id)
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS votes (
        vote_id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        message_id INTEGER,
        question TEXT NOT NULL,
        options TEXT NOT NULL,
        end_time INTEGER NOT NULL,
        created_by INTEGER NOT NULL,
        status TEXT DEFAULT 'open',
        created_at INTEGER NOT NULL
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS reminders (
        reminder_id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        remind_at INTEGER NOT NULL,
        sent INTEGER DEFAULT 0,
        created_at INTEGER NOT NULL
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS tickets (
        channel_id INTEGER PRIMARY KEY,
        guild_id INTEGER NOT NULL,
        opener_id INTEGER NOT NULL,
        ticket_type TEXT NOT NULL,
        claimed_by INTEGER,
        status TEXT DEFAULT 'open',
        important INTEGER DEFAULT 0,
        pinned INTEGER DEFAULT 0,
        base_name TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """
)

db_commit()

# =========================
# UTILITY FUNCTIONS
# =========================


def now_ts():
    return int(time.time())


def guild_is_configured():
    return GUILD_ID != 0


def is_admin(member):
    return bool(member.guild_permissions.administrator)


def is_event_manager(member):
    if is_admin(member):
        return True

    if EVENT_MANAGER_ROLE_ID == 0:
        return False

    return any(role.id == EVENT_MANAGER_ROLE_ID for role in member.roles)


def is_ticket_manager(member):
    if is_admin(member):
        return True

    if TICKET_MANAGER_ROLE_ID == 0:
        return False

    return any(role.id == TICKET_MANAGER_ROLE_ID for role in member.roles)


def parse_duration(text):
    """
    Supports:
      10s
      5m
      2h
      1d
      2h 30m
      1h 20m 15s
    """
    if not text:
        return None

    text = text.strip().lower()

    pattern = re.compile(r"(\d+)\s*([smhd])")
    matches = pattern.findall(text)

    if not matches:
        return None

    consumed = "".join(f"{value}{unit}" for value, unit in matches)
    normalized = re.sub(r"\s+", "", text)

    if consumed != normalized:
        return None

    units = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
    }

    total = sum(int(value) * units[unit] for value, unit in matches)

    return total if total > 0 else None


def format_duration(seconds):
    seconds = int(seconds)

    parts = []

    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)


def parse_utc_time_to_unix(text):
    """
    Accepts a 24-hour UTC clock time, e.g. "20:40" or "8:40".
    Resolves it to the next occurrence of that time (today if it
    hasn't happened yet, otherwise tomorrow) and returns a unix
    timestamp. Returns None if the format is invalid.
    """
    if not text:
        return None

    text = text.strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", text)

    if not match:
        return None

    hour = int(match.group(1))
    minute = int(match.group(2))

    now = datetime.now(timezone.utc)
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if candidate <= now:
        candidate += timedelta(days=1)

    return int(candidate.timestamp())


def valid_thumbnail_url(url):
    if not url:
        return False

    return url.startswith(("http://", "https://"))


async def safe_call(coro, context):
    try:
        return await coro, None

    except discord.Forbidden:
        return None, "I don't have permission to do that."

    except discord.NotFound as error:
        await log_error(context, error)
        return None, "That Discord resource could not be found."

    except discord.HTTPException as error:
        await log_error(context, error)
        return None, "Discord rejected that request. The error has been logged."

    except Exception as error:
        await log_error(context, error)
        return None, "Something went wrong. The error has been logged."


async def log_error(context, error):
    print(f"[ERROR] {context}: {error}")
    traceback.print_exc()

    if ERROR_LOG_CHANNEL_ID == 0:
        return

    try:
        channel = client.get_channel(ERROR_LOG_CHANNEL_ID)

        if not channel:
            return

        embed = discord.Embed(
            title="⚠️ Pandemonium Bot Error",
            description=f"**Context:** {context}\n**Error:** `{error}`",
            color=discord.Color.red(),
        )

        embed.timestamp = discord.utils.utcnow()

        traceback_text = traceback.format_exc().strip()

        if len(traceback_text) > 900:
            traceback_text = traceback_text[-900:]

        embed.add_field(
            name="Traceback",
            value=f"```{traceback_text}```",
            inline=False,
        )

        await channel.send(embed=embed)

    except Exception as inner:
        print(f"[ERROR] Failed to send error log: {inner}")


def make_embed(title, description=None, color=None):
    embed = discord.Embed(
        title=title,
        description=description or "",
        color=color or EMBED_COLOR,
    )

    embed.timestamp = discord.utils.utcnow()

    return embed


# ============================================================
# EVENT SYSTEM
# ============================================================


def build_event_embed(row):
    (
        event_id,
        guild_id,
        channel_id,
        message_id,
        title,
        description,
        event_time,
        created_by,
        thumbnail_url,
        warning_sent,
        live_sent,
        status,
        created_at,
    ) = row

    embed = discord.Embed(
        title=f"⚔️ {title}",
        description=description,
        color=EMBED_COLOR,
    )

    embed.add_field(
        name="🕐 Event Time",
        value=f"<t:{event_time}:F>\n<t:{event_time}:R>",
        inline=False,
    )

    embed.add_field(
        name="🆔 Event ID",
        value=f"`EVT-{event_id:04d}`",
        inline=True,
    )

    if status == "scheduled":
        status_text = "🟢 Scheduled"
    elif status == "live":
        status_text = "🔴 Live"
    else:
        status_text = "⚫ Finished"

    embed.add_field(
        name="Status",
        value=status_text,
        inline=True,
    )

    if thumbnail_url:
        embed.set_image(url=thumbnail_url)

    embed.set_footer(
        text=f"Event ID: EVT-{event_id:04d} • Created by {created_by}"
    )

    return embed


async def get_event(event_id):
    return await cursor.aexecute(
        """
        SELECT event_id, guild_id, channel_id, message_id, title,
               description, event_time, created_by, thumbnail_url,
               warning_sent, live_sent, status, created_at
        FROM events
        WHERE event_id = ?
        """,
        (event_id,),
        fetch="one",
    )


async def update_event_message(event_id):
    row = await get_event(event_id)

    if not row:
        return

    message_id = row[3]
    channel_id = row[2]

    if not message_id:
        return

    channel = client.get_channel(channel_id)

    if not channel:
        return

    try:
        message = await channel.fetch_message(message_id)

        await message.edit(
            embed=build_event_embed(row),
            view=EventNotificationView(event_id),
        )

    except discord.NotFound:
        pass

    except Exception as error:
        await log_error("update_event_message", error)


async def get_attendee_count(event_id):
    row = await cursor.aexecute(
        "SELECT COUNT(*) FROM event_attendees WHERE event_id = ?",
        (event_id,),
        fetch="one",
    )

    return int(row[0]) if row else 0


async def get_notification_user_ids(event_id):
    rows = await cursor.aexecute(
        """
        SELECT user_id
        FROM event_attendees
        WHERE event_id = ?
          AND notifications_enabled = 1
        """,
        (event_id,),
        fetch="all",
    )

    return [int(row[0]) for row in rows]


async def send_event_dm(user_id, embed):
    try:
        user = client.get_user(user_id)

        if user is None:
            user = await client.fetch_user(user_id)

        await user.send(embed=embed)

    except discord.Forbidden:
        # User has DMs disabled. This should not break the scheduler.
        pass

    except discord.NotFound:
        pass

    except Exception as error:
        await log_error(f"send_event_dm user={user_id}", error)


async def send_event_notification(event_row, live=False):
    (
        event_id,
        guild_id,
        channel_id,
        message_id,
        title,
        description,
        event_time,
        created_by,
        thumbnail_url,
        warning_sent,
        live_sent,
        status,
        created_at,
    ) = event_row

    channel = client.get_channel(channel_id)

    if live:
        embed = make_embed(
            "🚨 EVENT IS LIVE",
            f"**{title}** is now starting!\n\n{description}",
            discord.Color.red(),
        )
    else:
        embed = make_embed(
            "⚠️ Event Reminder",
            f"**{title}** begins in **10 minutes**.\n\n"
            f"🕐 <t:{event_time}:F>\n"
            f"🆔 `EVT-{event_id:04d}`",
            EMBED_COLOR,
        )

    if thumbnail_url:
        embed.set_image(url=thumbnail_url)

    user_ids = await get_notification_user_ids(event_id)

    mentions = []

    for user_id in user_ids:
        member = client.get_user(user_id)

        if member:
            mentions.append(member.mention)

        await send_event_dm(user_id, embed)

    if channel:
        if mentions:
            # Keep the mention message under Discord's message limit.
            content = " ".join(mentions)

            if len(content) > 1900:
                content = content[:1897] + "..."

            await safe_call(
                channel.send(content=content, embed=embed),
                "send_event_notification channel send",
            )
        else:
            await safe_call(
                channel.send(embed=embed),
                "send_event_notification channel send",
            )


class EventNotificationView(discord.ui.View):
    def __init__(self, event_id):
        super().__init__(timeout=None)
        self.event_id = event_id

        button = discord.ui.Button(
            label="🔔 Get Notification",
            style=discord.ButtonStyle.primary,
            custom_id=f"event_notify:{event_id}",
        )

        button.callback = self.toggle_notification
        self.add_item(button)

    async def toggle_notification(self, interaction):
        row = await get_event(self.event_id)

        if not row:
            await interaction.response.send_message(
                "This event no longer exists.",
                ephemeral=True,
            )
            return

        event_time = int(row[6])
        status = row[11]

        if status != "scheduled" or event_time <= now_ts():
            await interaction.response.send_message(
                "This event has already started or finished.",
                ephemeral=True,
            )
            return

        attendee = await cursor.aexecute(
            """
            SELECT notifications_enabled
            FROM event_attendees
            WHERE event_id = ? AND user_id = ?
            """,
            (self.event_id, interaction.user.id),
            fetch="one",
        )

        if attendee is None:
            async def write_join():
                await cursor.aexecute(
                    """
                    INSERT INTO event_attendees
                    (event_id, user_id, notifications_enabled, joined_at)
                    VALUES (?, ?, 1, ?)
                    """,
                    (self.event_id, interaction.user.id, now_ts()),
                    commit=True,
                )

            await write_join()

            await interaction.response.send_message(
                "✅ **Entry Confirmed**\n\n"
                "You are registered for this event and will receive:\n"
                "• A warning 10 minutes before it starts\n"
                "• A notification when it goes live",
                ephemeral=True,
            )

            return

        if int(attendee[0]) == 1:
            await interaction.response.send_message(
                "⚠️ **Disable Event Notifications?**\n\n"
                "You will no longer receive notifications for this event.",
                view=ConfirmLeaveEventView(self.event_id),
                ephemeral=True,
            )
            return

        async def write_enable():
            await cursor.aexecute(
                """
                UPDATE event_attendees
                SET notifications_enabled = 1
                WHERE event_id = ? AND user_id = ?
                """,
                (self.event_id, interaction.user.id),
                commit=True,
            )

        await write_enable()

        await interaction.response.send_message(
            "🔔 **Notifications Enabled**\n\n"
            "You will receive the 10-minute warning and the live notification.",
            ephemeral=True,
        )


class ConfirmLeaveEventView(discord.ui.View):
    def __init__(self, event_id):
        super().__init__(timeout=60)
        self.event_id = event_id

    @discord.ui.button(
        label="Confirm",
        style=discord.ButtonStyle.danger,
        emoji="🔕",
    )
    async def confirm(self, interaction, button):
        async def write_disable():
            await cursor.aexecute(
                """
                UPDATE event_attendees
                SET notifications_enabled = 0
                WHERE event_id = ? AND user_id = ?
                """,
                (self.event_id, interaction.user.id),
                commit=True,
            )

        await write_disable()

        await interaction.response.edit_message(
            content="🔕 **Notifications Disabled**\n\n"
                    "You will no longer receive notifications for this event.",
            view=None,
        )

    @discord.ui.button(
        label="Cancel",
        style=discord.ButtonStyle.secondary,
        emoji="✖️",
    )
    async def cancel(self, interaction, button):
        await interaction.response.edit_message(
            content="Cancelled. Your event notification setting was not changed.",
            view=None,
        )


# =========================
# EVENT COMMANDS
# =========================

event_group = app_commands.Group(
    name="event",
    description="Pandemonium event management and voting",
)


@event_group.command(
    name="create",
    description="Create a scheduled alliance event",
)
@app_commands.describe(
    title="Event title",
    description="Event description",
    time_utc="Event start time in UTC, 24-hour HH:MM (e.g. 20:40). Rolls to tomorrow if that time already passed today.",
    thumbnail="Optional large image shown below the event embed",
)
async def event_create(
    interaction: discord.Interaction,
    title: str,
    description: str,
    time_utc: str,
    thumbnail: discord.Attachment = None,
):
    if not is_event_manager(interaction.user):
        await interaction.response.send_message(
            "You don't have permission to create events.",
            ephemeral=True,
        )
        return

    time_unix = parse_utc_time_to_unix(time_utc)

    if time_unix is None:
        await interaction.response.send_message(
            "Invalid time format. Use 24-hour UTC time like `20:40`.",
            ephemeral=True,
        )
        return

    if time_unix <= now_ts():
        await interaction.response.send_message(
            "The event time must be in the future.",
            ephemeral=True,
        )
        return

    if len(title) > 256:
        await interaction.response.send_message(
            "The title is too long. Keep it under 256 characters.",
            ephemeral=True,
        )
        return

    if len(description) > 4000:
        await interaction.response.send_message(
            "The description is too long. Keep it under 4000 characters.",
            ephemeral=True,
        )
        return

    thumbnail_url = thumbnail.url if thumbnail else None

    if thumbnail_url and not valid_thumbnail_url(thumbnail_url):
        thumbnail_url = None

    channel = (
        client.get_channel(EVENT_CHANNEL_ID)
        if EVENT_CHANNEL_ID
        else interaction.channel
    )

    if channel is None:
        await interaction.response.send_message(
            "The configured event channel could not be found.",
            ephemeral=True,
        )
        return

    created_at = now_ts()

    async def write_event():
        await cursor.aexecute(
            """
            INSERT INTO events
            (guild_id, channel_id, message_id, title, description,
             event_time, created_by, thumbnail_url, warning_sent,
             live_sent, status, created_at)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, 0, 0, 'scheduled', ?)
            """,
            (
                interaction.guild.id,
                channel.id,
                title,
                description,
                time_unix,
                interaction.user.id,
                thumbnail_url,
                created_at,
            ),
            commit=True,
        )

    await write_event()

    event_id_row = await cursor.aexecute(
        """
        SELECT event_id
        FROM events
        WHERE guild_id = ?
          AND created_by = ?
          AND created_at = ?
        ORDER BY event_id DESC
        LIMIT 1
        """,
        (
            interaction.guild.id,
            interaction.user.id,
            created_at,
        ),
        fetch="one",
    )

    if not event_id_row:
        await interaction.response.send_message(
            "The event was saved, but I couldn't retrieve its ID.",
            ephemeral=True,
        )
        return

    event_id = int(event_id_row[0])

    row = await get_event(event_id)

    message, error = await safe_call(
        channel.send(
            embed=build_event_embed(row),
            view=EventNotificationView(event_id),
        ),
        "event_create channel send",
    )

    if error:
        await interaction.response.send_message(error, ephemeral=True)
        return

    async def write_message_id():
        await cursor.aexecute(
            "UPDATE events SET message_id = ? WHERE event_id = ?",
            (message.id, event_id),
            commit=True,
        )

    await write_message_id()

    await interaction.response.send_message(
        f"✅ Event created successfully.\n"
        f"**Event ID:** `EVT-{event_id:04d}`\n"
        f"**Posted:** {message.jump_url}",
        ephemeral=True,
    )


@event_group.command(
    name="management",
    description="View who joined an event",
)
@app_commands.describe(
    event_id="Event ID, for example EVT-0001 or 1",
)
async def event_management(
    interaction: discord.Interaction,
    event_id: str,
):
    if not is_event_manager(interaction.user):
        await interaction.response.send_message(
            "You don't have permission to use event management.",
            ephemeral=True,
        )
        return

    event_id = event_id.strip().upper().replace("EVT-", "")

    try:
        numeric_id = int(event_id)
    except ValueError:
        await interaction.response.send_message(
            "Invalid event ID. Example: `EVT-0001`.",
            ephemeral=True,
        )
        return

    row = await get_event(numeric_id)

    if not row:
        await interaction.response.send_message(
            "That event could not be found.",
            ephemeral=True,
        )
        return

    attendees = await cursor.aexecute(
        """
        SELECT user_id, notifications_enabled, joined_at
        FROM event_attendees
        WHERE event_id = ?
        ORDER BY joined_at ASC
        """,
        (numeric_id,),
        fetch="all",
    )

    embed = make_embed(
        f"📋 Event Management — EVT-{numeric_id:04d}",
        f"**{row[4]}**\n\n"
        f"🕐 <t:{row[6]}:F>\n"
        f"📊 Status: **{row[11].title()}**\n"
        f"👥 Attendees: **{len(attendees)}**",
    )

    if not attendees:
        embed.add_field(
            name="Participants",
            value="No one has joined this event yet.",
            inline=False,
        )

    else:
        lines = []

        for index, attendee in enumerate(attendees, start=1):
            user_id = int(attendee[0])
            notifications = bool(attendee[1])

            member = interaction.guild.get_member(user_id)

            if member:
                name = f"{member.display_name} ({member.mention})"
            else:
                name = f"<@{user_id}>"

            notification_icon = "🔔" if notifications else "🔕"

            lines.append(
                f"**{index}.** {name} {notification_icon}"
            )

        # Discord embed fields have a 1024-character limit.
        chunks = []
        current = ""

        for line in lines:
            if len(current) + len(line) + 1 > 1000:
                chunks.append(current)
                current = line
            else:
                current += ("\n" if current else "") + line

        if current:
            chunks.append(current)

        for index, chunk in enumerate(chunks[:10], start=1):
            embed.add_field(
                name="Participants" if index == 1 else f"Participants — Page {index}",
                value=chunk,
                inline=False,
            )

        if len(chunks) > 10:
            embed.add_field(
                name="Note",
                value=f"Showing the first {min(len(attendees), 100)} attendees.",
                inline=False,
            )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
    )


# =========================
# EVENT VOTING
# =========================

def parse_vote_options(options_text):
    raw_options = [
        option.strip()
        for option in options_text.split("|")
        if option.strip()
    ]

    # Remove duplicate options while preserving order.
    unique = []

    for option in raw_options:
        if option.casefold() not in {x.casefold() for x in unique}:
            unique.append(option)

    if len(unique) < 2:
        return None, "You need at least 2 options."

    if len(unique) > 10:
        return None, "You can have a maximum of 10 options."

    for option in unique:
        if len(option) > 100:
            return None, "Each option must be 100 characters or fewer."

    return unique, None


async def get_vote(vote_id):
    return await cursor.aexecute(
        """
        SELECT vote_id, guild_id, channel_id, message_id, question,
               options, end_time, created_by, status, created_at
        FROM votes
        WHERE vote_id = ?
        """,
        (vote_id,),
        fetch="one",
    )


async def get_vote_counts(vote_id, option_count):
    counts = [0] * option_count

    rows = await cursor.aexecute(
        """
        SELECT option_index, COUNT(*)
        FROM event_votes
        WHERE vote_id = ?
        GROUP BY option_index
        """,
        (vote_id,),
        fetch="all",
    )

    for option_index, count in rows:
        option_index = int(option_index)

        if 0 <= option_index < option_count:
            counts[option_index] = int(count)

    return counts


def build_vote_embed(row, counts):
    (
        vote_id,
        guild_id,
        channel_id,
        message_id,
        question,
        options_text,
        end_time,
        created_by,
        status,
        created_at,
    ) = row

    options = options_text.split("\n")

    lines = []

    for index, option in enumerate(options):
        count = counts[index]

        if status == "open":
            lines.append(
                f"**{index + 1}.** {option} — **{count}** vote"
                f"{'s' if count != 1 else ''}"
            )
        else:
            lines.append(
                f"**{index + 1}.** {option} — **{count}** vote"
                f"{'s' if count != 1 else ''}"
            )

    if status == "open":
        footer = f"Voting ends <t:{end_time}:R> • Vote ID: VOTE-{vote_id:04d}"
    else:
        footer = f"Voting closed • Vote ID: VOTE-{vote_id:04d}"

    embed = discord.Embed(
        title=f"🗳️ {question}",
        description="\n".join(lines),
        color=EMBED_COLOR if status == "open" else discord.Color.dark_grey(),
    )

    embed.add_field(
        name="⏳ Deadline",
        value=f"<t:{end_time}:F>\n<t:{end_time}:R>",
        inline=False,
    )

    embed.set_footer(text=footer)

    return embed


class VoteView(discord.ui.View):
    def __init__(self, vote_id, options):
        super().__init__(timeout=None)
        self.vote_id = vote_id

        select_options = []

        for index, option in enumerate(options):
            select_options.append(
                discord.SelectOption(
                    label=option[:100],
                    value=str(index),
                    emoji=str(index + 1) + "\uFE0F\u20E3",
                )
            )

        select = discord.ui.Select(
            placeholder="Select your vote...",
            options=select_options,
            custom_id=f"vote_select:{vote_id}",
        )

        async def callback(interaction):
            row = await get_vote(vote_id)

            if not row:
                await interaction.response.send_message(
                    "This vote no longer exists.",
                    ephemeral=True,
                )
                return

            if row[8] != "open" or int(row[6]) <= now_ts():
                await interaction.response.send_message(
                    "Voting has ended.",
                    ephemeral=True,
                )
                return

            option_index = int(select.values[0])

            if option_index < 0 or option_index >= len(options):
                await interaction.response.send_message(
                    "Invalid vote option.",
                    ephemeral=True,
                )
                return

            async def write_vote():
                await cursor.aexecute(
                    """
                    INSERT INTO event_votes
                    (vote_id, user_id, option_index, voted_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(vote_id, user_id)
                    DO UPDATE SET
                        option_index = excluded.option_index,
                        voted_at = excluded.voted_at
                    """,
                    (
                        vote_id,
                        interaction.user.id,
                        option_index,
                        now_ts(),
                    ),
                    commit=True,
                )

            await write_vote()

            counts = await get_vote_counts(vote_id, len(options))

            # Update the public vote message immediately.
            try:
                channel = interaction.channel
                message = interaction.message

                await message.edit(
                    embed=build_vote_embed(row, counts),
                    view=VoteView(vote_id, options),
                )
            except Exception as error:
                await log_error("vote callback message edit", error)

            await interaction.response.send_message(
                f"✅ Vote recorded: **{options[option_index]}**\n\n"
                "You can change your vote any time before the vote ends.",
                ephemeral=True,
            )

        select.callback = callback
        self.add_item(select)


@event_group.command(
    name="vote",
    description="Create a vote with multiple options",
)
@app_commands.describe(
    question="What are members voting on?",
    duration="How long voting stays open, e.g. 2h or 30m",
    options="Options separated with |, e.g. Friday | Saturday | Sunday",
)
async def event_vote(
    interaction: discord.Interaction,
    question: str,
    duration: str,
    options: str,
):
    if not is_event_manager(interaction.user):
        await interaction.response.send_message(
            "You don't have permission to create votes.",
            ephemeral=True,
        )
        return

    seconds = parse_duration(duration)

    if seconds is None:
        await interaction.response.send_message(
            "Invalid duration. Examples: `30m`, `2h`, `1d`, `2h 30m`.",
            ephemeral=True,
        )
        return

    if seconds < 10:
        await interaction.response.send_message(
            "Voting must stay open for at least 10 seconds.",
            ephemeral=True,
        )
        return

    if seconds > 30 * 86400:
        await interaction.response.send_message(
            "Voting cannot stay open for more than 30 days.",
            ephemeral=True,
        )
        return

    if len(question) > 256:
        await interaction.response.send_message(
            "The question must be 256 characters or fewer.",
            ephemeral=True,
        )
        return

    parsed_options, error = parse_vote_options(options)

    if error:
        await interaction.response.send_message(
            error,
            ephemeral=True,
        )
        return

    end_time = now_ts() + seconds
    created_at = now_ts()

    options_db = "\n".join(parsed_options)

    async def write_vote():
        await cursor.aexecute(
            """
            INSERT INTO votes
            (guild_id, channel_id, message_id, question, options,
             end_time, created_by, status, created_at)
            VALUES (?, ?, NULL, ?, ?, ?, ?, 'open', ?)
            """,
            (
                interaction.guild.id,
                interaction.channel.id,
                question,
                options_db,
                end_time,
                interaction.user.id,
                created_at,
            ),
            commit=True,
        )

    await write_vote()

    vote_id_row = await cursor.aexecute(
        """
        SELECT vote_id
        FROM votes
        WHERE guild_id = ?
          AND created_by = ?
          AND created_at = ?
        ORDER BY vote_id DESC
        LIMIT 1
        """,
        (
            interaction.guild.id,
            interaction.user.id,
            created_at,
        ),
        fetch="one",
    )

    if not vote_id_row:
        await interaction.response.send_message(
            "The vote was saved, but I couldn't retrieve its ID.",
            ephemeral=True,
        )
        return

    vote_id = int(vote_id_row[0])

    row = await get_vote(vote_id)

    message, send_error = await safe_call(
        interaction.channel.send(
            embed=build_vote_embed(row, [0] * len(parsed_options)),
            view=VoteView(vote_id, parsed_options),
        ),
        "event_vote channel send",
    )

    if send_error:
        await interaction.response.send_message(
            send_error,
            ephemeral=True,
        )
        return

    async def write_message_id():
        await cursor.aexecute(
            "UPDATE votes SET message_id = ? WHERE vote_id = ?",
            (message.id, vote_id),
            commit=True,
        )

    await write_message_id()

    await interaction.response.send_message(
        f"✅ Vote created.\n"
        f"**Vote ID:** `VOTE-{vote_id:04d}`\n"
        f"**Ends:** <t:{end_time}:F>",
        ephemeral=True,
    )


# ============================================================
# PERSONAL REMINDERS
# ============================================================


@tree.command(
    name="reminder",
    description="Create a personal DM reminder",
)
@app_commands.describe(
    title="Reminder title",
    description="What should the reminder say?",
    duration="Time from now, e.g. 10m, 2h, 1d 2h",
)
async def reminder(
    interaction: discord.Interaction,
    title: str,
    description: str,
    duration: str,
):
    seconds = parse_duration(duration)

    if seconds is None:
        await interaction.response.send_message(
            "Invalid duration. Examples: `10m`, `2h`, `1d 2h`.",
            ephemeral=True,
        )
        return

    if seconds < 10:
        await interaction.response.send_message(
            "Reminder time must be at least 10 seconds.",
            ephemeral=True,
        )
        return

    if seconds > 365 * 86400:
        await interaction.response.send_message(
            "Reminder time cannot be more than 365 days.",
            ephemeral=True,
        )
        return

    if len(title) > 256:
        await interaction.response.send_message(
            "The title is too long.",
            ephemeral=True,
        )
        return

    if len(description) > 4000:
        await interaction.response.send_message(
            "The description is too long.",
            ephemeral=True,
        )
        return

    remind_at = now_ts() + seconds

    async def write_reminder():
        await cursor.aexecute(
            """
            INSERT INTO reminders
            (guild_id, user_id, title, description, remind_at, sent, created_at)
            VALUES (?, ?, ?, ?, ?, 0, ?)
            """,
            (
                interaction.guild.id,
                interaction.user.id,
                title,
                description,
                remind_at,
                now_ts(),
            ),
            commit=True,
        )

    await write_reminder()

    await interaction.response.send_message(
        f"⏰ **Reminder Created**\n\n"
        f"**{title}**\n"
        f"{description}\n\n"
        f"I'll remind you <t:{remind_at}:R>.",
        ephemeral=True,
    )


async def process_reminders():
    rows = await cursor.aexecute(
        """
        SELECT reminder_id, user_id, title, description, remind_at
        FROM reminders
        WHERE sent = 0
          AND remind_at <= ?
        ORDER BY remind_at ASC
        LIMIT 100
        """,
        (now_ts(),),
        fetch="all",
    )

    for reminder_id, user_id, title, description, remind_at in rows:
        embed = make_embed(
            f"⏰ {title}",
            description,
            EMBED_COLOR,
        )

        embed.add_field(
            name="Scheduled",
            value=f"<t:{int(remind_at)}:F>",
            inline=False,
        )

        embed.set_footer(text=f"Reminder ID: {reminder_id}")

        try:
            user = client.get_user(int(user_id))

            if user is None:
                user = await client.fetch_user(int(user_id))

            await user.send(embed=embed)

        except discord.Forbidden:
            pass

        except discord.NotFound:
            pass

        except Exception as error:
            await log_error(
                f"process_reminder DM user={user_id}",
                error,
            )

        async def mark_sent(reminder_id=reminder_id):
            await cursor.aexecute(
                "UPDATE reminders SET sent = 1 WHERE reminder_id = ?",
                (reminder_id,),
                commit=True,
            )

        await mark_sent()


# ============================================================
# EVENT / VOTE SCHEDULER
# ============================================================


async def process_events():
    current = now_ts()

    # -------------------------
    # 10-MINUTE EVENT WARNINGS
    # -------------------------

    warning_rows = await cursor.aexecute(
        """
        SELECT event_id, guild_id, channel_id, message_id, title,
               description, event_time, created_by, thumbnail_url,
               warning_sent, live_sent, status, created_at
        FROM events
        WHERE status = 'scheduled'
          AND warning_sent = 0
          AND event_time <= ?
          AND event_time > ?
        ORDER BY event_time ASC
        LIMIT 100
        """,
        (current + 600, current),
        fetch="all",
    )

    for row in warning_rows:
        event_id = int(row[0])

        # Mark first so duplicate scheduler passes cannot send twice.
        async def mark_warning(event_id=event_id):
            await cursor.aexecute(
                """
                UPDATE events
                SET warning_sent = 1
                WHERE event_id = ?
                  AND warning_sent = 0
                """,
                (event_id,),
                commit=True,
            )

        await mark_warning()

        await send_event_notification(row, live=False)

    # -------------------------
    # LIVE EVENT NOTIFICATIONS
    # -------------------------

    live_rows = await cursor.aexecute(
        """
        SELECT event_id, guild_id, channel_id, message_id, title,
               description, event_time, created_by, thumbnail_url,
               warning_sent, live_sent, status, created_at
        FROM events
        WHERE status = 'scheduled'
          AND live_sent = 0
          AND event_time <= ?
        ORDER BY event_time ASC
        LIMIT 100
        """,
        (current,),
        fetch="all",
    )

    for row in live_rows:
        event_id = int(row[0])

        async def mark_live(event_id=event_id):
            await cursor.aexecute(
                """
                UPDATE events
                SET live_sent = 1,
                    status = 'live'
                WHERE event_id = ?
                  AND live_sent = 0
                """,
                (event_id,),
                commit=True,
            )

        await mark_live()

        await send_event_notification(row, live=True)

        # Update the original event post to show that it is live.
        await update_event_message(event_id)

    # -------------------------
    # FINISH EVENTS AFTER LIVE
    # -------------------------

    await cursor.aexecute(
        """
        UPDATE events
        SET status = 'finished'
        WHERE status = 'live'
          AND event_time <= ?
          AND live_sent = 1
        """,
        (current - 3600,),
        commit=True,
    )


async def process_votes():
    rows = await cursor.aexecute(
        """
        SELECT vote_id, guild_id, channel_id, message_id, question,
               options, end_time, created_by, status, created_at
        FROM votes
        WHERE status = 'open'
          AND end_time <= ?
        ORDER BY end_time ASC
        LIMIT 100
        """,
        (now_ts(),),
        fetch="all",
    )

    for row in rows:
        vote_id = int(row[0])
        options = row[5].split("\n")

        counts = await get_vote_counts(
            vote_id,
            len(options),
        )

        winner_count = max(counts) if counts else 0
        winners = [
            options[index]
            for index, count in enumerate(counts)
            if count == winner_count
        ]

        if winner_count == 0:
            result = "No votes were cast."
        elif len(winners) == 1:
            result = (
                f"🏆 **Winner:** {winners[0]}\n"
                f"**Votes:** {winner_count}"
            )
        else:
            result = (
                "🤝 **Tie:**\n"
                + "\n".join(f"• {winner}" for winner in winners)
                + f"\n\nEach tied option received **{winner_count}** votes."
            )

        closed_embed = build_vote_embed(row, counts)

        closed_embed.add_field(
            name="🏁 Final Result",
            value=result,
            inline=False,
        )

        try:
            channel = client.get_channel(int(row[2]))

            if channel and row[3]:
                message = await channel.fetch_message(int(row[3]))

                await message.edit(
                    embed=closed_embed,
                    view=None,
                )

        except discord.NotFound:
            pass

        except Exception as error:
            await log_error("process_votes message update", error)

        async def close_vote(vote_id=vote_id):
            await cursor.aexecute(
                """
                UPDATE votes
                SET status = 'closed'
                WHERE vote_id = ?
                  AND status = 'open'
                """,
                (vote_id,),
                commit=True,
            )

        await close_vote()


async def scheduler_loop():
    await client.wait_until_ready()

    while not client.is_closed():
        try:
            await process_events()
            await process_votes()
            await process_reminders()

        except Exception as error:
            await log_error("scheduler_loop", error)

        await asyncio.sleep(SCHEDULER_INTERVAL)


# ============================================================
# TICKET SYSTEM
# ============================================================

TICKET_TYPES = {
    "Alliance Support": {
        "emoji": "🛡️",
        "description": "General alliance support and assistance.",
    },
    "Recruitment": {
        "emoji": "📜",
        "description": "Questions or requests regarding recruitment.",
    },
    "Report a Member": {
        "emoji": "🚨",
        "description": "Report an issue involving an alliance member.",
    },
    "Other": {
        "emoji": "💬",
        "description": "Anything that does not fit the other categories.",
    },
}


def ticket_staff_allowed(member, ticket_type):
    if is_ticket_manager(member):
        return True

    role_id = TICKET_ROLE_IDS.get(ticket_type, 0)

    if role_id == 0:
        return False

    return any(role.id == role_id for role in member.roles)


async def get_ticket_row(channel_id):
    return await cursor.aexecute(
        """
        SELECT channel_id, guild_id, opener_id, ticket_type,
               claimed_by, status, important, pinned, base_name
        FROM tickets
        WHERE channel_id = ?
        """,
        (channel_id,),
        fetch="one",
    )


def build_ticket_channel_name(base_name, important, solved, pinned):
    prefix = ""

    if pinned:
        prefix += "📌"

    if important:
        prefix += "🔴"

    if solved:
        prefix += "✅"

    return (prefix + base_name)[:100]


async def apply_ticket_channel_name(channel):
    row = await get_ticket_row(channel.id)

    if not row:
        return

    (
        channel_id,
        guild_id,
        opener_id,
        ticket_type,
        claimed_by,
        status,
        important,
        pinned,
        base_name,
    ) = row

    new_name = build_ticket_channel_name(
        base_name,
        bool(important),
        status == "solved",
        bool(pinned),
    )

    if new_name == channel.name:
        return

    try:
        await channel.edit(name=new_name)

    except discord.HTTPException as error:
        await log_error("apply_ticket_channel_name", error)

    except Exception as error:
        await log_error("apply_ticket_channel_name", error)


async def create_ticket(interaction, ticket_type):
    guild = interaction.guild
    opener = interaction.user

    # One open ticket per user.
    existing = await cursor.aexecute(
        """
        SELECT channel_id
        FROM tickets
        WHERE guild_id = ?
          AND opener_id = ?
          AND status IN ('open', 'solved')
        LIMIT 1
        """,
        (guild.id, opener.id),
        fetch="one",
    )

    if existing:
        existing_channel = guild.get_channel(int(existing[0]))

        if existing_channel:
            await interaction.response.send_message(
                f"You already have an open ticket: {existing_channel.mention}",
                ephemeral=True,
            )
            return

        await cursor.aexecute(
            "DELETE FROM tickets WHERE channel_id = ?",
            (int(existing[0]),),
            commit=True,
        )

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=False,
        ),
        opener: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
        ),
    }

    bot_member = guild.me

    if bot_member:
        overwrites[bot_member] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            manage_messages=True,
        )

    manager_role = (
        guild.get_role(TICKET_MANAGER_ROLE_ID)
        if TICKET_MANAGER_ROLE_ID
        else None
    )

    if manager_role:
        overwrites[manager_role] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
        )

    role_id = TICKET_ROLE_IDS.get(ticket_type, 0)

    if role_id:
        role = guild.get_role(role_id)

        if role:
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
            )

    category = (
        guild.get_channel(TICKET_CATEGORY_ID)
        if TICKET_CATEGORY_ID
        else None
    )

    safe_name = re.sub(
        r"[^a-z0-9\-]",
        "",
        opener.name.lower().replace(" ", "-"),
    )[:20]

    if not safe_name:
        safe_name = str(opener.id)

    safe_type = re.sub(
        r"[^a-z0-9\-]",
        "",
        ticket_type.lower().replace(" ", "-"),
    )

    base_name = f"ticket-{safe_type}-{safe_name}"

    channel, error = await safe_call(
        guild.create_text_channel(
            base_name,
            category=category if isinstance(category, discord.CategoryChannel) else None,
            overwrites=overwrites,
            reason=f"Pandemonium ticket opened by {opener} ({opener.id})",
        ),
        "create_ticket channel",
    )

    if error:
        await interaction.response.send_message(
            error,
            ephemeral=True,
        )
        return

    async def write_ticket():
        await cursor.aexecute(
            """
            INSERT INTO tickets
            (channel_id, guild_id, opener_id, ticket_type,
             claimed_by, status, important, pinned, base_name, created_at)
            VALUES (?, ?, ?, ?, NULL, 'open', 0, 0, ?, ?)
            """,
            (
                channel.id,
                guild.id,
                opener.id,
                ticket_type,
                base_name,
                now_ts(),
            ),
            commit=True,
        )

    await write_ticket()

    config = TICKET_TYPES[ticket_type]

    mentions = []

    if manager_role:
        mentions.append(manager_role.mention)

    if role_id:
        role = guild.get_role(role_id)

        if role and role != manager_role:
            mentions.append(role.mention)

    embed = make_embed(
        f"{config['emoji']} {ticket_type}",
        f"{opener.mention} opened this ticket.\n\n"
        f"**Purpose:** {config['description']}\n\n"
        "Staff will assist you as soon as possible.\n"
        "Use the controls below to manage the ticket.",
    )

    embed.set_footer(
        text=f"Ticket type: {ticket_type}"
    )

    message, send_error = await safe_call(
        channel.send(
            content=" ".join(mentions) if mentions else None,
            embed=embed,
            view=TicketControlView(),
        ),
        "create_ticket initial message",
    )

    if send_error:
        await interaction.response.send_message(
            send_error,
            ephemeral=True,
        )
        return

    try:
        await message.pin()
    except Exception as error:
        await log_error("create_ticket pin", error)

    await interaction.response.send_message(
        f"🎫 Ticket created: {channel.mention}",
        ephemeral=True,
    )


class TicketTypeSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label=label,
                description=config["description"][:100],
                emoji=config["emoji"],
                value=label,
            )
            for label, config in TICKET_TYPES.items()
        ]

        super().__init__(
            placeholder="Select a ticket category...",
            options=options,
            custom_id="pandemonium_ticket_panel_select",
        )

    async def callback(self, interaction):
        await create_ticket(
            interaction,
            self.values[0],
        )


class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketTypeSelect())


class ConfirmCloseTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(
        label="Confirm Close",
        style=discord.ButtonStyle.danger,
        emoji="🔒",
    )
    async def confirm(self, interaction, button):
        row = await get_ticket_row(interaction.channel.id)

        if not row:
            await interaction.response.send_message(
                "This is not a tracked ticket.",
                ephemeral=True,
            )
            return

        if (
            interaction.user.id != int(row[2])
            and not ticket_staff_allowed(interaction.user, row[3])
        ):
            await interaction.response.send_message(
                "You don't have permission to close this ticket.",
                ephemeral=True,
            )
            return

        async def close_ticket():
            await cursor.aexecute(
                """
                UPDATE tickets
                SET status = 'closed'
                WHERE channel_id = ?
                """,
                (interaction.channel.id,),
                commit=True,
            )

        await close_ticket()

        await interaction.response.send_message(
            "🔒 Ticket closed. This channel will be deleted in 5 seconds.",
            ephemeral=True,
        )

        await interaction.channel.send(
            f"🔒 Ticket closed by {interaction.user.mention}."
        )

        await asyncio.sleep(5)

        try:
            await interaction.channel.delete(
                reason=f"Ticket closed by {interaction.user}"
            )
        except Exception as error:
            await log_error("close ticket delete", error)

    @discord.ui.button(
        label="Cancel",
        style=discord.ButtonStyle.secondary,
        emoji="✖️",
    )
    async def cancel(self, interaction, button):
        await interaction.response.send_message(
            "Cancelled.",
            ephemeral=True,
        )


class TicketControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Close",
        style=discord.ButtonStyle.danger,
        emoji="🔒",
        custom_id="pandemonium_ticket_close",
    )
    async def close_button(self, interaction, button):
        row = await get_ticket_row(interaction.channel.id)

        if not row:
            await interaction.response.send_message(
                "This is not a tracked ticket channel.",
                ephemeral=True,
            )
            return

        opener_id = int(row[2])
        ticket_type = row[3]

        if (
            interaction.user.id != opener_id
            and not ticket_staff_allowed(interaction.user, ticket_type)
        ):
            await interaction.response.send_message(
                "You don't have permission to close this ticket.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "Are you sure you want to close this ticket?",
            view=ConfirmCloseTicketView(),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Claim",
        style=discord.ButtonStyle.primary,
        emoji="🙋",
        custom_id="pandemonium_ticket_claim",
    )
    async def claim_button(self, interaction, button):
        row = await get_ticket_row(interaction.channel.id)

        if not row:
            await interaction.response.send_message(
                "This is not a tracked ticket channel.",
                ephemeral=True,
            )
            return

        ticket_type = row[3]
        claimed_by = row[4]

        if not ticket_staff_allowed(interaction.user, ticket_type):
            await interaction.response.send_message(
                "You don't have permission to claim this ticket.",
                ephemeral=True,
            )
            return

        if claimed_by:
            claimer = interaction.guild.get_member(int(claimed_by))

            await interaction.response.send_message(
                f"Already claimed by "
                f"{claimer.mention if claimer else f'<@{claimed_by}>'}.",
                ephemeral=True,
            )
            return

        async def claim():
            await cursor.aexecute(
                """
                UPDATE tickets
                SET claimed_by = ?
                WHERE channel_id = ?
                """,
                (
                    interaction.user.id,
                    interaction.channel.id,
                ),
                commit=True,
            )

        await claim()

        await interaction.response.send_message(
            f"🙋 Ticket claimed by {interaction.user.mention}."
        )

    @discord.ui.button(
        label="Mark Solved",
        style=discord.ButtonStyle.success,
        emoji="✅",
        custom_id="pandemonium_ticket_solved",
    )
    async def solved_button(self, interaction, button):
        row = await get_ticket_row(interaction.channel.id)

        if not row:
            await interaction.response.send_message(
                "This is not a tracked ticket channel.",
                ephemeral=True,
            )
            return

        ticket_type = row[3]

        if not ticket_staff_allowed(interaction.user, ticket_type):
            await interaction.response.send_message(
                "You don't have permission to do that.",
                ephemeral=True,
            )
            return

        new_status = (
            "open"
            if row[5] == "solved"
            else "solved"
        )

        async def update_status():
            await cursor.aexecute(
                """
                UPDATE tickets
                SET status = ?
                WHERE channel_id = ?
                """,
                (
                    new_status,
                    interaction.channel.id,
                ),
                commit=True,
            )

        await update_status()

        text = (
            f"✅ Marked as solved by {interaction.user.mention}."
            if new_status == "solved"
            else f"↩️ Ticket reopened by {interaction.user.mention}."
        )

        await interaction.response.send_message(text)

        await apply_ticket_channel_name(
            interaction.channel
        )

    @discord.ui.button(
        label="Pin",
        style=discord.ButtonStyle.secondary,
        emoji="📌",
        custom_id="pandemonium_ticket_pin",
    )
    async def pin_button(self, interaction, button):
        row = await get_ticket_row(interaction.channel.id)

        if not row:
            await interaction.response.send_message(
                "This is not a tracked ticket channel.",
                ephemeral=True,
            )
            return

        ticket_type = row[3]

        if not ticket_staff_allowed(interaction.user, ticket_type):
            await interaction.response.send_message(
                "You don't have permission to do that.",
                ephemeral=True,
            )
            return

        new_pinned = 0 if int(row[7]) else 1

        async def update_pin():
            await cursor.aexecute(
                """
                UPDATE tickets
                SET pinned = ?
                WHERE channel_id = ?
                """,
                (
                    new_pinned,
                    interaction.channel.id,
                ),
                commit=True,
            )

        await update_pin()

        if new_pinned:
            try:
                await interaction.message.pin()
            except Exception as error:
                await log_error("ticket pin", error)

            text = "📌 Ticket pinned."

        else:
            try:
                await interaction.message.unpin()
            except Exception as error:
                await log_error("ticket unpin", error)

            text = "↩️ Ticket unpinned."

        await interaction.response.send_message(
            text,
            ephemeral=True,
        )

        await apply_ticket_channel_name(
            interaction.channel
        )

    @discord.ui.button(
        label="Mark Important",
        style=discord.ButtonStyle.secondary,
        emoji="🔴",
        custom_id="pandemonium_ticket_important",
    )
    async def important_button(self, interaction, button):
        row = await get_ticket_row(interaction.channel.id)

        if not row:
            await interaction.response.send_message(
                "This is not a tracked ticket channel.",
                ephemeral=True,
            )
            return

        ticket_type = row[3]

        if not ticket_staff_allowed(interaction.user, ticket_type):
            await interaction.response.send_message(
                "You don't have permission to do that.",
                ephemeral=True,
            )
            return

        new_important = 0 if int(row[6]) else 1

        async def update_important():
            await cursor.aexecute(
                """
                UPDATE tickets
                SET important = ?
                WHERE channel_id = ?
                """,
                (
                    new_important,
                    interaction.channel.id,
                ),
                commit=True,
            )

        await update_important()

        text = (
            "🔴 Marked as important."
            if new_important
            else "↩️ Removed important marker."
        )

        await interaction.response.send_message(
            text,
            ephemeral=True,
        )

        await apply_ticket_channel_name(
            interaction.channel
        )


@tree.command(
    name="ticket",
    description="Post the Pandemonium ticket panel",
)
async def ticket_panel(interaction):
    if not is_admin(interaction.user):
        await interaction.response.send_message(
            "Administrators only.",
            ephemeral=True,
        )
        return

    embed = make_embed(
        "🎫 Pandemonium Support",
        "Need help from the alliance staff?\n\n"
        "Select the category that best describes your request below.\n\n"
        "Please provide clear information so the staff team can assist you efficiently.",
    )

    await interaction.channel.send(
        embed=embed,
        view=TicketPanelView(),
    )

    await interaction.response.send_message(
        "✅ Ticket panel posted.",
        ephemeral=True,
    )


# ============================================================
# HELP / BASIC COMMANDS
# ============================================================


@tree.command(
    name="ping",
    description="Check whether Pandemonium is online",
)
async def ping(interaction):
    latency = round(client.latency * 1000)

    await interaction.response.send_message(
        f"🏰 **{BOT_NAME}** is online.\n"
        f"Latency: **{latency}ms**",
        ephemeral=True,
    )


@tree.command(
    name="help",
    description="Show Pandemonium bot commands",
)
async def help_command(interaction):
    embed = make_embed(
        "🏰 Pandemonium — Commands",
        "Alliance management and reminder tools.",
    )

    embed.add_field(
        name="Events",
        value=(
            "`/event create` — Create an event\n"
            "`/event management` — View event attendees\n"
            "`/event vote` — Create a member vote"
        ),
        inline=False,
    )

    embed.add_field(
        name="Personal",
        value="`/reminder` — Create a private DM reminder",
        inline=False,
    )

    embed.add_field(
        name="Tickets",
        value="`/ticket` — Post the ticket panel (Administrators)",
        inline=False,
    )

    embed.add_field(
        name="Utility",
        value="`/ping` — Check bot status",
        inline=False,
    )

    embed.set_footer(
        text="Event Manager permissions are controlled by EVENT_MANAGER_ROLE_ID."
    )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
    )


# ============================================================
# COMMAND REGISTRATION
# ============================================================

tree.add_command(event_group)


# ============================================================
# STARTUP
# ============================================================


@client.event
async def on_ready():
    print(f"Logged in as {client.user} ({client.user.id})")

    if GUILD_ID:
        guild = discord.Object(id=GUILD_ID)

        try:
            tree.copy_global_to(guild=guild)
            await tree.sync(guild=guild)
            print(f"[Discord] Synced commands to guild {GUILD_ID}.")
        except Exception as error:
            await log_error("guild command sync", error)

    else:
        try:
            await tree.sync()
            print("[Discord] Synced global commands.")
        except Exception as error:
            await log_error("global command sync", error)

    # Persistent views survive bot restarts.
    if not getattr(client, "persistent_views_registered", False):
        client.add_view(TicketPanelView())
        client.add_view(TicketControlView())

        # Event and vote views have dynamic custom IDs, so restore the
        # currently scheduled database records.
        try:
            event_rows = await cursor.aexecute(
                """
                SELECT event_id
                FROM events
                WHERE status = 'scheduled'
                  AND event_time > ?
                """,
                (now_ts(),),
                fetch="all",
            )

            for row in event_rows:
                client.add_view(
                    EventNotificationView(int(row[0]))
                )

            vote_rows = await cursor.aexecute(
                """
                SELECT vote_id, options
                FROM votes
                WHERE status = 'open'
                  AND end_time > ?
                """,
                (now_ts(),),
                fetch="all",
            )

            for row in vote_rows:
                options = row[1].split("\n")

                client.add_view(
                    VoteView(
                        int(row[0]),
                        options,
                    )
                )

        except Exception as error:
            await log_error(
                "persistent dynamic view registration",
                error,
            )

        client.persistent_views_registered = True

    if not getattr(client, "scheduler_started", False):
        client.scheduler_started = True
        client.loop.create_task(
            scheduler_loop()
        )

    print("[Pandemonium] Ready.")


@client.event
async def on_error(event_method, *args, **kwargs):
    await log_error(
        f"Discord event error: {event_method}",
        Exception(traceback.format_exc()),
    )


@tree.error
async def on_app_command_error(
    interaction,
    error,
):
    await log_error(
        f"Slash command error: {interaction.command}",
        error,
    )

    try:
        message = (
            "Something went wrong. The error has been logged."
        )

        if interaction.response.is_done():
            await interaction.followup.send(
                message,
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                message,
                ephemeral=True,
            )

    except Exception:
        pass


# ============================================================
# MAIN
# ============================================================


def validate_environment():
    required = [
        "DISCORD_TOKEN",
        "TURSO_DATABASE_URL",
        "TURSO_AUTH_TOKEN",
    ]

    missing = [
        key
        for key in required
        if not os.environ.get(key)
    ]

    if missing:
        raise RuntimeError(
            "Missing Railway environment variables: "
            + ", ".join(missing)
        )


if __name__ == "__main__":
    validate_environment()

    token = os.environ["DISCORD_TOKEN"]

    print("============================================")
    print("      PANDEMONIUM DISCORD BOT")
    print("============================================")
    print(f"Guild ID: {GUILD_ID or 'GLOBAL COMMAND SYNC'}")
    print(f"Event Manager Role: {EVENT_MANAGER_ROLE_ID or 'NOT SET'}")
    print(f"Event Channel: {EVENT_CHANNEL_ID or 'CURRENT COMMAND CHANNEL'}")
    print("Starting bot...")

    client.run(token)
