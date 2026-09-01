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
EMBED_COLOR = discord.Color(0x7851A9)  # Royal purple

# Single-server configuration.
GUILD_ID =1543949293180682311 # TODO: Put Pandemonium's Discord server ID here.

# Event manager role. Administrators always bypass this check.
EVENT_MANAGER_ROLE_ID = 1544004325234184202  # TODO: Put the Event Manager role ID here.

# Channel where event warnings / live announcements are posted.
EVENT_CHANNEL_ID = 1544002924081053726  # TODO: Put the event announcement channel ID here.

# Channel where errors are logged. 0 disables Discord error logging.
ERROR_LOG_CHANNEL_ID = 1544004794337861754  # TODO: Put an error-log channel ID here.

# Channel where new-member welcome messages are posted. 0 disables the welcomer.
WELCOME_CHANNEL_ID = 1544003651629097050  # TODO: Put the welcome channel ID here.

# Role granted to a member once they complete DM verification.
MEMBER_ROLE_ID = 1544011040059039867

# Ticket system.
# New ticket channels are created inside this category.
TICKET_CATEGORY_ID = 1544004584123539506  # TODO: Put the ticket category ID here.

# Optional role that can see/manage every ticket.
TICKET_MANAGER_ROLE_ID = 1544005160051675177  # TODO: Put the ticket manager role ID here.

# Ticket staff roles for each category.
# Add role IDs when you are ready.
TICKET_ROLE_IDS = {
    "Alliance Support": 1544005160051675177,
    "Recruitment": 1544005160051675177,
    "Report a Member": 1544005160051675177,
    "Other": 1544005160051675177,
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

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS pending_verifications (
        user_id INTEGER PRIMARY KEY,
        guild_id INTEGER NOT NULL,
        dm_channel_id INTEGER,
        dm_message_id INTEGER,
        created_at INTEGER NOT NULL
    )
    """
)

db_commit()

# Older databases won't have this column yet — add it if missing so the
# "close event" button on the live announcement can be located/edited later.
try:
    cursor.execute("ALTER TABLE events ADD COLUMN live_message_id INTEGER")
    db_commit()
except Exception as error:
    if "duplicate column" not in str(error).lower():
        raise

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


def parse_event_datetime_to_unix(time_text, date_text=None):
    """
    Combines a UTC clock time (HH:MM, 24-hour) with an optional UTC
    date (YYYY-MM-DD) into a unix timestamp.

    If date_text is omitted, resolves to the next occurrence of that
    time — today if it hasn't happened yet, otherwise tomorrow.

    If date_text is given, the event is scheduled for exactly that
    UTC date and time. The caller is responsible for checking that
    the result is actually in the future.

    Returns None if either value is invalid.
    """
    if not time_text:
        return None

    time_text = time_text.strip()
    time_match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", time_text)

    if not time_match:
        return None

    hour = int(time_match.group(1))
    minute = int(time_match.group(2))

    if date_text:
        date_text = date_text.strip()

        try:
            date_part = datetime.strptime(date_text, "%Y-%m-%d")
        except ValueError:
            return None

        candidate = date_part.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
            tzinfo=timezone.utc,
        )

    else:
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
            color=EMBED_COLOR,
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
        live_message_id,
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
    elif status == "closed":
        status_text = "🔒 Closed"
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
               warning_sent, live_sent, status, created_at,
               live_message_id
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
    status = row[11]

    if not message_id:
        return

    channel = client.get_channel(channel_id)

    if not channel:
        return

    # Only a still-scheduled event can be joined, so only it gets the
    # notification button. Live/closed/finished events show no button.
    view = EventNotificationView(event_id) if status == "scheduled" else None

    try:
        message = await channel.fetch_message(message_id)

        await message.edit(
            embed=build_event_embed(row),
            view=view,
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
        live_message_id,
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
        # The live announcement gets a Close Event button so event
        # managers can end the event straight from that message.
        view = EventCloseView(event_id) if live else None

        if mentions:
            # Keep the mention message under Discord's message limit.
            content = " ".join(mentions)

            if len(content) > 1900:
                content = content[:1897] + "..."

            message, error = await safe_call(
                channel.send(content=content, embed=embed, view=view),
                "send_event_notification channel send",
            )
        else:
            message, error = await safe_call(
                channel.send(embed=embed, view=view),
                "send_event_notification channel send",
            )

        if live and message and not error:
            await cursor.aexecute(
                "UPDATE events SET live_message_id = ? WHERE event_id = ?",
                (message.id, event_id),
                commit=True,
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


async def close_event_status(event_id):
    """
    Marks an event as closed. Only a 'scheduled' or 'live' event can be
    closed. Returns (success, row) — row is the event's latest state
    (None if the event does not exist).
    """
    row = await get_event(event_id)

    if not row:
        return False, None

    status = row[11]

    if status not in ("scheduled", "live"):
        return False, row

    await cursor.aexecute(
        """
        UPDATE events
        SET status = 'closed'
        WHERE event_id = ?
          AND status = ?
        """,
        (event_id, status),
        commit=True,
    )

    row = await get_event(event_id)
    return True, row


async def finalize_event_closure(row):
    """
    After an event has been marked closed in the database, disables the
    Close Event button on the live announcement message (if it still
    exists) and refreshes the original event post.
    """
    event_id = row[0]
    channel_id = row[2]
    live_message_id = row[13]

    if live_message_id:
        channel = client.get_channel(channel_id)

        if channel:
            try:
                message = await channel.fetch_message(live_message_id)

                closed_view = EventCloseView(event_id)

                for item in closed_view.children:
                    item.disabled = True

                embed = message.embeds[0] if message.embeds else None

                if embed:
                    embed.color = discord.Color.dark_grey()
                    embed.set_footer(text="🔒 This event has been closed.")

                await message.edit(embed=embed, view=closed_view)

            except discord.NotFound:
                pass

            except Exception as error:
                await log_error("finalize_event_closure", error)

    await update_event_message(event_id)


class EventCloseView(discord.ui.View):
    def __init__(self, event_id):
        super().__init__(timeout=None)
        self.event_id = event_id

        button = discord.ui.Button(
            label="🔒 Close Event",
            style=discord.ButtonStyle.danger,
            custom_id=f"event_close:{event_id}",
        )

        button.callback = self.close_event
        self.add_item(button)

    async def close_event(self, interaction):
        if not is_event_manager(interaction.user):
            await interaction.response.send_message(
                "Only event managers can close this event.",
                ephemeral=True,
            )
            return

        success, row = await close_event_status(self.event_id)

        if not success:
            if row:
                await interaction.response.send_message(
                    "This event is already closed or finished.",
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message(
                    "This event could not be found.",
                    ephemeral=True,
                )
            return

        for item in self.children:
            item.disabled = True

        embed = interaction.message.embeds[0] if interaction.message.embeds else None

        if embed:
            embed.color = discord.Color.dark_grey()
            embed.set_footer(text="🔒 This event has been closed.")

        await interaction.response.edit_message(embed=embed, view=self)

        await update_event_message(self.event_id)

        await interaction.followup.send(
            f"🔒 **{row[4]}** (`EVT-{self.event_id:04d}`) has been closed "
            f"by {interaction.user.mention}.",
            ephemeral=False,
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
    time_utc="Event start time in UTC, 24-hour HH:MM (e.g. 20:40).",
    date_utc="Optional date in UTC, YYYY-MM-DD (e.g. 2026-09-03). If omitted, uses today/tomorrow based on the time.",
    thumbnail="Optional large image shown below the event embed",
)
async def event_create(
    interaction: discord.Interaction,
    title: str,
    description: str,
    time_utc: str,
    date_utc: str = None,
    thumbnail: discord.Attachment = None,
):
    if not is_event_manager(interaction.user):
        await interaction.response.send_message(
            "You don't have permission to create events.",
            ephemeral=True,
        )
        return

    # Acknowledge immediately. Everything below involves several
    # sequential DB calls and a channel send, which can occasionally
    # take longer than Discord's 3-second initial-response window
    # (especially if the gateway falls behind). Deferring buys a
    # 15-minute followup window instead.
    await interaction.response.defer(ephemeral=True)

    time_unix = parse_event_datetime_to_unix(time_utc, date_utc)

    if time_unix is None:
        if date_utc:
            await interaction.followup.send(
                "Invalid date or time format. Use `YYYY-MM-DD` for the "
                "date (e.g. `2026-09-03`) and 24-hour UTC time like "
                "`20:40`.",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                "Invalid time format. Use 24-hour UTC time like `20:40`.",
                ephemeral=True,
            )
        return

    if time_unix <= now_ts():
        await interaction.followup.send(
            "The event time must be in the future.",
            ephemeral=True,
        )
        return

    if len(title) > 256:
        await interaction.followup.send(
            "The title is too long. Keep it under 256 characters.",
            ephemeral=True,
        )
        return

    if len(description) > 4000:
        await interaction.followup.send(
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
        await interaction.followup.send(
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
        await interaction.followup.send(
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
        await interaction.followup.send(error, ephemeral=True)
        return

    async def write_message_id():
        await cursor.aexecute(
            "UPDATE events SET message_id = ? WHERE event_id = ?",
            (message.id, event_id),
            commit=True,
        )

    await write_message_id()

    await interaction.followup.send(
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

    await interaction.response.defer(ephemeral=True)

    event_id = event_id.strip().upper().replace("EVT-", "")

    try:
        numeric_id = int(event_id)
    except ValueError:
        await interaction.followup.send(
            "Invalid event ID. Example: `EVT-0001`.",
            ephemeral=True,
        )
        return

    row = await get_event(numeric_id)

    if not row:
        await interaction.followup.send(
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

    await interaction.followup.send(
        embed=embed,
        ephemeral=True,
    )


@event_group.command(
    name="close",
    description="Close a running or scheduled event",
)
@app_commands.describe(
    event_id="Event ID, for example EVT-0001 or 1",
)
async def event_close(
    interaction: discord.Interaction,
    event_id: str,
):
    if not is_event_manager(interaction.user):
        await interaction.response.send_message(
            "You don't have permission to close events.",
            ephemeral=True,
        )
        return

    # Closing touches the live message, the original event post, and
    # the database — several sequential API/DB calls that can exceed
    # Discord's 3-second initial-response window. Defer first.
    await interaction.response.defer(ephemeral=True)

    event_id = event_id.strip().upper().replace("EVT-", "")

    try:
        numeric_id = int(event_id)
    except ValueError:
        await interaction.followup.send(
            "Invalid event ID. Example: `EVT-0001`.",
            ephemeral=True,
        )
        return

    success, row = await close_event_status(numeric_id)

    if not success:
        if row is None:
            await interaction.followup.send(
                "That event could not be found.",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                f"`EVT-{numeric_id:04d}` is already **{row[11]}** "
                "and can't be closed.",
                ephemeral=True,
            )
        return

    await finalize_event_closure(row)

    await interaction.followup.send(
        f"🔒 **{row[4]}** (`EVT-{numeric_id:04d}`) has been closed.",
        ephemeral=True,
    )


@event_group.command(
    name="list",
    description="View currently running and upcoming events",
)
async def event_list(interaction: discord.Interaction):
    await interaction.response.defer()

    rows = await cursor.aexecute(
        """
        SELECT event_id, guild_id, channel_id, message_id, title,
               description, event_time, created_by, thumbnail_url,
               warning_sent, live_sent, status, created_at,
               live_message_id
        FROM events
        WHERE guild_id = ?
          AND status IN ('scheduled', 'live')
        ORDER BY event_time ASC
        LIMIT 25
        """,
        (interaction.guild.id,),
        fetch="all",
    )

    count_map = {}

    if rows:
        placeholders = ",".join("?" * len(rows))

        count_rows = await cursor.aexecute(
            f"""
            SELECT event_id, COUNT(*)
            FROM event_attendees
            WHERE event_id IN ({placeholders})
            GROUP BY event_id
            """,
            tuple(int(row[0]) for row in rows),
            fetch="all",
        )

        count_map = {int(r[0]): int(r[1]) for r in count_rows}

    live_rows = [row for row in rows if row[11] == "live"]
    upcoming_rows = [row for row in rows if row[11] == "scheduled"]

    def build_field(rows, upcoming):
        if not rows:
            return (
                "No events are currently scheduled."
                if upcoming
                else "No events are live right now."
            )

        lines = []

        for row in rows:
            event_id = int(row[0])
            title = row[4]
            event_time = row[6]
            count = count_map.get(event_id, 0)
            plural = "participant" if count == 1 else "participants"

            if upcoming:
                time_part = f"<t:{event_time}:F> (<t:{event_time}:R>)"
            else:
                time_part = f"started <t:{event_time}:R>"

            lines.append(
                f"**{title}** — `EVT-{event_id:04d}`\n"
                f"👥 {count} {plural} • {time_part}"
            )

        text = ""
        kept = 0

        for line in lines:
            addition = ("\n\n" if text else "") + line

            if len(text) + len(addition) > 950:
                break

            text += addition
            kept += 1

        remaining = len(lines) - kept

        if remaining > 0:
            text += f"\n\n*…and {remaining} more.*"

        return text

    embed = make_embed(
        "📅 Alliance Events",
        "Overview of events currently running and coming up.",
    )

    embed.add_field(
        name="🔴 Currently Running",
        value=build_field(live_rows, upcoming=False),
        inline=False,
    )

    embed.add_field(
        name="📆 Upcoming",
        value=build_field(upcoming_rows, upcoming=True),
        inline=False,
    )

    await interaction.followup.send(
        embed=embed,
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
        color=EMBED_COLOR,
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

    await interaction.response.defer(ephemeral=True)

    seconds = parse_duration(duration)

    if seconds is None:
        await interaction.followup.send(
            "Invalid duration. Examples: `30m`, `2h`, `1d`, `2h 30m`.",
            ephemeral=True,
        )
        return

    if seconds < 10:
        await interaction.followup.send(
            "Voting must stay open for at least 10 seconds.",
            ephemeral=True,
        )
        return

    if seconds > 30 * 86400:
        await interaction.followup.send(
            "Voting cannot stay open for more than 30 days.",
            ephemeral=True,
        )
        return

    if len(question) > 256:
        await interaction.followup.send(
            "The question must be 256 characters or fewer.",
            ephemeral=True,
        )
        return

    parsed_options, error = parse_vote_options(options)

    if error:
        await interaction.followup.send(
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
        await interaction.followup.send(
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
        await interaction.followup.send(
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

    await interaction.followup.send(
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

    await interaction.response.defer(ephemeral=True)

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

    await interaction.followup.send(
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
               warning_sent, live_sent, status, created_at,
               live_message_id
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
               warning_sent, live_sent, status, created_at,
               live_message_id
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

    await interaction.response.defer(ephemeral=True)

    embed = make_embed(
        "🎫 Pandemonium Support",
        "Need help from the alliance staff?\n\n"
        "Select the category that best describes your request below.\n\n"
        "Please provide clear information so the staff team can assist you efficiently.",
        discord.Color(0xFFFFFF),
    )

    await interaction.channel.send(
        embed=embed,
        view=TicketPanelView(),
    )

    await interaction.followup.send(
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
            "`/event close` — Close a running or scheduled event\n"
            "`/event list` — View running and upcoming events\n"
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
    )


# ============================================================
# COMMAND REGISTRATION
# ============================================================

tree.add_command(event_group)


# ============================================================
# VERIFICATION
# ============================================================


class VerificationModal(discord.ui.Modal, title="Server Verification"):
    ign = discord.ui.TextInput(
        label="In-Game Name",
        placeholder="Enter your exact in-game name",
        max_length=32,
        required=True,
    )

    def __init__(self, guild_id, user_id):
        super().__init__()
        self.guild_id = guild_id
        self.user_id = user_id

    async def on_submit(self, interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This verification form isn't for you.",
                ephemeral=True,
            )
            return

        guild = client.get_guild(self.guild_id)

        if guild is None:
            await interaction.response.send_message(
                "The server could not be found. Please contact staff.",
                ephemeral=True,
            )
            return

        member = guild.get_member(self.user_id)

        if member is None:
            try:
                member = await guild.fetch_member(self.user_id)
            except discord.NotFound:
                member = None

        if member is None:
            await interaction.response.send_message(
                "You don't appear to be a member of the server anymore. "
                "Please rejoin and try again.",
                ephemeral=True,
            )
            return

        name = str(self.ign).strip()

        if not name:
            await interaction.response.send_message(
                "Your in-game name can't be empty. Please try again.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        nickname_error = None

        try:
            await member.edit(nick=name[:32])
        except discord.Forbidden:
            nickname_error = (
                "I couldn't set your nickname (missing permissions), "
                "but you're still verified."
            )
        except discord.HTTPException as error:
            await log_error(f"verification nickname edit user={member.id}", error)
            nickname_error = (
                "Something went wrong setting your nickname, but you're "
                "still verified."
            )

        role_error = None
        role = guild.get_role(MEMBER_ROLE_ID)

        if role is None:
            role_error = "The member role could not be found. Please contact staff."
        else:
            try:
                await member.add_roles(role, reason="Completed verification")
            except discord.Forbidden:
                role_error = (
                    "I couldn't assign the member role (missing "
                    "permissions). Please contact staff."
                )
            except discord.HTTPException as error:
                await log_error(f"verification role add user={member.id}", error)
                role_error = (
                    "Something went wrong assigning your role. Please "
                    "contact staff."
                )

        # Delete the original verification DM (the one with the button).
        row = await cursor.aexecute(
            """
            SELECT dm_channel_id, dm_message_id
            FROM pending_verifications
            WHERE user_id = ?
            """,
            (self.user_id,),
            fetch="one",
        )

        if row:
            dm_channel_id, dm_message_id = row

            if dm_channel_id and dm_message_id:
                try:
                    dm_channel = client.get_channel(
                        int(dm_channel_id)
                    ) or await client.fetch_channel(int(dm_channel_id))

                    dm_message = await dm_channel.fetch_message(int(dm_message_id))
                    await dm_message.delete()

                except (discord.NotFound, discord.Forbidden):
                    pass

                except Exception as error:
                    await log_error(
                        f"verification DM delete user={self.user_id}",
                        error,
                    )

        await cursor.aexecute(
            "DELETE FROM pending_verifications WHERE user_id = ?",
            (self.user_id,),
            commit=True,
        )

        # Confirmation DM.
        confirmation = make_embed(
            "✅ Verification Confirmed",
            f"You're verified as **{name}** and have been given the "
            f"member role in **{guild.name}**.",
        )

        await safe_call(
            member.send(embed=confirmation),
            f"verification confirmation DM user={member.id}",
        )

        summary = "✅ You're verified!"

        if nickname_error:
            summary += f"\n\n⚠️ {nickname_error}"

        if role_error:
            summary += f"\n\n⚠️ {role_error}"

        await interaction.followup.send(summary, ephemeral=True)


class VerificationView(discord.ui.View):
    def __init__(self, guild_id, user_id):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.user_id = user_id

        button = discord.ui.Button(
            label="✅ Verify",
            style=discord.ButtonStyle.success,
            custom_id=f"verify_start:{guild_id}:{user_id}",
        )

        button.callback = self.start_verification
        self.add_item(button)

    async def start_verification(self, interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This verification button isn't for you.",
                ephemeral=True,
            )
            return

        guild = client.get_guild(self.guild_id)

        if guild is None:
            await interaction.response.send_message(
                "The server could not be found. Please contact staff.",
                ephemeral=True,
            )
            return

        member = guild.get_member(self.user_id)

        if member is None:
            try:
                member = await guild.fetch_member(self.user_id)
            except discord.NotFound:
                member = None

        if member is None:
            await interaction.response.send_message(
                "You don't appear to be a member of the server anymore. "
                "Please rejoin and try again.",
                ephemeral=True,
            )
            return

        await interaction.response.send_modal(
            VerificationModal(self.guild_id, self.user_id)
        )


async def send_verification_dm(member):
    embed = make_embed(
        "🛡️ Verify Your Account",
        f"Welcome to **{member.guild.name}**!\n\n"
        "To gain access to the server, press the button below and "
        "enter your in-game name.",
    )

    view = VerificationView(member.guild.id, member.id)

    message, error = await safe_call(
        member.send(embed=embed, view=view),
        f"send_verification_dm user={member.id}",
    )

    if error or not message:
        # DMs are closed or otherwise unreachable — nothing more we can
        # do automatically here.
        return

    await cursor.aexecute(
        """
        INSERT INTO pending_verifications
        (user_id, guild_id, dm_channel_id, dm_message_id, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            guild_id = excluded.guild_id,
            dm_channel_id = excluded.dm_channel_id,
            dm_message_id = excluded.dm_message_id,
            created_at = excluded.created_at
        """,
        (member.id, member.guild.id, message.channel.id, message.id, now_ts()),
        commit=True,
    )


# ============================================================
# WELCOMER
# ============================================================


@client.event
async def on_member_join(member):
    await send_verification_dm(member)

    if WELCOME_CHANNEL_ID == 0:
        return

    channel = client.get_channel(WELCOME_CHANNEL_ID)

    if channel is None:
        return

    embed = make_embed(
        f"👋 Welcome to {member.guild.name}!",
        f"{member.mention}, glad to have you here. "
        f"You're member **#{member.guild.member_count}**.",
    )

    if member.display_avatar:
        embed.set_thumbnail(url=member.display_avatar.url)

    embed.timestamp = discord.utils.utcnow()

    await safe_call(
        channel.send(content=member.mention, embed=embed),
        "on_member_join welcome message",
    )


@client.event
async def on_member_remove(member):
    await cursor.aexecute(
        "DELETE FROM pending_verifications WHERE user_id = ? AND guild_id = ?",
        (member.id, member.guild.id),
        commit=True,
    )


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

            live_event_rows = await cursor.aexecute(
                """
                SELECT event_id
                FROM events
                WHERE status = 'live'
                """,
                (),
                fetch="all",
            )

            for row in live_event_rows:
                client.add_view(
                    EventCloseView(int(row[0]))
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

            verification_rows = await cursor.aexecute(
                """
                SELECT user_id, guild_id
                FROM pending_verifications
                """,
                (),
                fetch="all",
            )

            for row in verification_rows:
                client.add_view(
                    VerificationView(int(row[1]), int(row[0]))
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
